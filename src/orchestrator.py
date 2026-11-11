"""
Periodic market-discovery loop (Task 1.4).

Every ``interval_sec`` (default 10 min) one discovery cycle:

1. Ask Gamma for the top ``n_markets`` open markets by 24h volume.
2. For subscribed markets that are NOT in this top-N, look up their current
   status by id (``GammaClient.markets_by_ids``).
3. Apply the subscription rules below, subscribe / unsubscribe the tokens
   on the WS pool.
4. Write one line per market (top-N + everything still subscribed) to the
   markets stream of the WAL, with the decision taken for it.

Subscription rules (goal: once a market is recorded, record it until it
ends, so order books in the WAL are continuous):

- In top-N, not subscribed yet          -> subscribe (``added``)
- Dropped out of top-N, still trading    -> keep (``kept``)
- Dropped out of top-N, ended            -> unsubscribe (``removed``/``ended``);
  ended = ``closed`` or not ``acceptingOrders``. ``endDate`` is NOT used:
  for sports markets it is the kick-off time, the market trades long after.
- Dropped out of top-N, not returned by Gamma -> one strike; unsubscribe
  after ``missing_strikes_to_remove`` consecutive cycles (``not_found``).
- More than ``max_subscribed_markets``    -> unsubscribe the lowest-volume
  markets that are not in this top-N (``cap``). A top-N market is never
  evicted, hence ``n_markets <= max_subscribed_markets``.
- Status lookup fails                    -> add as usual, remove nothing
  (not even for the cap; it is enforced on the next successful cycle).
- Top-N fetch fails                      -> change nothing this cycle.

Markets stream line (see ``Walwriter.write_market_snapshot``)::

    {"ts_recv_ns": ..., "market_id": "6236747", "source": "top" | "lookup",
     "rank": 3 | null, "raw": {...Gamma dict...} | null,
     "sub": {"action": "added" | "kept" | "removed",
             "reason": null | "ended" | "cap" | "not_found" | "lookup_failed",
             "missing_strikes": 0}}

Connection events (connected / subscribed / unsubscribed / disconnected /
reconnecting / watchdog_timeout / closed) go to the events stream of the
WAL, so downstream can find gaps in the data. Per-second metrics samples go
to the metrics stream of the WAL (observability only).

Subscription state lives in memory only: after a restart the first cycle
starts again from the current top-N.
"""

from __future__ import annotations

import asyncio
import logging
import statistics
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src import config
from src.market.gamma_client import GammaClient, GammaError, MarketInfo
from src.common.utilities import Rec, now_ns
from src.wal.writer import Walwriter
from src.ws.connection import ConnectionEvent
from src.ws.pool import WSPool

logger = logging.getLogger(__name__)


METRICS_SAMPLE_INTERVAL_SEC = 1.0


@dataclass
class _SubscribedMarket:
    """One subscribed market, tracked across discovery cycles."""
    info: MarketInfo                   # latest known Gamma data (tokens, volume24hr, raw)
    first_subscribed_ns: int
    last_in_top_ns: int                # last cycle it was in the top-N
    missing_strikes: int = 0           # consecutive cycles the by-id lookup didn't return it


# =============================================================================
# Metrics collector (single-pool, live).
#
# Subset of orchestra_benchmark's Metrics — same measurement rules
# (skip list-shaped messages, dict messages with int(timestamp) only),
# but no warmup window or A/B comparison. Just rolling counters.
# =============================================================================

class _Metrics:
    """
    Live metrics for the orchestrator's WS pool.

    Kept private to this module — the orchestrator owns the only instance
    and feeds it from its on_message / on_event callbacks. State mutates
    only on the asyncio event loop, so no locking needed.
    """

    def __init__(self) -> None:
        self.t_start_ns = now_ns()

        # Rolling latency samples. ~50/s × hours could grow; we cap to
        # the most recent N to avoid unbounded memory in long runs.
        self._latencies_ms: list[float] = []
        self._latency_cap = 100_000

        # Cumulative throughput counters
        self.msgs_total = 0
        self.bytes_total = 0
        self.callback_ns_total = 0

        # Per-connection visibility
        self.msgs_per_conn: dict[int, int] = defaultdict(int)
        self.errors_per_conn: dict[int, int] = defaultdict(int)

        # Error counters
        self.json_decode_failures = 0
        self.missing_timestamp = 0
        self.event_count: dict[str, int] = defaultdict(int)
        self.reconnect_total = 0
        self.disconnect_total = 0
        self.watchdog_total = 0

        # For per-second deltas
        self._prev_msgs = 0
        self._prev_bytes = 0
        self._prev_lat_idx = 0

    # --- callback-side updates (called from on_message / on_event) ---

    def on_message(
        self,
        rec: Rec,
        parsed: Any,
        conn_id: int,
        callback_ns: int,
    ) -> None:
        self.msgs_total += 1
        self.bytes_total += len(rec.raw)
        self.callback_ns_total += callback_ns
        self.msgs_per_conn[conn_id] += 1

        # Latency only meaningful for dict messages with 'timestamp'.
        # The initial-subscribe response is a *list* of book snapshots
        # whose 'timestamp' fields are last-trade times (not send times),
        # so they're skipped entirely.
        if not isinstance(parsed, dict):
            return
        ts_str = parsed.get("timestamp")
        if ts_str is None:
            self.missing_timestamp += 1
            return
        try:
            t_server_ms = int(ts_str)
        except (TypeError, ValueError):
            self.missing_timestamp += 1
            return
        lag_ms = rec.ts_recv_ns / 1e6 - t_server_ms

        # Bounded list — drop oldest when we hit the cap.
        if len(self._latencies_ms) >= self._latency_cap:
            # Half-truncate so we don't pay this cost every msg.
            half = self._latency_cap // 2
            self._latencies_ms = self._latencies_ms[half:]
            self._prev_lat_idx = max(0, self._prev_lat_idx - half)
        self._latencies_ms.append(lag_ms)

    def on_event(self, conn_id: int, event: ConnectionEvent, _extra: dict) -> None:
        self.event_count[event.value] += 1
        if event == ConnectionEvent.DISCONNECTED:
            self.disconnect_total += 1
            self.errors_per_conn[conn_id] += 1
        elif event == ConnectionEvent.RECONNECTING:
            self.reconnect_total += 1
        elif event == ConnectionEvent.WATCHDOG_TIMEOUT:
            self.watchdog_total += 1
            self.errors_per_conn[conn_id] += 1

    # --- snapshot ---

    def snapshot(self, t_ns: int, subscribed_markets: int, subscribed_tokens: int) -> dict:
        """
        Per-second snapshot. Computes deltas vs. previous snapshot.
        The subscription counts are owned by the orchestrator and passed in.
        """
        elapsed_s = (t_ns - self.t_start_ns) / 1e9
        d_msgs = self.msgs_total - self._prev_msgs
        d_bytes = self.bytes_total - self._prev_bytes

        # Latency stats over the last sample window only
        lat_window = self._latencies_ms[self._prev_lat_idx:]
        lat_stats = self._lat_stats(lat_window)

        snap = {
            "t_rel_s": round(elapsed_s, 2),
            "msgs_total": self.msgs_total,
            "msgs_per_sec_window": d_msgs,
            "bytes_per_sec_window": d_bytes,
            "subscribed_markets": subscribed_markets,
            "subscribed_tokens": subscribed_tokens,
            "lat_window_ms": lat_stats,
            "errors": {
                "json_decode": self.json_decode_failures,
                "missing_timestamp": self.missing_timestamp,
                "disconnects": self.disconnect_total,
                "reconnects": self.reconnect_total,
                "watchdog": self.watchdog_total,
            },
            "errors_per_conn": dict(self.errors_per_conn),
            "msgs_per_conn": dict(self.msgs_per_conn),
        }
        self._prev_msgs = self.msgs_total
        self._prev_bytes = self.bytes_total
        self._prev_lat_idx = len(self._latencies_ms)
        return snap

    @staticmethod
    def _lat_stats(samples: list[float]) -> dict:
        if not samples:
            return {"n": 0}
        s = sorted(samples)
        return {
            "n": len(s),
            "mean": round(statistics.fmean(s), 1),
            "median": round(s[len(s) // 2], 1),
            "p95": round(s[min(len(s) - 1, int(len(s) * 0.95))], 1),
            "max": round(s[-1], 1),
        }


# =============================================================================
# Orchestrator
# =============================================================================

class Orchestrator:

    def __init__(
        self,
        wal: Walwriter,
        interval_sec: float = config.DISCOVERY_INTERVAL_SEC,
        n_markets: int = config.TOP_N_MARKETS,
        max_subscribed_markets: int = config.MAX_SUBSCRIBED_MARKETS,
        missing_strikes_to_remove: int = config.MISSING_STRIKES_TO_REMOVE,
    ) -> None:
        if interval_sec <= 0:
            raise ValueError("interval_sec must be > 0")
        if n_markets <= 0:
            raise ValueError("n_markets must be > 0")
        if n_markets > max_subscribed_markets:
            # Top-N markets are never evicted, so the cap must hold all of them.
            raise ValueError(
                f"n_markets ({n_markets}) must be <= max_subscribed_markets "
                f"({max_subscribed_markets})"
            )
        if missing_strikes_to_remove <= 0:
            raise ValueError("missing_strikes_to_remove must be > 0")

        self._interval_sec = interval_sec
        self._n_markets = n_markets
        self._max_subscribed_markets = max_subscribed_markets
        self._missing_strikes_to_remove = missing_strikes_to_remove
        self._wal = wal

        # Live metrics; sampled into the WAL metrics stream once per second
        self._metrics = _Metrics()
        self._sampler_task: asyncio.Task | None = None

        # Pool wiring: bind the bound methods so the pool can call them.
        # Both callbacks intentionally do little work; everything heavy
        # (WAL persist, sampling) is async-friendly.
        self._pool = WSPool(
            on_message=self._on_msg,
            on_event=self._on_event,
        )

        # Source of truth for "what we currently have subscribed", per market
        # (market id -> state). The pool only knows tokens; rules need markets.
        self._subs: dict[str, _SubscribedMarket] = {}

        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    # --- callbacks fed to the WS pool ---

    async def _on_msg(
        self,
        rec: Rec,
        parsed: Any,
        conn_id: int,
    ) -> None:
        # Persist first, then update metrics. WAL is the durability path;
        # metrics are best-effort observability.
        t0 = time.perf_counter_ns()
        await self._wal.write(rec)
        cb_ns = time.perf_counter_ns() - t0
        self._metrics.on_message(rec, parsed, conn_id, cb_ns)

    async def _on_event(
        self,
        conn_id: int,
        event: ConnectionEvent,
        extra: dict,
    ) -> None:
        self._metrics.on_event(conn_id, event, extra)
        try:
            await self._wal.write_event(
                now_ns(), {"conn_id": conn_id, "event": event.value, "extra": extra}
            )
        except Exception:
            logger.exception("write_event failed (%s, conn %d)", event.value, conn_id)

    # --- public API (unchanged shape) ---

    @property
    def current_subscriptions(self) -> frozenset[str]:
        """Read-only snapshot of the asset_ids we believe are subscribed."""
        return frozenset(t for sub in self._subs.values() for t in sub.info.tokens)

    @property
    def subscribed_markets(self) -> frozenset[str]:
        """Read-only snapshot of the market ids we have subscribed."""
        return frozenset(self._subs)

    async def start(self) -> None:
        """Begin the loop. Returns immediately; first cycle runs in the
        background. To wait for the first cycle to finish, see
        ``run_one_cycle()`` which is the building block we expose for tests."""
        if self._task is not None and not self._task.done():
            return

        # Pool MUST be started before we try to add subscriptions; its
        # add_subscriptions() raises RuntimeError if it's not running.
        await self._pool.start()

        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_forever(), name="etl-loop")
        self._sampler_task = asyncio.create_task(
            self._sampler_loop(), name="etl-metrics-sampler"
        )

    async def stop(self) -> None:
        """Signal stop and wait for the loop task to finish its current cycle."""
        self._stop_event.set()
        for t in (self._task, self._sampler_task):
            if t is not None:
                t.cancel()
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
        self._task = None
        self._sampler_task = None
        # Tear down the pool — closes all WS connections and waits for
        # their run-loops to exit. Without this, conn tasks leak past
        # orchestrator.stop().
        await self._pool.stop()

    # --- internal loops ---

    async def _run_forever(self) -> None:
        """Run cycles back-to-back at ``interval_sec`` cadence.

        We run the first cycle immediately on start, then wait between each.
        If a cycle takes longer than the interval (Gamma slow / many
        markets), we don't try to catch up — next cycle runs immediately
        and the schedule slips. This is the simplest and safest behavior.
        """
        while not self._stop_event.is_set():
            try:
                await self.run_one_cycle()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Any unexpected error (bug / Gamma down / etc.). Sleep and
                # try again next cycle.
                logger.exception("etl cycle failed; will retry next interval")

            # Sleep until next cycle, but wake early if stop requested.
            try:
                await asyncio.wait_for(self._stop_event.wait(), self._interval_sec)
            except asyncio.TimeoutError:
                continue  # interval elapsed normally
            else:
                break  # stop was set

    async def _sampler_loop(self) -> None:
        """Every METRICS_SAMPLE_INTERVAL_SEC, snapshot metrics -> WAL metrics stream."""
        while True:
            await asyncio.sleep(METRICS_SAMPLE_INTERVAL_SEC)
            ts = now_ns()
            snap = self._metrics.snapshot(
                ts,
                subscribed_markets=len(self._subs),
                subscribed_tokens=sum(len(s.info.tokens) for s in self._subs.values()),
            )
            try:
                await self._wal.write_metrics(ts, snap)
            except Exception:
                # Observability only: never let it stop the sampler.
                logger.exception("write_metrics failed")
            lat = snap["lat_window_ms"]
            logger.info(
                "metrics t=%5.1fs msgs+%4d/s subs=%d/%d lat n=%d med=%s p95=%s err(d/r/w)=%d/%d/%d",
                snap["t_rel_s"], snap["msgs_per_sec_window"],
                snap["subscribed_markets"], snap["subscribed_tokens"],
                lat.get("n", 0), lat.get("median", "-"), lat.get("p95", "-"),
                snap["errors"]["disconnects"],
                snap["errors"]["reconnects"],
                snap["errors"]["watchdog"],
            )

    async def run_one_cycle(self) -> bool:
        """
        Run a single discovery cycle and apply it to the pool (rules: see the
        module docstring). What was decided for each market is written to
        the markets stream of the WAL.

        Returns True if the cycle was applied, False if the top-N fetch
        failed (then nothing changed). Gamma errors are logged, never raised;
        the next cycle retries.
        """
        cycle_started_at = datetime.now(timezone.utc)
        try:
            async with GammaClient() as client:
                top_list = await client.top_markets_by_volume24h(n=self._n_markets)
                top = {m.id: m for m in top_list}   # keeps rank order
                dropped = [mid for mid in self._subs if mid not in top]
                looked_up: dict[str, MarketInfo] = {}
                lookup_ok = True
                if dropped:
                    try:
                        looked_up = await client.markets_by_ids(dropped)
                    except GammaError as e:
                        # Can't tell ended from still trading: remove nothing.
                        logger.warning("status lookup failed, removing nothing this cycle: %s", e)
                        lookup_ok = False
        except GammaError as e:
            # Half-baked top-N would look like markets dropping out. Skip cycle.
            logger.warning("discovery cycle aborted (Gamma error): %s", e)
            return False

        cycle_ts_ns = now_ns()
        added_mkts, removed_mkts, records = self._apply_rules(
            top, dropped, looked_up, lookup_ok, cycle_ts_ns
        )

        # Persist what Gamma said and what we decided, before touching the pool.
        try:
            await self._wal.write_market_snapshot(cycle_ts_ns, records)
        except Exception:
            # Persistence failure shouldn't kill the cycle.
            logger.exception("write_market_snapshot failed")

        added_tokens = [t for mid in added_mkts for t in self._subs[mid].info.tokens]
        removed_tokens = [t for sub, _reason in removed_mkts.values() for t in sub.info.tokens]
        if added_tokens:
            await self._pool.add_subscriptions(added_tokens)
        if removed_tokens:
            await self._pool.remove_subscriptions(removed_tokens)

        reasons = _count(reason for _sub, reason in removed_mkts.values())
        logger.info(
            "discovery: top=%d, +%d markets, -%d markets%s, kept outside top=%d, "
            "subscribed %d markets / %d tokens, lookup_ok=%s, took %.1fs",
            len(top), len(added_mkts), len(removed_mkts),
            f" {dict(reasons)}" if reasons else "",
            sum(1 for mid in self._subs if mid not in top), len(self._subs),
            sum(len(sub.info.tokens) for sub in self._subs.values()),
            lookup_ok, (datetime.now(timezone.utc) - cycle_started_at).total_seconds(),
        )
        return True

    def _apply_rules(
        self,
        top: dict[str, MarketInfo],
        dropped: list[str],
        looked_up: dict[str, MarketInfo],
        lookup_ok: bool,
        ts_ns: int,
    ) -> tuple[list[str], dict[str, tuple[_SubscribedMarket, str]], list[dict]]:
        """
        Update ``self._subs`` for one cycle. No I/O.

        Returns (added market ids, removed {market id: (state, reason)},
        markets-stream records: one per market in top-N or subscribed).
        """
        records: dict[str, dict] = {}

        def record(mid: str, source: str, rank: int | None, raw: dict | None,
                   action: str, reason: str | None, strikes: int = 0) -> None:
            records[mid] = {
                "market_id": mid, "source": source, "rank": rank, "raw": raw,
                "sub": {"action": action, "reason": reason, "missing_strikes": strikes},
            }

        # 1. This cycle's top-N: subscribe new ones, refresh the rest.
        added: list[str] = []
        for rank, (mid, info) in enumerate(top.items(), start=1):
            sub = self._subs.get(mid)
            if sub is None:
                self._subs[mid] = _SubscribedMarket(info, first_subscribed_ns=ts_ns, last_in_top_ns=ts_ns)
                added.append(mid)
                record(mid, "top", rank, info.raw, "added", None)
            else:
                sub.info, sub.last_in_top_ns, sub.missing_strikes = info, ts_ns, 0
                record(mid, "top", rank, info.raw, "kept", None)

        # 2. Subscribed but not in top-N: decide from the by-id lookup.
        removed: dict[str, tuple[_SubscribedMarket, str]] = {}
        for mid in dropped:
            sub = self._subs[mid]
            if not lookup_ok:
                record(mid, "lookup", None, None, "kept", "lookup_failed", sub.missing_strikes)
                continue
            info = looked_up.get(mid)
            if info is None:
                sub.missing_strikes += 1
                if sub.missing_strikes >= self._missing_strikes_to_remove:
                    removed[mid] = (sub, "not_found")
                    record(mid, "lookup", None, None, "removed", "not_found", sub.missing_strikes)
                else:
                    record(mid, "lookup", None, None, "kept", "not_found", sub.missing_strikes)
            elif not info.is_trading:
                sub.info = info
                removed[mid] = (sub, "ended")
                record(mid, "lookup", None, info.raw, "removed", "ended")
            else:
                sub.info, sub.missing_strikes = info, 0
                record(mid, "lookup", None, info.raw, "kept", None)

        # 3. Over the cap: evict the lowest-volume markets not in this top-N.
        #    Skipped when the lookup failed (remove nothing that cycle); the
        #    cap is enforced again on the next successful cycle.
        over = len(self._subs) - len(removed) - self._max_subscribed_markets
        if over > 0 and lookup_ok:
            candidates = sorted(
                (mid for mid in self._subs if mid not in top and mid not in removed),
                key=lambda mid: self._subs[mid].info.volume24hr,
            )
            for mid in candidates[:over]:
                removed[mid] = (self._subs[mid], "cap")
                records[mid]["sub"]["action"] = "removed"
                records[mid]["sub"]["reason"] = "cap"

        for mid in removed:
            del self._subs[mid]
        return added, removed, list(records.values())


def _count(values) -> dict[str, int]:
    out: dict[str, int] = defaultdict(int)
    for v in values:
        out[v] += 1
    return out


# =============================================================================
# CLI
# =============================================================================

async def _main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    data_dir = Path("../data/orchestrator")

    async with Walwriter(data_dir=data_dir) as wal:
        orch = Orchestrator(wal=wal)
        await orch.start()
        try:
            # Park until Ctrl-C
            await asyncio.Event().wait()
        finally:
            await orch.stop()


if __name__ == "__main__":
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        logger.info("stopped by user")
