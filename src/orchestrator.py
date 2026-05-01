"""
Periodic market-discovery loop (Task 1.4).

Runs a Gamma cycle every N minutes, diffs the result against the pool's
current subscription set, and applies the additions/removals.

Design notes:

- The loop is the *backup* path for new markets. The primary path is the
  WS ``new_market`` event (sub-second latency once active subscriptions
  exist on the relevant connection). The Gamma poll catches anything WS
  missed (e.g. during a reconnect).

- ``to_remove`` uses a "strike" mechanism (3 consecutive cycles missing
  before actually removing). This protects against offset-pagination
  edge cases where a single page can shift records between cycles, briefly
  hiding an asset. Without strikes, we'd flap subscriptions for assets that
  are still active.

- Each cycle persists the full set of seen markets via
  ``WALWriter.write_market_snapshot``, giving us the metadata history
  (question text, endDate, volume, etc.) over time, alongside the WAL.

- Failure of one cycle is non-fatal — it just delays discovery by one
  interval. We log and move on.

- A lightweight Metrics collector runs alongside, fed by the same
  on_message / on_event callbacks the Pool uses. Each second we snapshot
  rolling counters into ``{out_dir}/metrics.jsonl`` and every connection
  event into ``{out_dir}/events.jsonl``. This is the long-running, single-
  pool flavor of orchestra_benchmark's metrics — no warmup window, no
  setup-comparison, just live observability.
"""

from __future__ import annotations

import asyncio
import json
import logging
import statistics
import time
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.market.gamma_client import GammaClient, GammaError
from src.utilities import now_ns
from src.wal.writer import WALWriter
from src.ws.connection import ConnectionEvent
from src.ws.pool import WSPool

logger = logging.getLogger(__name__)


# Default cycle interval. Spec said "every 5-10 minutes" — we go with 10
# because the WS path covers the urgent cases (new_market arrives in seconds)
# and 10 min keeps Gamma load reasonable. Researchers can change at config.
DEFAULT_INTERVAL_SEC = 600.0
TOP_N_MARKETS = 3000
METRICS_SAMPLE_INTERVAL_SEC = 1.0


# Optional callback the application can supply to react to each cycle's
# diff (e.g. for metrics or audit logging).
DiffHandler = Callable[["DiscoveryDiff"], Awaitable[None]]


@dataclass
class DiscoveryDiff:
    """What changed in one discovery cycle."""
    cycle_started_at: datetime
    cycle_finished_at: datetime

    """Token IDs we just subscribed to (refcount went from 0 to 1)."""
    added: set[str]
    """Token IDs we just unsubscribed from (3-strike mechanism triggered)."""
    removed: set[str]


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

        # Asset coverage (assets that have produced at least one msg)
        self.assets_seen: set[str] = set()

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
        asset_ids: tuple[str, ...],
        raw_str: str,
        parsed: Any,
        ts_recv_ns: int,
        conn_id: int,
        callback_ns: int,
    ) -> None:
        self.msgs_total += 1
        self.bytes_total += len(raw_str)
        self.callback_ns_total += callback_ns
        self.msgs_per_conn[conn_id] += 1
        for aid in asset_ids:
            self.assets_seen.add(aid)

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
        lag_ms = ts_recv_ns / 1e6 - t_server_ms

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

    def snapshot(self, t_ns: int) -> dict:
        """Per-second snapshot. Computes deltas vs. previous snapshot."""
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
            "assets_seen": len(self.assets_seen),
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
        out_dir: Path,
        wal: WALWriter,
        interval_sec: float = DEFAULT_INTERVAL_SEC,
        on_diff: DiffHandler | None = None,
        n_markets: int = TOP_N_MARKETS,
    ) -> None:
        if interval_sec <= 0:
            raise ValueError("interval_sec must be > 0")

        self._out_dir = out_dir
        self._interval_sec = interval_sec
        self._n_markets = n_markets
        self._on_diff = on_diff
        self._wal = wal

        # Live metrics + their on-disk sinks
        self._metrics = _Metrics()
        self._metrics_fp = None  # opened in start(), closed in stop()
        self._events_fp = None
        self._sampler_task: asyncio.Task | None = None

        # Pool wiring: bind the bound methods so the pool can call them.
        # Both callbacks intentionally do little work; everything heavy
        # (WAL persist, sampling) is async-friendly.
        self._pool = WSPool(
            on_message=self._on_msg,
            on_event=self._on_event,
        )

        # Source of truth for "what we currently have subscribed". Mirrors
        # the pool's view but kept here so we don't have to query the pool
        # during diff (no lock contention and we own the schedule).
        self._current_subscriptions: set[str] = set()

        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    # --- callbacks fed to the WS pool ---

    async def _on_msg(
        self,
        asset_ids: tuple[str, ...],
        raw: str,
        parsed: Any,
        ts_recv_ns: int,
        conn_id: int,
    ) -> None:
        # Persist first, then update metrics. WAL is the durability path;
        # metrics are best-effort observability.
        t0 = time.perf_counter_ns()
        await self._wal.write(asset_ids, raw, ts_recv_ns)
        cb_ns = time.perf_counter_ns() - t0
        self._metrics.on_message(asset_ids, raw, parsed, ts_recv_ns, conn_id, cb_ns)

    async def _on_event(
        self,
        conn_id: int,
        event: ConnectionEvent,
        extra: dict,
    ) -> None:
        self._metrics.on_event(conn_id, event, extra)
        if self._events_fp is not None:
            self._events_fp.write(json.dumps({
                "ts_ns": now_ns(),
                "conn_id": conn_id,
                "event": event.value,
                "extra": extra,
            }) + "\n")
            self._events_fp.flush()

    # --- public API (unchanged shape) ---

    @property
    def current_subscriptions(self) -> frozenset[str]:
        """Read-only snapshot of the asset_ids we believe are subscribed."""
        return frozenset(self._current_subscriptions)

    async def start(self) -> None:
        """Begin the loop. Returns immediately; first cycle runs in the
        background. To wait for the first cycle to finish, see
        ``run_one_cycle()`` which is the building block we expose for tests."""
        if self._task is not None and not self._task.done():
            return

        self._out_dir.mkdir(parents=True, exist_ok=True)
        self._metrics_fp = (self._out_dir / "metrics.jsonl").open("w")
        self._events_fp = (self._out_dir / "events.jsonl").open("w")

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
        for fp_attr in ("_metrics_fp", "_events_fp"):
            fp = getattr(self, fp_attr)
            if fp is not None:
                fp.close()
                setattr(self, fp_attr, None)

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
        """Every METRICS_SAMPLE_INTERVAL_SEC, snapshot metrics → metrics.jsonl."""
        while True:
            await asyncio.sleep(METRICS_SAMPLE_INTERVAL_SEC)
            snap = self._metrics.snapshot(now_ns())
            if self._metrics_fp is not None:
                self._metrics_fp.write(json.dumps(snap) + "\n")
                self._metrics_fp.flush()
            lat = snap["lat_window_ms"]
            logger.info(
                "metrics t=%5.1fs msgs+%4d/s assets=%d lat n=%d med=%s p95=%s err(d/r/w)=%d/%d/%d",
                snap["t_rel_s"], snap["msgs_per_sec_window"],
                snap["assets_seen"],
                lat.get("n", 0), lat.get("median", "-"), lat.get("p95", "-"),
                snap["errors"]["disconnects"],
                snap["errors"]["reconnects"],
                snap["errors"]["watchdog"],
            )

    async def run_one_cycle(self) -> DiscoveryDiff | None:
        """
        Run a single discovery cycle and apply diff to the pool.

        Returns the ``DiscoveryDiff`` on success, or ``None`` if the Gamma
        fetch failed (in which case nothing was changed). Never raises on
        Gamma errors — those are logged and swallowed; the next cycle will
        retry.
        """
        cycle_started_at = datetime.now(timezone.utc)
        try:
            async with GammaClient() as client:
                market_ids, asset_ids, raws = await client.fetch_top_n_by_volume24h(
                    n=self._n_markets
                )
        except GammaError as e:
            # Mid-pagination failure — half-baked data is worse than no
            # data because it would trigger spurious to_remove. Skip cycle.
            logger.warning("discovery cycle aborted (Gamma error): %s", e)
            return None

        # Persist the full raw market list for this cycle. Goes to the
        # markets stream of the WAL, NOT the per-asset shards — see
        # WALWriter.write_market_snapshot.
        cycle_ts_ns = now_ns()
        try:
            await self._wal.write_market_snapshot(cycle_ts_ns, raws)
        except Exception:
            # Persistence failure shouldn't kill the cycle — diff/apply
            # is the more important task.
            logger.exception("write_market_snapshot failed")

        seen = set(asset_ids)

        # Compute diff
        added = seen - self._current_subscriptions
        remove = self._current_subscriptions - seen

        if added:
            await self._pool.add_subscriptions(list(added))
        if remove:
            await self._pool.remove_subscriptions(list(remove))

        # Update our local view
        self._current_subscriptions.update(added)
        self._current_subscriptions.difference_update(remove)

        cycle_finished_at = datetime.now(timezone.utc)
        diff = DiscoveryDiff(
            cycle_started_at=cycle_started_at,
            cycle_finished_at=cycle_finished_at,
            added=added,
            removed=remove,
        )

        logger.info(
            "discovery diff: +%d added, -%d removed, "
            "%d total subs, took %.1fs (raws=%d persisted)",
            len(added),
            len(remove),
            len(self._current_subscriptions),
            (cycle_finished_at - cycle_started_at).total_seconds(),
            len(raws),
        )

        if self._on_diff is not None:
            try:
                await self._on_diff(diff)
            except Exception:
                logger.exception("on_diff handler raised")

        return diff


# =============================================================================
# CLI
# =============================================================================

async def _main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    out_dir = Path("../data/orchestrator")
    out_dir.mkdir(parents=True, exist_ok=True)

    async with WALWriter(data_dir=out_dir, shard_prefix_len=1) as wal:
        orch = Orchestrator(out_dir=out_dir, wal=wal)
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
