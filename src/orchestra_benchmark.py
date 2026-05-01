"""
Latency / efficiency / feed-error comparison:

  Setup A:  ALL asset_ids on  1 WSConnection
  Setup B:  ASSET_IDS evenly split across 20 WSConnections (one market each)

Both setups subscribe to the same universe — current top-20 markets by
volume24hr (so 40 asset_ids = 20 markets × 2 outcomes), use the SAME WAL
writer pattern, and run for the same wall-clock duration.

What we measure (live, written to disk every second):

  latency_ms   — wire latency = ts_recv_ns/1e6 - parsed['timestamp'] (ms),
                 computed per dict-message that carries 'timestamp'.
                 Reported as count / mean / median / p95 / max.
  efficiency   — msgs/sec throughput, bytes/sec, callback time per msg.
                 Asset coverage: how many of the subscribed asset_ids
                 actually received any message (subscription health proxy).
  feed_errors  — DISCONNECTED / RECONNECTING / WATCHDOG_TIMEOUT events,
                 JSON decode failures, messages without parseable
                 'timestamp', per-conn (so a flaky conn shows up).

Real-time recording: three artifacts (one per setup, prefixed):
  {prefix}.metrics.jsonl  — one line per second, full snapshot
  {prefix}.events.jsonl   — every ConnectionEvent + every error
  stdout                  — one progress line per second + final summary

A WARMUP_SECONDS window at the start is excluded from the comparison
metrics (the initial 'book' snapshot for each asset shows up here and
would otherwise dominate the latency distribution — observed wire-lag
of ~6500ms for snapshot vs ~50ms for live ticks).
"""

from __future__ import annotations

import asyncio
import json
import logging
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from src.market.gamma_client import GammaClient
from src.wal.writer import WALWriter
from src.ws.connection import ConnectionEvent, WSConnection
from src.utilities import now_ns

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("orchestra")

N_CONNECTIONS = 20
TOP_N_MARKETS = 200
RUN_SECONDS = 600              # total wall-clock per setup
WARMUP_SECONDS = 5            # exclude early window from comparison stats
SAMPLE_INTERVAL_S = 1.0       # progress + metrics.jsonl cadence


# =============================================================================
# Metrics collector — one instance per setup
# =============================================================================

class Metrics:
    """
    Holds all running counters. We keep latency samples in-memory (one float
    per qualifying message) — at ~50 msgs/s × 90s × 8B ≈ 36KB, this is fine.

    All mutations happen on the asyncio event loop thread (callbacks +
    sample task), so no locking needed.
    """

    def __init__(self, setup_label: str, num_conns: int, asset_ids: set[str]):
        self.setup = setup_label
        self.num_conns = num_conns
        self.asset_universe = asset_ids
        self.t_start_ns = now_ns()
        self.t_end_ns: int | None = None  # set by run_setup when the loop returns

        # Latency samples (post-warmup only)
        self._latencies_ms: list[float] = []

        # Throughput counters (cumulative)
        self.msgs_total = 0
        self.bytes_total = 0
        self.callback_ns_total = 0
        self.msgs_post_warmup = 0
        self.bytes_post_warmup = 0

        # Per-conn counters (visibility into conn imbalance)
        self.msgs_per_conn: dict[int, int] = defaultdict(int)
        self.errors_per_conn: dict[int, int] = defaultdict(int)

        # Per-asset coverage — how many distinct assets actually got data?
        self.assets_seen: set[str] = set()

        # Error counters
        self.json_decode_failures = 0
        self.missing_timestamp = 0
        self.dropped_warmup = 0
        self.event_count: dict[str, int] = defaultdict(int)
        self.reconnect_total = 0
        self.disconnect_total = 0
        self.watchdog_total = 0

        # Once frozen, on_event still records to events.jsonl (caller-side)
        # but stops mutating the running tallies. We freeze right before
        # calling stop() on the connections so the orderly-shutdown
        # disconnects don't get counted as feed errors.
        self._frozen = False

        # For per-second deltas
        self._prev_msgs = 0
        self._prev_bytes = 0
        self._prev_lat_idx = 0

    def freeze(self) -> None:
        self._frozen = True

    # ---- callback-side updates ----

    def on_message(
        self,
        asset_ids: tuple[str, ...],
        raw_str: str,
        parsed: Any,
        ts_recv_ns: int,
        conn_id: int,
        cb_ns: int,
    ) -> None:
        self.msgs_total += 1
        self.bytes_total += len(raw_str)
        self.callback_ns_total += cb_ns
        self.msgs_per_conn[conn_id] += 1
        for aid in asset_ids:
            self.assets_seen.add(aid)

        in_warmup = (ts_recv_ns - self.t_start_ns) < WARMUP_SECONDS * 1e9
        if in_warmup:
            self.dropped_warmup += 1
            return

        self.msgs_post_warmup += 1
        self.bytes_post_warmup += len(raw_str)

        # Latency only meaningful for dict messages with 'timestamp'.
        # The initial-subscribe response is a *list* of book snapshots —
        # we skip those entirely. Lone 'book' dicts during normal flow
        # are real updates and DO get measured.
        if isinstance(parsed, dict):
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
            self._latencies_ms.append(lag_ms)
        # list types (initial snapshots) are intentionally not measured.

    def on_decode_failure(self, conn_id: int) -> None:
        self.json_decode_failures += 1
        self.errors_per_conn[conn_id] += 1

    def on_event(self, conn_id: int, event: ConnectionEvent, _extra: dict) -> None:
        if self._frozen:
            return
        self.event_count[event.value] += 1
        if event == ConnectionEvent.DISCONNECTED:
            self.disconnect_total += 1
            self.errors_per_conn[conn_id] += 1
        elif event == ConnectionEvent.RECONNECTING:
            self.reconnect_total += 1
        elif event == ConnectionEvent.WATCHDOG_TIMEOUT:
            self.watchdog_total += 1
            self.errors_per_conn[conn_id] += 1

    # ---- snapshot / final ----

    def snapshot(self, t_ns: int) -> dict:
        """Per-second snapshot. Computes deltas vs. previous snapshot."""
        elapsed_s = (t_ns - self.t_start_ns) / 1e9
        d_msgs = self.msgs_total - self._prev_msgs
        d_bytes = self.bytes_total - self._prev_bytes

        # Latency stats over the last 1s window only
        lat_window = self._latencies_ms[self._prev_lat_idx:]
        lat_stats = self._lat_stats(lat_window)

        snap = {
            "setup": self.setup,
            "t_rel_s": round(elapsed_s, 2),
            "msgs_total": self.msgs_total,
            "msgs_per_sec_window": d_msgs,
            "bytes_per_sec_window": d_bytes,
            "assets_seen": len(self.assets_seen),
            "assets_universe": len(self.asset_universe),
            "lat_window_ms": lat_stats,
            "errors": {
                "json_decode": self.json_decode_failures,
                "missing_timestamp": self.missing_timestamp,
                "disconnects": self.disconnect_total,
                "reconnects": self.reconnect_total,
                "watchdog": self.watchdog_total,
            },
            "errors_per_conn": dict(self.errors_per_conn),
            "in_warmup": elapsed_s < WARMUP_SECONDS,
        }
        self._prev_msgs = self.msgs_total
        self._prev_bytes = self.bytes_total
        self._prev_lat_idx = len(self._latencies_ms)
        return snap

    def final_summary(self) -> dict:
        # Use the snapshot taken when run_setup() returned, NOT now_ns():
        # by the time the orchestrator prints the final comparison, the
        # *next* setup may already have finished too.
        t_end = self.t_end_ns if self.t_end_ns is not None else now_ns()
        elapsed_s = (t_end - self.t_start_ns) / 1e9
        post = max(elapsed_s - WARMUP_SECONDS, 1e-9)
        return {
            "setup": self.setup,
            "num_conns": self.num_conns,
            "elapsed_s": round(elapsed_s, 1),
            "warmup_s": WARMUP_SECONDS,
            "post_warmup_s": round(post, 1),
            "throughput_post_warmup": {
                "msgs": self.msgs_post_warmup,
                "msgs_per_sec": round(self.msgs_post_warmup / post, 1),
                "bytes_per_sec": round(self.bytes_post_warmup / post, 0),
            },
            "callback_efficiency": {
                "total_msgs": self.msgs_total,
                "avg_callback_us": (
                    round(self.callback_ns_total / 1e3 / self.msgs_total, 2)
                    if self.msgs_total else None
                ),
            },
            "asset_coverage": {
                "subscribed": len(self.asset_universe),
                "received_data": len(self.assets_seen),
                "ratio": round(
                    len(self.assets_seen) / max(len(self.asset_universe), 1), 3
                ),
            },
            "latency_ms_post_warmup": self._lat_stats(self._latencies_ms),
            "feed_errors": {
                "json_decode": self.json_decode_failures,
                "missing_timestamp": self.missing_timestamp,
                "disconnects": self.disconnect_total,
                "reconnects": self.reconnect_total,
                "watchdog_timeouts": self.watchdog_total,
                "events": dict(self.event_count),
                "per_conn": dict(self.errors_per_conn),
            },
            "msgs_per_conn": dict(self.msgs_per_conn),
        }

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
# Setup runner — runs ONE configuration end-to-end
# =============================================================================

async def run_setup(
    setup_label: str,
    asset_groups: list[list[str]],   # one inner list per WSConnection
    out_dir: Path,
    wal: "WALWriter",
) -> Metrics:
    """
    Spin up `len(asset_groups)` connections with their respective asset_ids,
    run for RUN_SECONDS, write metrics + events live, persist every WS
    message to `wal`, and return the final Metrics.
    """
    universe = {a for g in asset_groups for a in g}
    metrics = Metrics(setup_label, num_conns=len(asset_groups), asset_ids=universe)

    metrics_fp = (out_dir / f"{setup_label}.metrics.jsonl").open("w")
    events_fp = (out_dir / f"{setup_label}.events.jsonl").open("w")

    def jlog(fp, obj):
        fp.write(json.dumps(obj) + "\n")
        fp.flush()  # fsync would be excessive — flush gives us live tail-able file

    async def on_msg(aids, raw, parsed, ts_recv_ns, conn_id):
        t0 = time.perf_counter_ns()
        # Callback work in production = update metrics + persist to WAL.
        # The WAL is shared structurally between A and B (same writer
        # implementation, same fsync cadence) so its overhead applies
        # symmetrically — the relative comparison stays valid, and the
        # numbers now reflect real "WS + persistence" cost, not a
        # decorative no-op handler.
        metrics.on_message(aids, raw, parsed, ts_recv_ns, conn_id, 0)
        await wal.write(aids, raw, ts_recv_ns)
        cb_ns = time.perf_counter_ns() - t0
        # Add callback time AFTER on_message so we don't reentrantly
        # measure ourselves inside the metrics update.
        metrics.callback_ns_total += cb_ns

    async def on_event(conn_id, event: ConnectionEvent, extra):
        metrics.on_event(conn_id, event, extra)
        jlog(events_fp, {
            "ts_ns": now_ns(),
            "setup": setup_label,
            "conn_id": conn_id,
            "event": event.value,
            "extra": extra,
        })

    # Build connections + tasks
    conns: list[WSConnection] = []
    run_tasks: list[asyncio.Task] = []
    for cid, asset_ids in enumerate(asset_groups):
        c = WSConnection(conn_id=cid, on_message=on_msg, on_event=on_event)
        await c.add_subscriptions(asset_ids)
        run_tasks.append(asyncio.create_task(c.run(), name=f"{setup_label}-ws-{cid}"))
        conns.append(c)

    log.info("[%s] launched %d conn(s), %d total asset_ids",
             setup_label, len(conns), len(universe))

    async def sampler():
        while True:
            await asyncio.sleep(SAMPLE_INTERVAL_S)
            snap = metrics.snapshot(now_ns())
            jlog(metrics_fp, snap)
            lat = snap["lat_window_ms"]
            log.info(
                "[%s] t=%5.1fs msgs+%4d/s assets=%d/%d lat n=%d med=%s p95=%s err(d/r/w)=%d/%d/%d",
                setup_label, snap["t_rel_s"], snap["msgs_per_sec_window"],
                snap["assets_seen"], snap["assets_universe"],
                lat.get("n", 0), lat.get("median", "-"), lat.get("p95", "-"),
                snap["errors"]["disconnects"], snap["errors"]["reconnects"],
                snap["errors"]["watchdog"],
            )

    sampler_task = asyncio.create_task(sampler(), name=f"{setup_label}-sampler")

    try:
        await asyncio.sleep(RUN_SECONDS)
    finally:
        # Freeze metrics BEFORE stopping connections so the cascade of
        # 'disconnected'/'closed' events triggered by our own stop() does
        # NOT count as feed errors.
        metrics.freeze()
        for c in conns:
            c.stop()
        sampler_task.cancel()
        await asyncio.gather(*run_tasks, sampler_task, return_exceptions=True)
        metrics.t_end_ns = now_ns()
        metrics_fp.close()
        events_fp.close()

    return metrics


# =============================================================================
# Orchestrator
# =============================================================================

async def main() -> None:
    out_dir = Path(__file__).resolve().parent.parent / "data" / "orchestra"
    out_dir.mkdir(parents=True, exist_ok=True)
    log.info("output dir: %s", out_dir)

    # ---- Discover the universe ONCE; both setups use the same asset_ids ----
    async with GammaClient() as client:
        ids, results = await client.fetch_top_n_by_volume24h(n=TOP_N_MARKETS)
    asset_ids: list[str] = []
    per_market_assets: list[list[str]] = []
    for _market, raw in results:
        tids = json.loads(raw["clobTokenIds"])
        per_market_assets.append([str(t) for t in tids])
        asset_ids.extend(str(t) for t in tids)

    if len(per_market_assets) != TOP_N_MARKETS:
        log.warning("got %d markets, expected %d", len(per_market_assets), TOP_N_MARKETS)

    log.info("universe: %d markets → %d asset_ids", len(per_market_assets), len(asset_ids))

    # ---- Setup A: all on 1 conn ----
    # Each setup gets its OWN WALWriter rooted at a separate data_dir.
    # Same code path / same fsync cadence on both sides so the persistence
    # cost applies symmetrically, but the file handles + locks are
    # independent so the two setups don't bottleneck on each other.
    log.info("=" * 70)
    log.info("SETUP A: 1 connection, %d asset_ids", len(asset_ids))
    log.info("=" * 70)
    async with WALWriter(data_dir=out_dir / "wal_A", shard_prefix_len=1) as wal_a:
        metrics_a = await run_setup("setupA_1conn", [asset_ids], out_dir, wal_a)
        wal_a_stats = wal_a.stats()

    # Brief gap so connection state is fully unwound before we open 20 more
    await asyncio.sleep(2)

    # ---- Setup B: 20 conns ----
    log.info("=" * 70)
    log.info("SETUP B: %d connections", len(per_market_assets))
    log.info("=" * 70)

    chunks: list[list[str]] = [[] for _ in range(N_CONNECTIONS)]
    for i, item in enumerate(asset_ids):
        chunks[i % N_CONNECTIONS].append(item)

    async with WALWriter(data_dir=out_dir / "wal_B") as wal_b:
        metrics_b = await run_setup("setupB_20conns", chunks, out_dir, wal_b)
        wal_b_stats = wal_b.stats()

    # ---- Final comparison ----
    sa = metrics_a.final_summary()
    sb = metrics_b.final_summary()
    sa["wal_stats"] = wal_a_stats
    sb["wal_stats"] = wal_b_stats

    final_path = out_dir / "comparison.json"
    final_path.write_text(json.dumps({"setupA": sa, "setupB": sb}, indent=2))

    print("\n" + "=" * 70)
    print("FINAL COMPARISON")
    print("=" * 70)
    _print_side_by_side(sa, sb)
    print()
    print(f"Wrote: {final_path}")
    print(f"Per-second metrics: {out_dir}/setupA_1conn.metrics.jsonl")
    print(f"                    {out_dir}/setupB_20conns.metrics.jsonl")
    print(f"Connection events:  {out_dir}/setupA_1conn.events.jsonl")
    print(f"                    {out_dir}/setupB_20conns.events.jsonl")
    print(f"WAL data:           {out_dir}/wal_A/")
    print(f"                    {out_dir}/wal_B/")


def _print_side_by_side(a: dict, b: dict) -> None:
    rows = [
        ("connections",          a["num_conns"], b["num_conns"]),
        ("elapsed_s",            a["elapsed_s"], b["elapsed_s"]),
        ("post_warmup_s",        a["post_warmup_s"], b["post_warmup_s"]),
        ("",                     "",            ""),
        ("--- THROUGHPUT (post-warmup) ---", "", ""),
        ("msgs",                 a["throughput_post_warmup"]["msgs"], b["throughput_post_warmup"]["msgs"]),
        ("msgs/sec",             a["throughput_post_warmup"]["msgs_per_sec"], b["throughput_post_warmup"]["msgs_per_sec"]),
        ("bytes/sec",            a["throughput_post_warmup"]["bytes_per_sec"], b["throughput_post_warmup"]["bytes_per_sec"]),
        ("",                     "",            ""),
        ("--- LATENCY (ms, post-warmup) ---", "", ""),
        ("samples",              a["latency_ms_post_warmup"].get("n",0), b["latency_ms_post_warmup"].get("n",0)),
        ("mean",                 a["latency_ms_post_warmup"].get("mean","-"), b["latency_ms_post_warmup"].get("mean","-")),
        ("median",               a["latency_ms_post_warmup"].get("median","-"), b["latency_ms_post_warmup"].get("median","-")),
        ("p95",                  a["latency_ms_post_warmup"].get("p95","-"), b["latency_ms_post_warmup"].get("p95","-")),
        ("max",                  a["latency_ms_post_warmup"].get("max","-"), b["latency_ms_post_warmup"].get("max","-")),
        ("",                     "",            ""),
        ("--- EFFICIENCY ---", "", ""),
        ("avg callback (µs)",    a["callback_efficiency"]["avg_callback_us"], b["callback_efficiency"]["avg_callback_us"]),
        ("asset coverage",       f"{a['asset_coverage']['received_data']}/{a['asset_coverage']['subscribed']}",
                                 f"{b['asset_coverage']['received_data']}/{b['asset_coverage']['subscribed']}"),
        ("wal msgs persisted",   a.get("wal_stats",{}).get("messages","-"), b.get("wal_stats",{}).get("messages","-")),
        ("wal bytes persisted",  a.get("wal_stats",{}).get("bytes","-"), b.get("wal_stats",{}).get("bytes","-")),
        ("",                     "",            ""),
        ("--- FEED ERRORS ---", "", ""),
        ("disconnects",          a["feed_errors"]["disconnects"], b["feed_errors"]["disconnects"]),
        ("reconnects",           a["feed_errors"]["reconnects"], b["feed_errors"]["reconnects"]),
        ("watchdog timeouts",    a["feed_errors"]["watchdog_timeouts"], b["feed_errors"]["watchdog_timeouts"]),
        ("json decode fails",    a["feed_errors"]["json_decode"], b["feed_errors"]["json_decode"]),
        ("missing timestamp",    a["feed_errors"]["missing_timestamp"], b["feed_errors"]["missing_timestamp"]),
    ]
    print(f"{'metric':35s}  {'A: 1 conn':>15s}  {'B: 20 conns':>15s}")
    print("-" * 70)
    for label, av, bv in rows:
        print(f"{label:35s}  {str(av):>15s}  {str(bv):>15s}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("stopped by user")
