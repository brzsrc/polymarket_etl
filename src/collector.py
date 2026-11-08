"""
Collector entry point: Orchestrator (discovery + WS pool) -> Walwriter ->
Uploader, wired together with an explicit data dir, a destination, a run
length and a clean shutdown.

Command line (local end-to-end run, any directory as the destination)::

    python -m src.collector --data-dir /tmp/pm --landing /tmp/fake_volume \\
        --n-markets 20 --duration-sec 600 --segment-max-mb 5

Databricks notebook (the notebook already runs an event loop, so await)::

    from src.collector import run
    stats = await run(data_dir="/tmp/polymarket",
                      landing="/Volumes/polymarket/raw/landing",
                      host_id="dbx", n_markets=200, duration_sec=4 * 3600)

Shutdown order matters and is fixed by the nesting below:

    1. stop the orchestrator      no new messages
    2. exit the writer            closes and seals every active segment
    3. exit the uploader          uploads what is left, then the run logs

Stops when ``duration_sec`` elapses, when ``stop_event`` is set, or (command
line only) on SIGTERM / SIGINT. A kill that skips Python cleanup (SIGKILL,
a hard job timeout) skips steps 2-3, so set ``duration_sec`` below any
external timeout.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import re
import signal
import socket
import sys
from functools import partial
from pathlib import Path

from src.orchestrator import TOP_N_MARKETS, Orchestrator
from src.wal.uploader import DEFAULT_SCAN_INTERVAL_SEC, Uploader, put_file
from src.wal.writer import DEFAULT_CHECK_INTERVAL_SEC, DEFAULT_SEGMENT_MAX_BYTES, Walwriter

logger = logging.getLogger(__name__)

# Orchestrator writes these into data_dir; uploaded under _runs/ on exit.
_RUN_LOGS = ("metrics.jsonl", "events.jsonl")


async def run(
    data_dir: Path | str,
    landing: Path | str,
    *,
    host_id: str | None = None,
    n_markets: int = TOP_N_MARKETS,
    duration_sec: float | None = None,
    segment_max_bytes: int | None = DEFAULT_SEGMENT_MAX_BYTES,
    check_interval_sec: float = DEFAULT_CHECK_INTERVAL_SEC,
    scan_interval_sec: float = DEFAULT_SCAN_INTERVAL_SEC,
    keep_local: bool = False,
    stop_event: asyncio.Event | None = None,
    handle_signals: bool = False,
) -> dict:
    """
    Run the collector until ``duration_sec`` elapses (None = forever) or
    ``stop_event`` is set, then shut down cleanly. Returns writer and
    uploader stats; ``stats["uploader"]["pending"] > 0`` means some sealed
    files did not reach ``landing``.

    data_dir:  local working dir for the WAL (must support appends:
               local disk, not /Volumes).
    landing:   destination root: a UC volume path on Databricks, or any
               directory for local runs.
    handle_signals: turn SIGTERM / SIGINT into a clean stop. Leave False in
               notebooks, where the kernel owns SIGINT.
    """
    data_dir = Path(data_dir)
    landing = Path(landing)
    if data_dir.resolve() == landing.resolve():
        raise ValueError("data_dir and landing must be different directories")
    host_id = host_id or default_host_id()
    stop = stop_event or asyncio.Event()
    put = partial(put_file, dest_root=landing)

    remove_handlers = _install_signal_handlers(stop) if handle_signals else (lambda: None)
    try:
        async with Uploader(
            data_dir, put, host_id,
            scan_interval_sec=scan_interval_sec, keep_local=keep_local,
        ) as up:
            async with Walwriter(
                data_dir,
                segment_max_bytes=segment_max_bytes,
                check_interval_sec=check_interval_sec,
            ) as wal:
                orch = Orchestrator(out_dir=data_dir, wal=wal, n_markets=n_markets)
                await orch.start()
                logger.info(
                    "collector running (data_dir=%s, landing=%s, n_markets=%d, duration=%s)",
                    data_dir, landing, n_markets,
                    f"{duration_sec:.0f}s" if duration_sec is not None else "unlimited",
                )
                try:
                    await _wait_for_stop(stop, duration_sec)
                finally:
                    await orch.stop()
            # Writer has exited: every segment is sealed. The uploader exit
            # (next) uploads them; run logs go now, best effort.
            await _upload_run_logs(data_dir, put, up.source_id)
        result = {"wal": wal.stats(), "uploader": up.stats()}
    finally:
        remove_handlers()

    logger.info("collector finished: %s", result)
    return result


def default_host_id() -> str:
    """Hostname reduced to the characters allowed in file names."""
    return re.sub(r"[^A-Za-z0-9_-]", "-", socket.gethostname())[:32] or "host"


async def _wait_for_stop(stop: asyncio.Event, duration_sec: float | None) -> None:
    try:
        await asyncio.wait_for(stop.wait(), duration_sec)
        logger.info("collector: stop requested")
    except asyncio.TimeoutError:
        logger.info("collector: duration reached")


async def _upload_run_logs(data_dir: Path, put, source_id: str) -> None:
    """Copy metrics/events logs to _runs/{source_id}/. Failure is logged, not raised."""
    for name in _RUN_LOGS:
        p = data_dir / name
        if not p.exists():
            continue
        try:
            await asyncio.to_thread(put, p, f"_runs/{source_id}/{name}")
        except Exception:
            logger.exception("collector: could not upload run log %s", p)


def _install_signal_handlers(stop: asyncio.Event):
    """SIGTERM / SIGINT -> stop.set(). Returns a function that removes them again."""
    loop = asyncio.get_running_loop()
    installed = []
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _on_signal, sig, stop)
            installed.append(sig)
        except (NotImplementedError, RuntimeError, ValueError):
            # Not the main thread, or a platform without loop signal support.
            logger.warning("collector: cannot handle %s here", sig.name)

    def remove() -> None:
        for sig in installed:
            loop.remove_signal_handler(sig)

    return remove


def _on_signal(sig: signal.Signals, stop: asyncio.Event) -> None:
    logger.info("collector: received %s, shutting down", sig.name)
    stop.set()


# ----------------------------------------------------------------------
# Command line
# ----------------------------------------------------------------------

def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run the Polymarket collector.")
    p.add_argument("--data-dir", required=True, type=Path,
                   help="local WAL directory (local disk, not /Volumes)")
    p.add_argument("--landing", required=True, type=Path,
                   help="destination root: /Volumes/<catalog>/<schema>/<volume> or any directory")
    p.add_argument("--host-id", default=None,
                   help="prefix of the source id in remote file names (default: hostname)")
    p.add_argument("--n-markets", type=int, default=TOP_N_MARKETS)
    p.add_argument("--duration-sec", type=float, default=None,
                   help="stop after this many seconds (default: run until SIGTERM/SIGINT)")
    p.add_argument("--segment-max-mb", type=float, default=None,
                   help="seal a segment at this size (default: writer default)")
    p.add_argument("--keep-local", action="store_true",
                   help="keep local .gz files after upload")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    segment_max = (
        int(args.segment_max_mb * 1024 * 1024) if args.segment_max_mb else DEFAULT_SEGMENT_MAX_BYTES
    )
    stats = asyncio.run(run(
        args.data_dir, args.landing,
        host_id=args.host_id,
        n_markets=args.n_markets,
        duration_sec=args.duration_sec,
        segment_max_bytes=segment_max,
        keep_local=args.keep_local,
        handle_signals=True,
    ))
    pending = stats["uploader"]["pending"]
    if pending:
        logger.error("collector: %d sealed files were not uploaded", pending)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
