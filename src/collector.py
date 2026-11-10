"""
Collector entry point: Orchestrator (discovery + WS pool) -> Walwriter ->
Uploader, wired together with an explicit data dir, a destination, a run
length and a clean shutdown.

Command line (local end-to-end run, any directory as the destination)::

    python -m src.collector --data-dir /tmp/pm --landing /tmp/fake_volume --duration-sec 600

Local collector uploading straight to ADLS Gen2 (auth: DefaultAzureCredential,
e.g. AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET)::

    python -m src.collector --data-dir ~/polymarket_data \\
        --landing abfss://landing@myaccount.dfs.core.windows.net/polymarket

Tunables (markets per cycle, subscription cap, segment size, ...) come from
``src/config.py``; the command line only says where to run and for how long.

Databricks notebook (the notebook already runs an event loop, so await)::

    from src.collector import run
    stats = await run(data_dir="/tmp/polymarket",
                      landing="/Volumes/polymarket/raw/landing",
                      host_id="dbx", duration_sec=4 * 3600)

Shutdown order matters and is fixed by the nesting below:

    1. stop the orchestrator      no new messages
    2. exit the writer            closes and seals every active segment
    3. exit the uploader          uploads what is left

Stops when ``duration_sec`` elapses, when ``stop_event`` is set, when the
running task is cancelled once (e.g. notebook Interrupt), or (command
line only) on SIGTERM / SIGINT. All of these return stats. A kill that skips Python cleanup (SIGKILL,
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
from collections.abc import Callable
from functools import partial
from pathlib import Path

from src import config
from src.orchestrator import Orchestrator
from src.wal.uploader import PutFn, Uploader, put_file
from src.wal.writer import Walwriter

logger = logging.getLogger(__name__)

ADLS_SCHEME = "abfss://"


def make_put(landing: str | Path, data_dir: Path) -> tuple[PutFn, Callable[[], None]]:
    """
    Pick the upload function for ``landing``. Returns (put, close).

    - ``abfss://<container>@<account>.dfs.core.windows.net[/<folder>]``
      -> ``AdlsPut`` (ADLS Gen2, staging + atomic rename)
    - anything else -> a directory (UC volume path or local dir), ``put_file``

    ``src.wal.adls`` (and the azure SDK) is imported only for ADLS, so the
    Databricks notebook runs without the azure packages.
    """
    if str(landing).startswith(ADLS_SCHEME):
        from src.wal.adls import AdlsPut
        put = AdlsPut(str(landing))
        return put, put.close
    dest = Path(landing)
    if data_dir.resolve() == dest.resolve():
        raise ValueError("data_dir and landing must be different directories")
    return partial(put_file, dest_root=dest), (lambda: None)


async def run(
    data_dir: Path | str,
    landing: str | Path,
    *,
    host_id: str | None = None,
    duration_sec: float | None = None,
    keep_local: bool = False,
    stop_event: asyncio.Event | None = None,
    handle_signals: bool = False,
) -> dict:
    """
    Run the collector until ``duration_sec`` elapses (None = forever),
    ``stop_event`` is set or the task is cancelled once, then shut down
    cleanly. A second cancel during shutdown aborts it. Returns writer and
    uploader stats; ``stats["uploader"]["pending"] > 0`` means some sealed
    files did not reach ``landing``.

    data_dir:  local working dir for the WAL (must support appends:
               local disk, not /Volumes).
    landing:   destination root: a UC volume path on Databricks, any
               directory for local runs, or an abfss:// URL to upload to
               ADLS Gen2 (see make_put).
    handle_signals: turn SIGTERM / SIGINT into a clean stop. Leave False in
               notebooks, where the kernel owns SIGINT.

    All tunables (markets per cycle, subscription cap, segment size, ...)
    come from ``src/config.py``. They are read here, at call time, and passed
    down explicitly, so changing ``config.X`` before calling run() takes
    effect (the components' own defaults are fixed at import time).
    """
    data_dir = Path(data_dir)
    # Before anything starts, so a bad landing fails fast. Kept as given (not
    # Path()): Path would collapse the "//" of an abfss:// URL.
    put, close_put = make_put(landing, data_dir)
    host_id = host_id or default_host_id()
    stop = stop_event or asyncio.Event()

    remove_handlers = _install_signal_handlers(stop) if handle_signals else (lambda: None)
    try:
        async with Uploader(
            data_dir, put, host_id,
            keep_local=keep_local,
            scan_interval_sec=config.UPLOAD_SCAN_INTERVAL_SEC,
            exit_timeout_sec=config.UPLOAD_EXIT_TIMEOUT_SEC,
        ) as up:
            async with Walwriter(
                data_dir,
                segment_max_bytes=config.SEGMENT_MAX_BYTES,
                fsync_interval_sec=config.FSYNC_INTERVAL_SEC,
                check_interval_sec=config.CHECK_INTERVAL_SEC,
            ) as wal:
                orch = Orchestrator(
                    wal=wal,
                    n_markets=config.TOP_N_MARKETS,
                    max_subscribed_markets=config.MAX_SUBSCRIBED_MARKETS,
                    interval_sec=config.DISCOVERY_INTERVAL_SEC,
                    missing_strikes_to_remove=config.MISSING_STRIKES_TO_REMOVE,
                )
                await orch.start()
                logger.info(
                    "collector running (data_dir=%s, landing=%s, n_markets=%d, "
                    "max_subscribed_markets=%d, duration=%s)",
                    data_dir, landing, config.TOP_N_MARKETS, config.MAX_SUBSCRIBED_MARKETS,
                    f"{duration_sec:.0f}s" if duration_sec is not None else "unlimited",
                )
                try:
                    await _wait_for_stop(stop, duration_sec)
                finally:
                    await orch.stop()
            # Writer has exited: every segment is sealed; the uploader exit
            # (next) uploads them.
        result = {"wal": wal.stats(), "uploader": up.stats()}
    finally:
        remove_handlers()
        close_put()

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
    except asyncio.CancelledError:
        # Notebook "Interrupt" cancels the cell. Treat the first cancel as a
        # stop request so run() still shuts down cleanly and returns stats.
        # uncancel() clears the request; a second cancel during shutdown
        # still aborts.
        task = asyncio.current_task()
        if task is not None:
            task.uncancel()
        logger.info("collector: cancelled, shutting down cleanly")


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
    p.add_argument("--landing", required=True,
                   help="destination root: /Volumes/<catalog>/<schema>/<volume>, any directory, "
                        "or abfss://<container>@<account>.dfs.core.windows.net/<folder> (ADLS Gen2)")
    p.add_argument("--host-id", default=None,
                   help="prefix of the source id in remote file names (default: hostname)")
    p.add_argument("--duration-sec", type=float, default=None,
                   help="stop after this many seconds (default: run until SIGTERM/SIGINT)")
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
    stats = asyncio.run(run(
        args.data_dir, args.landing,
        host_id=args.host_id,
        duration_sec=args.duration_sec,
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
