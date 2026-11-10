"""
Tests for src/collector.py with a fake Orchestrator (no network).

Run from the repo root:  .venv/bin/python -m pytest tests/test_collector.py -q -p no:anyio
"""
import asyncio
import gzip
import json
import os
import signal

import pytest

import src.collector as collector
from src import config
from src.common.utilities import Rec, now_ns


class FakeOrchestrator:
    """Writes WS messages and a market snapshot through the real writer."""

    instances: list["FakeOrchestrator"] = []

    def __init__(self, out_dir, wal, **kwargs):
        self.out_dir, self.wal, self.kwargs = out_dir, wal, kwargs
        self.task = None
        self.stopped = False
        self.n_written = 0
        FakeOrchestrator.instances.append(self)

    async def start(self):
        (self.out_dir / "metrics.jsonl").write_text('{"m":1}\n')
        await self.wal.write_market_snapshot(now_ns(), [{"market_id": "m1", "raw": {"id": "m1"}}])
        await self.wal.write_event(now_ns(), {"conn_id": 0, "event": "connected", "extra": {}})
        self.task = asyncio.create_task(self._produce())

    async def _produce(self):
        while True:
            await self.wal.write(Rec(ts_recv_ns=now_ns(), asset_ids=("111",), raw=json.dumps({"i": self.n_written})))
            self.n_written += 1
            await asyncio.sleep(0.001)

    async def stop(self):
        self.task.cancel()
        try:
            await self.task
        except asyncio.CancelledError:
            pass
        self.stopped = True


@pytest.fixture(autouse=True)
def fake_orchestrator(monkeypatch):
    FakeOrchestrator.instances.clear()
    monkeypatch.setattr(collector, "Orchestrator", FakeOrchestrator)


def landed_ws_records(landing):
    out = []
    for p in sorted((landing / "ws").rglob("*.jsonl.gz")):
        out += [json.loads(l)["raw"] for l in gzip.open(p).read().splitlines()]
    return out


@pytest.fixture
def fast_config(monkeypatch):
    """Small segments and short intervals so a 0.3 s run rotates and uploads."""
    monkeypatch.setattr(config, "SEGMENT_MAX_BYTES", 2000)
    monkeypatch.setattr(config, "CHECK_INTERVAL_SEC", 0.02)
    monkeypatch.setattr(config, "UPLOAD_SCAN_INTERVAL_SEC", 0.02)


def test_run_lands_everything_and_cleans_up(tmp_path, fast_config, monkeypatch):
    monkeypatch.setattr(config, "TOP_N_MARKETS", 7)
    monkeypatch.setattr(config, "MAX_SUBSCRIBED_MARKETS", 9)
    data, landing = tmp_path / "data", tmp_path / "landing"

    stats = asyncio.run(collector.run(data, landing, host_id="t", duration_sec=0.3))

    orch = FakeOrchestrator.instances[0]
    # tunables are read from config at call time
    assert orch.stopped
    assert orch.kwargs["n_markets"] == 7 and orch.kwargs["max_subscribed_markets"] == 9
    # every message the fake wrote reached the destination, exactly once
    got = landed_ws_records(landing)
    assert len(got) == orch.n_written == stats["wal"]["messages"] > 0
    assert len(set(got)) == len(got)
    assert len(list((landing / "ws").rglob("*.gz"))) > 1          # rotated mid-run
    assert len(list((landing / "markets").rglob("*.gz"))) == 1
    assert len(list((landing / "events").rglob("*.gz"))) == 1     # events go through the WAL
    # run logs (metrics only) uploaded under _runs/{source_id}/
    sid = stats["uploader"]["source_id"]
    assert (landing / "_runs" / sid / "metrics.jsonl").read_text() == '{"m":1}\n'
    assert not (landing / "_runs" / sid / "events.jsonl").exists()
    # nothing left behind
    assert stats["uploader"]["pending"] == 0
    assert not list(data.glob("*/*/*.jsonl")) and not list(data.glob("*/*/*.jsonl.gz"))
    assert list((landing / "_staging").iterdir()) == []


def test_stop_event_ends_run_early(tmp_path):
    async def go():
        stop = asyncio.Event()
        task = asyncio.create_task(collector.run(
            tmp_path / "data", tmp_path / "landing", host_id="t",
            duration_sec=60, stop_event=stop,
        ))
        await asyncio.sleep(0.2)
        stop.set()
        return await asyncio.wait_for(task, 10)

    stats = asyncio.run(go())
    assert stats["uploader"]["pending"] == 0
    assert landed_ws_records(tmp_path / "landing")


def test_cancel_once_shuts_down_cleanly_and_returns_stats(tmp_path):
    # Notebook "Interrupt" cancels the cell's task.
    async def go():
        task = asyncio.create_task(collector.run(
            tmp_path / "data", tmp_path / "landing", host_id="t", duration_sec=60,
        ))
        await asyncio.sleep(0.2)
        task.cancel()
        stats = await asyncio.wait_for(task, 10)
        return stats, task.cancelling()

    stats, cancelling = asyncio.run(go())
    assert cancelling == 0
    assert FakeOrchestrator.instances[0].stopped
    assert stats["uploader"]["pending"] == 0
    assert len(landed_ws_records(tmp_path / "landing")) == stats["wal"]["messages"] > 0


def test_sigterm_triggers_clean_shutdown(tmp_path):
    async def go():
        loop = asyncio.get_running_loop()
        loop.call_later(0.2, os.kill, os.getpid(), signal.SIGTERM)
        return await asyncio.wait_for(collector.run(
            tmp_path / "data", tmp_path / "landing", host_id="t",
            duration_sec=60, handle_signals=True,
        ), 10)

    stats = asyncio.run(go())
    assert FakeOrchestrator.instances[0].stopped
    assert stats["uploader"]["pending"] == 0
    assert landed_ws_records(tmp_path / "landing")


def test_signal_handlers_removed_after_run(tmp_path):
    async def go():
        await collector.run(tmp_path / "data", tmp_path / "landing", host_id="t",
                            duration_sec=0.05, handle_signals=True)
        loop = asyncio.get_running_loop()
        # a handler still registered would make remove_signal_handler return True
        return loop.remove_signal_handler(signal.SIGTERM)

    assert asyncio.run(go()) is False


def test_same_data_dir_and_landing_rejected(tmp_path):
    with pytest.raises(ValueError):
        asyncio.run(collector.run(tmp_path, tmp_path, duration_sec=0))


def test_default_host_id_is_filename_safe():
    import re
    assert re.fullmatch(r"[A-Za-z0-9_-]+", collector.default_host_id())


def test_cli_returns_nonzero_when_files_left_behind(tmp_path, monkeypatch):
    def failing_put(local, rel, *, dest_root):
        raise OSError("volume down")

    monkeypatch.setattr(collector, "put_file", failing_put)
    monkeypatch.setattr(config, "UPLOAD_EXIT_TIMEOUT_SEC", 0.1)
    code = collector.main([
        "--data-dir", str(tmp_path / "data"), "--landing", str(tmp_path / "landing"),
        "--host-id", "t", "--duration-sec", "0.1",
    ])
    assert code == 1


# --- landing: local directory vs ADLS ------------------------------------------------

ADLS_URL = "abfss://landing@acct.dfs.core.windows.net/polymarket"


def test_make_put_local_dir(tmp_path):
    put, close = collector.make_put(str(tmp_path / "landing"), tmp_path / "data")
    assert put.func is collector.put_file and put.keywords == {"dest_root": tmp_path / "landing"}
    close()                                                     # no-op, must not raise


def test_make_put_adls_builds_adls_put(tmp_path):
    from src.wal.adls import AdlsPut
    put, close = collector.make_put(ADLS_URL, tmp_path / "data")
    try:
        assert isinstance(put, AdlsPut)
        assert put.location.file_system == "landing" and put.location.prefix == "polymarket"
        assert put.location.account_url == "https://acct.dfs.core.windows.net"
    finally:
        close()


def test_make_put_bad_adls_url_fails_fast(tmp_path):
    with pytest.raises(ValueError, match="not an ADLS URL"):
        collector.make_put("abfss://no-container.dfs.core.windows.net/x", tmp_path / "data")


def test_run_uploads_to_adls(tmp_path, fast_config, monkeypatch):
    # Real Walwriter + Uploader + AdlsPut; only the ADLS file system is faked.
    import src.wal.adls as adls
    from tests.wal.test_adls import FakeFs

    fs, closed = FakeFs(), []

    class TestAdlsPut(adls.AdlsPut):
        def __init__(self, url):
            super().__init__(url, file_system_client=fs)

        def close(self):
            closed.append(True)

    monkeypatch.setattr(adls, "AdlsPut", TestAdlsPut)

    stats = asyncio.run(collector.run(tmp_path / "data", ADLS_URL, host_id="t", duration_sec=0.3))

    sid = stats["uploader"]["source_id"]
    got = [json.loads(l)["raw"]
           for p in sorted(p for p in fs.files if p.startswith("polymarket/ws/"))
           for l in gzip.decompress(fs.files[p]).splitlines()]
    assert len(got) == stats["wal"]["messages"] > 0
    assert any(p.startswith("polymarket/markets/") for p in fs.files)
    assert any(p.startswith("polymarket/events/") for p in fs.files)
    assert fs.files[f"polymarket/_runs/{sid}/metrics.jsonl"] == b'{"m":1}\n'
    assert not any("/_staging/" in p for p in fs.files)
    assert stats["uploader"]["pending"] == 0
    assert closed == [True]                                     # HTTP client closed on exit


def test_cli_keeps_abfss_url_intact():
    args = collector._parse_args(["--data-dir", "/tmp/d", "--landing", ADLS_URL])
    assert args.landing == ADLS_URL                             # Path() would turn "//" into "/"
