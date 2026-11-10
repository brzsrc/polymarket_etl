"""
Tests for src/wal/uploader.py (put_file + Uploader), using a local directory
as the destination in place of a UC volume.

Run from the repo root:  .venv/bin/python -m pytest tests/wal -q -p no:anyio
"""
import asyncio
import gzip
import json
import logging
import shutil
from functools import partial
from pathlib import Path

import pytest

from src.common.utilities import Rec
from src.wal.uploader import Uploader, put_file
from src.wal.writer import Walwriter

T0 = 1762560000_000000000          # 2025-11-08 00:00:00 UTC
DATE = "2025-11-08"


def run(coro):
    return asyncio.run(coro)


def rec(i: int) -> Rec:
    return Rec(ts_recv_ns=T0 + i, asset_ids=("111",), raw=json.dumps({"i": i}))


def sealed(data_dir: Path, stream: str, n: int, body: bytes = b"x\n", date: str = DATE) -> Path:
    """Create a sealed segment as the writer would leave it."""
    d = data_dir / stream / date
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{n:06d}.jsonl.gz"
    with gzip.open(p, "wb") as f:
        f.write(body)
    return p


def files_under(root: Path) -> list[str]:
    return sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file())


def dir_put(dest: Path):
    return partial(put_file, dest_root=dest)


# ---------------------------------------------------------------------------
# put_file
# ---------------------------------------------------------------------------

def test_put_file_places_file_and_leaves_staging_empty(tmp_path):
    src = tmp_path / "a.gz"
    src.write_bytes(b"hello")
    dest = tmp_path / "dest"

    put_file(src, "ws/2025-11-08/x.gz", dest_root=dest)

    assert (dest / "ws/2025-11-08/x.gz").read_bytes() == b"hello"
    assert list((dest / "_staging").iterdir()) == []


def test_put_file_overwrites_existing_target(tmp_path):
    dest = tmp_path / "dest"
    (dest / "ws").mkdir(parents=True)
    (dest / "ws/x.gz").write_bytes(b"old")
    src = tmp_path / "a.gz"
    src.write_bytes(b"new")

    put_file(src, "ws/x.gz", dest_root=dest)

    assert (dest / "ws/x.gz").read_bytes() == b"new"


def test_put_file_failure_leaves_nothing(tmp_path, monkeypatch):
    src = tmp_path / "a.gz"
    src.write_bytes(b"hello")
    dest = tmp_path / "dest"

    def broken_copy(a, b):
        Path(b).write_bytes(b"hel")           # partial copy, then the "network" dies
        raise OSError("connection reset")

    monkeypatch.setattr(shutil, "copyfile", broken_copy)
    with pytest.raises(OSError):
        put_file(src, "ws/x.gz", dest_root=dest)

    assert not (dest / "ws/x.gz").exists()
    assert list((dest / "_staging").iterdir()) == []


def test_put_file_size_mismatch_is_an_error(tmp_path, monkeypatch):
    src = tmp_path / "a.gz"
    src.write_bytes(b"hello")
    dest = tmp_path / "dest"
    monkeypatch.setattr(shutil, "copyfile", lambda a, b: Path(b).write_bytes(b"hel"))

    with pytest.raises(OSError, match="size mismatch"):
        put_file(src, "ws/x.gz", dest_root=dest)
    assert not (dest / "ws/x.gz").exists()


@pytest.mark.parametrize("rel", ["", "/abs/x.gz", "../x.gz", "ws/../../x.gz"])
def test_put_file_rejects_bad_paths(tmp_path, rel):
    src = tmp_path / "a.gz"
    src.write_bytes(b"x")
    with pytest.raises(ValueError):
        put_file(src, rel, dest_root=tmp_path / "dest")
    assert not (tmp_path / "dest").exists()


def test_put_file_same_basename_in_two_streams(tmp_path):
    dest = tmp_path / "dest"
    a, b = tmp_path / "a", tmp_path / "b"
    a.write_bytes(b"ws")
    b.write_bytes(b"markets")
    put_file(a, "ws/d/x.gz", dest_root=dest)
    put_file(b, "markets/d/x.gz", dest_root=dest)
    assert (dest / "ws/d/x.gz").read_bytes() == b"ws"
    assert (dest / "markets/d/x.gz").read_bytes() == b"markets"


# ---------------------------------------------------------------------------
# Uploader
# ---------------------------------------------------------------------------

def test_uploads_both_streams_with_expected_names(tmp_path):
    data, dest = tmp_path / "data", tmp_path / "dest"
    sealed(data, "wal", 3, b"ws\n")
    sealed(data, "markets", 1, b"mk\n")

    async def go():
        async with Uploader(data, dir_put(dest), host_id="dbx") as up:
            return up.source_id

    sid = run(go())
    assert sid.startswith("dbx-")
    assert files_under(dest) == [
        f"markets/{DATE}/{sid}-{DATE}-000001.jsonl.gz",
        f"ws/{DATE}/{sid}-{DATE}-000003.jsonl.gz",
    ]
    assert gzip.open(dest / f"ws/{DATE}/{sid}-{DATE}-000003.jsonl.gz").read() == b"ws\n"


def test_local_gz_replaced_by_marker(tmp_path):
    data, dest = tmp_path / "data", tmp_path / "dest"
    p = sealed(data, "wal", 0)

    async def go():
        async with Uploader(data, dir_put(dest), host_id="dbx"):
            pass

    run(go())
    assert not p.exists()
    assert p.with_name(p.name + ".uploaded").exists()


def test_keep_local_keeps_gz(tmp_path):
    data, dest = tmp_path / "data", tmp_path / "dest"
    p = sealed(data, "wal", 0)

    async def go():
        async with Uploader(data, dir_put(dest), host_id="dbx", keep_local=True):
            pass

    run(go())
    assert p.exists() and p.with_name(p.name + ".uploaded").exists()


def test_failed_put_is_retried_and_uploaded_once(tmp_path):
    data, dest = tmp_path / "data", tmp_path / "dest"
    sealed(data, "wal", 0)
    real = dir_put(dest)
    calls = []

    def flaky(local, rel):
        calls.append(rel)
        if len(calls) == 1:
            raise OSError("volume hiccup")
        real(local, rel)

    async def go():
        async with Uploader(data, flaky, host_id="dbx", scan_interval_sec=0.01) as up:
            for _ in range(200):
                if up.stats()["uploaded"]:
                    break
                await asyncio.sleep(0.01)
            return up.stats()

    st = run(go())
    assert len(calls) == 2 and calls[0] == calls[1]
    assert st["uploaded"] == 1 and st["failures"] == 1
    assert len(list((dest / "ws").rglob("*.gz"))) == 1


def test_marked_files_are_not_uploaded_again(tmp_path):
    data, dest = tmp_path / "data", tmp_path / "dest"
    p = sealed(data, "wal", 0)
    p.with_name(p.name + ".uploaded").touch()       # e.g. crash between marker and delete
    calls = []

    async def go():
        async with Uploader(data, lambda l, r: calls.append(r), host_id="dbx"):
            pass

    run(go())
    assert calls == []


def test_ignores_active_tmp_and_legacy_files(tmp_path):
    data, dest = tmp_path / "data", tmp_path / "dest"
    day = data / "wal" / DATE
    (day / "gen0").mkdir(parents=True)
    (day / "000005.jsonl").write_bytes(b"active\n")
    (day / "000004.jsonl.gz.tmp").write_bytes(b"sealing")
    (day / "gen0" / "1.jsonl").write_bytes(b"legacy\n")
    (day / "archive_000.zip").write_bytes(b"PK")
    (data / "markets").mkdir()
    (data / "markets" / f"{DATE}.jsonl").write_bytes(b"old markets\n")
    calls = []

    async def go():
        async with Uploader(data, lambda l, r: calls.append(r), host_id="dbx"):
            pass

    run(go())
    assert calls == []


def test_source_id_persists_per_data_dir(tmp_path):
    async def sid(data):
        async with Uploader(data, lambda l, r: None, host_id="dbx") as up:
            return up.source_id

    a1 = run(sid(tmp_path / "a"))
    a2 = run(sid(tmp_path / "a"))
    b = run(sid(tmp_path / "b"))
    assert a1 == a2 and a1 != b


def test_bad_host_id_rejected(tmp_path):
    with pytest.raises(ValueError):
        Uploader(tmp_path, lambda l, r: None, host_id="a/b")


def test_exit_gives_up_after_timeout_and_logs(tmp_path, caplog):
    data = tmp_path / "data"
    p = sealed(data, "wal", 0)

    def always_fail(local, rel):
        raise OSError("volume down")

    async def go():
        async with Uploader(data, always_fail, host_id="dbx", exit_timeout_sec=0.2) as up:
            pass
        return up.stats()

    with caplog.at_level(logging.ERROR, logger="src.wal.uploader"):
        st = run(go())
    assert p.exists() and st["pending"] == 1 and st["failures"] >= 1
    assert "NOT uploaded at exit" in caplog.text


# ---------------------------------------------------------------------------
# With the writer
# ---------------------------------------------------------------------------

def test_with_writer_every_segment_reaches_destination(tmp_path):
    data, dest = tmp_path / "data", tmp_path / "dest"

    async def go():
        async with Uploader(data, dir_put(dest), host_id="dbx", scan_interval_sec=0.01) as up:
            async with Walwriter(data, segment_max_bytes=300, check_interval_sec=0.01) as w:
                for i in range(20):
                    await w.write(rec(i))
                    await asyncio.sleep(0.002)
                await w.write_market_snapshot(T0, [{"market_id": "m1", "raw": {"id": "m1"}}])
                await w.write_event(T0, {"conn_id": 0, "event": "connected", "extra": {}})
            # writer exited: its last segments were sealed on exit
        return up.stats()

    st = run(go())
    ws = sorted((dest / "ws" / DATE).glob("*.gz"))
    got = [json.loads(l)["ts_recv_ns"] - T0 for p in ws for l in gzip.open(p).read().splitlines()]
    assert len(ws) > 1                                  # rotation happened mid-run
    assert got == list(range(20))                       # nothing lost, in order, incl. the exit seal
    assert len(list((dest / "markets" / DATE).glob("*.gz"))) == 1
    assert len(list((dest / "events" / DATE).glob("dbx-*.jsonl.gz"))) == 1
    assert st["pending"] == 0
    assert not list(data.glob("*/*/*.jsonl")) and not list(data.glob("*/*/*.jsonl.gz"))
    assert list((dest / "_staging").iterdir()) == []


def test_restart_does_not_reuse_numbers_or_overwrite(tmp_path):
    # Persistent data dir (VM-like): local .gz are deleted after upload, only
    # markers remain. The second run must continue at 000001, not overwrite 000000.
    data, dest = tmp_path / "data", tmp_path / "dest"

    async def once(i):
        async with Uploader(data, dir_put(dest), host_id="vm") as up:
            async with Walwriter(data, segment_max_bytes=None) as w:
                await w.write(rec(i))
        return up.source_id

    sid1 = run(once(0))
    sid2 = run(once(1))
    assert sid1 == sid2
    names = sorted(p.name for p in (dest / "ws" / DATE).iterdir())
    assert names == [f"{sid1}-{DATE}-000000.jsonl.gz", f"{sid1}-{DATE}-000001.jsonl.gz"]
    first = gzip.open(dest / "ws" / DATE / names[0]).read()
    assert json.loads(first)["ts_recv_ns"] == T0             # first run's data intact
