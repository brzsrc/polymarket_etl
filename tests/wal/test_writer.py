"""
Tests for src/wal/writer.py (segment layout) and its compatibility with
src/replay_engine/wal_reader.py.

Run from the repo root:  .venv/bin/python -m pytest tests/wal -q
"""
import asyncio
import gzip
import json
import zipfile
from pathlib import Path

from src.common.utilities import Rec
from src.replay_engine.wal_reader import Dedup, _files, _records
from src.wal.writer import Walwriter

T0 = 1762560000_000000000          # 2025-11-08 00:00:00 UTC
DATE = "2025-11-08"


def rec(i: int, aids: tuple[str, ...] = ("111", "222")) -> Rec:
    return Rec(ts_recv_ns=T0 + i, asset_ids=aids, raw=json.dumps({"i": i}))


def names(d: Path) -> list[str]:
    return sorted(p.name for p in d.iterdir())


def gz_lines(p: Path) -> list[bytes]:
    return gzip.open(p, "rb").read().splitlines()


def write_gz(p: Path, data: bytes) -> None:
    with gzip.open(p, "wb") as f:
        f.write(data)


def run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

def test_basic_write_single_segment(tmp_path):
    async def go():
        async with Walwriter(tmp_path, seal_on_exit=False, segment_max_bytes=None) as w:
            await w.write(rec(0))
            await w.write(rec(1, ()))
            return w.stats()

    st = run(go())
    day = tmp_path / "wal" / DATE
    assert names(day) == ["000000.jsonl"]          # one file, no shard folders
    lines = (day / "000000.jsonl").read_bytes().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["asset_ids"] == ["111", "222"]   # multi-asset message written once
    assert json.loads(lines[1])["asset_ids"] == []               # no-route message still written
    assert st["messages"] == 2 and st["no_route"] == 1


def test_size_rotation_seals_and_continues(tmp_path):
    async def go():
        async with Walwriter(tmp_path, seal_on_exit=False, segment_max_bytes=200) as w:
            for i in range(5):
                await w.write(rec(i))
            await w.check_now()
            await w.wait_seals()
            await w.write(rec(99))

    run(go())
    day = tmp_path / "wal" / DATE
    assert names(day) == ["000000.jsonl.gz", "000001.jsonl"]
    assert len(gz_lines(day / "000000.jsonl.gz")) == 5
    assert json.loads((day / "000001.jsonl").read_bytes())["ts_recv_ns"] == T0 + 99


def test_restart_seals_leftover_byte_identical(tmp_path):
    async def first():
        async with Walwriter(tmp_path, seal_on_exit=False, segment_max_bytes=None) as w:
            await w.write(rec(0))
            await w.write(rec(1))

    async def second():
        async with Walwriter(tmp_path, seal_on_exit=False, segment_max_bytes=None) as w:
            await w.wait_seals()
            await w.write(rec(2))

    run(first())
    day = tmp_path / "wal" / DATE
    before = (day / "000000.jsonl").read_bytes()
    run(second())
    assert names(day) == ["000000.jsonl.gz", "000001.jsonl"]
    assert gzip.open(day / "000000.jsonl.gz").read() == before


def test_torn_tail_dropped(tmp_path):
    day = tmp_path / "wal" / DATE
    day.mkdir(parents=True)
    (day / "000000.jsonl").write_bytes(b'{"a":1}\n{"a":2}\n{"a":')

    async def go():
        async with Walwriter(tmp_path) as w:
            await w.wait_seals()
            return w.stats()

    st = run(go())
    assert gz_lines(day / "000000.jsonl.gz") == [b'{"a":1}', b'{"a":2}']
    assert st["torn_bytes"] == len(b'{"a":')


def test_archived_numbers_never_reused(tmp_path):
    day = tmp_path / "wal" / DATE
    day.mkdir(parents=True)
    write_gz(day / "000000.jsonl.gz", b"x\n")
    write_gz(day / "000001.jsonl.gz", b"y\n")

    async def go():
        async with Walwriter(tmp_path, seal_on_exit=False) as w:
            await w.write(rec(0))

    run(go())
    assert "000002.jsonl" in names(day)
    assert gz_lines(day / "000000.jsonl.gz") == [b"x"]   # existing archives untouched
    assert gz_lines(day / "000001.jsonl.gz") == [b"y"]


def test_existing_target_keeps_source(tmp_path):
    day = tmp_path / "wal" / DATE
    day.mkdir(parents=True)
    (day / "000000.jsonl").write_bytes(b"new\n")
    write_gz(day / "000000.jsonl.gz", b"old\n")

    async def go():
        async with Walwriter(tmp_path) as w:
            await w.wait_seals()

    run(go())
    assert (day / "000000.jsonl").read_bytes() == b"new\n"
    assert gz_lines(day / "000000.jsonl.gz") == [b"old"]


def test_orphan_tmp_removed(tmp_path):
    day = tmp_path / "wal" / DATE
    day.mkdir(parents=True)
    (day / "000001.jsonl.gz.tmp").write_bytes(b"junk")

    async def go():
        async with Walwriter(tmp_path):
            pass

    run(go())
    assert not (day / "000001.jsonl.gz.tmp").exists()


def test_legacy_layout_untouched(tmp_path):
    day = tmp_path / "wal" / DATE
    (day / "gen0").mkdir(parents=True)
    (day / "gen0" / "1.jsonl").write_bytes(b"legacy\n")
    with zipfile.ZipFile(day / "archive_000.zip", "w") as z:
        z.writestr("1.jsonl", "legacy\n")

    async def go():
        async with Walwriter(tmp_path, segment_max_bytes=50) as w:
            await w.write(rec(0))
            await w.check_now()
            await w.wait_seals()

    run(go())
    assert (day / "gen0" / "1.jsonl").read_bytes() == b"legacy\n"
    assert (day / "archive_000.zip").exists()


def test_markets_written_as_segment(tmp_path):
    async def go():
        async with Walwriter(tmp_path, seal_on_exit=False, segment_max_bytes=None) as w:
            await w.write_market_snapshot(T0, [{"market_id": "m1", "raw": {"id": "m1"}}, {"market_id": "m2", "raw": {"id": "m2"}}])
            return w.stats()

    st = run(go())
    mdir = tmp_path / "markets" / DATE
    assert names(mdir) == ["000000.jsonl"]
    lines = [json.loads(l) for l in (mdir / "000000.jsonl").read_text().splitlines()]
    assert [l["raw"]["id"] for l in lines] == ["m1", "m2"]
    assert all(l["ts_recv_ns"] == T0 for l in lines)
    assert st["market_records"] == 2


def test_market_record_fields_written_as_is(tmp_path):
    # The writer adds ts_recv_ns and copies every record field unchanged.
    record = {"market_id": "6236747", "source": "lookup", "rank": None, "raw": None,
              "sub": {"action": "removed", "reason": "ended", "missing_strikes": 0}}

    async def go():
        async with Walwriter(tmp_path, seal_on_exit=False, segment_max_bytes=None) as w:
            await w.write_market_snapshot(T0, [record])

    run(go())
    line = json.loads((tmp_path / "markets" / DATE / "000000.jsonl").read_text())
    assert line == {"ts_recv_ns": T0, **record}


def test_events_written_to_own_stream(tmp_path):
    async def go():
        async with Walwriter(tmp_path, segment_max_bytes=None) as w:
            await w.write(rec(0))
            await w.write_event(T0 + 1, {"conn_id": 0, "event": "connected",
                                         "extra": {"assets": ["111", "222"], "asset_count": 2}})
            await w.write_event(T0 + 2, {"conn_id": 0, "event": "disconnected", "extra": {}})
            return w.stats()

    st = run(go())
    assert names(tmp_path / "events" / DATE) == ["000000.jsonl.gz"]   # sealed on exit like the others
    lines = [json.loads(l) for l in gz_lines(tmp_path / "events" / DATE / "000000.jsonl.gz")]
    assert [(l["ts_ns"], l["event"]) for l in lines] == [(T0 + 1, "connected"), (T0 + 2, "disconnected")]
    assert lines[0]["extra"]["assets"] == ["111", "222"]
    assert st["events"] == 2 and st["messages"] == 1


def test_metrics_written_to_own_stream(tmp_path):
    async def go():
        async with Walwriter(tmp_path, segment_max_bytes=None) as w:
            await w.write(rec(0))
            await w.write_metrics(T0 + 1, {"msgs_total": 1, "errors": {"disconnects": 0}})
            await w.write_metrics(T0 + 2, {"msgs_total": 5, "errors": {"disconnects": 1}})
            return w.stats()

    st = run(go())
    assert names(tmp_path / "metrics" / DATE) == ["000000.jsonl.gz"]   # sealed on exit like the others
    lines = [json.loads(l) for l in gz_lines(tmp_path / "metrics" / DATE / "000000.jsonl.gz")]
    assert [(l["ts_ns"], l["msgs_total"]) for l in lines] == [(T0 + 1, 1), (T0 + 2, 5)]
    assert lines[1]["errors"] == {"disconnects": 1}
    assert st["metrics"] == 2 and st["messages"] == 1


def test_metrics_leftover_from_crash_sealed_on_start(tmp_path):
    # A crash leaves the active metrics segment as .jsonl; the next start seals it
    # (this used to be a metrics.jsonl that the restart truncated).
    d = tmp_path / "metrics" / DATE
    d.mkdir(parents=True)
    (d / "000000.jsonl").write_bytes(b'{"ts_ns":1,"msgs_total":7}\n{"ts_ns":2,"msg')   # torn tail

    async def go():
        async with Walwriter(tmp_path, segment_max_bytes=None) as w:
            await w.wait_seals()
            await w.write_metrics(T0, {"msgs_total": 0})

    run(go())
    assert names(d) == ["000000.jsonl.gz", "000001.jsonl.gz"]       # old kept, new number
    assert [json.loads(l)["msgs_total"] for l in gz_lines(d / "000000.jsonl.gz")] == [7]


def test_markets_rotate_and_seal_like_wal(tmp_path):
    async def go():
        async with Walwriter(tmp_path, seal_on_exit=False, segment_max_bytes=100) as w:
            await w.write_market_snapshot(T0, [{"market_id": f"m{i}", "raw": {"id": f"m{i}"}} for i in range(5)])
            await w.check_now()
            await w.wait_seals()
            await w.write_market_snapshot(T0 + 1, [{"market_id": "late", "raw": {"id": "late"}}])

    run(go())
    mdir = tmp_path / "markets" / DATE
    assert names(mdir) == ["000000.jsonl.gz", "000001.jsonl"]
    assert len(gz_lines(mdir / "000000.jsonl.gz")) == 5
    assert json.loads((mdir / "000001.jsonl").read_bytes())["raw"]["id"] == "late"


def test_streams_are_independent(tmp_path):
    # Only the WS stream is over the limit: markets must not rotate, and
    # each stream keeps its own numbering.
    async def go():
        async with Walwriter(tmp_path, seal_on_exit=False, segment_max_bytes=300) as w:
            for i in range(5):
                await w.write(rec(i))
            await w.write_market_snapshot(T0, [{"market_id": "m1", "raw": {"id": "m1"}}])
            await w.check_now()
            await w.wait_seals()
            await w.write(rec(5))
            await w.write_market_snapshot(T0 + 5, [{"market_id": "m2", "raw": {"id": "m2"}}])
            return w.stats()

    st = run(go())
    assert names(tmp_path / "wal" / DATE) == ["000000.jsonl.gz", "000001.jsonl"]
    assert names(tmp_path / "markets" / DATE) == ["000000.jsonl"]
    assert len((tmp_path / "markets" / DATE / "000000.jsonl").read_bytes().splitlines()) == 2
    assert st["current_segment"] == {f"wal/{DATE}": 1, f"markets/{DATE}": 0}


def test_markets_leftover_sealed_on_restart(tmp_path):
    async def first():
        async with Walwriter(tmp_path, seal_on_exit=False, segment_max_bytes=None) as w:
            await w.write_market_snapshot(T0, [{"market_id": "m1", "raw": {"id": "m1"}}])

    async def second():
        async with Walwriter(tmp_path, seal_on_exit=False, segment_max_bytes=None) as w:
            await w.wait_seals()
            await w.write_market_snapshot(T0 + 1, [{"market_id": "m2", "raw": {"id": "m2"}}])

    run(first())
    run(second())
    mdir = tmp_path / "markets" / DATE
    assert names(mdir) == ["000000.jsonl.gz", "000001.jsonl"]
    assert json.loads(gz_lines(mdir / "000000.jsonl.gz")[0])["raw"]["id"] == "m1"


def test_legacy_markets_file_untouched(tmp_path):
    (tmp_path / "markets").mkdir()
    legacy = tmp_path / "markets" / f"{DATE}.jsonl"           # old one-file-per-day layout
    legacy.write_bytes(b'{"ts_ns":1,"raw":{}}\n')

    async def go():
        async with Walwriter(tmp_path, segment_max_bytes=10) as w:
            await w.write_market_snapshot(T0, [{"market_id": "m1", "raw": {"id": "m1"}}])
            await w.check_now()
            await w.wait_seals()

    run(go())
    assert legacy.read_bytes() == b'{"ts_ns":1,"raw":{}}\n'
    assert (tmp_path / "markets" / DATE / "000000.jsonl.gz").exists()


def test_concurrent_writes_with_rotation_lose_nothing(tmp_path):
    async def go():
        async with Walwriter(
            tmp_path, segment_max_bytes=2000, check_interval_sec=0.01, fsync_interval_sec=0.01,
        ) as w:
            async def producer(k):
                for i in range(500):
                    await w.write(rec(k * 1000 + i))
                    if i % 50 == 0:
                        await asyncio.sleep(0.005)    # let the check loop rotate mid-stream
            await asyncio.gather(*(producer(k) for k in range(4)))
        async with Walwriter(tmp_path) as w:           # restart seals the last active segment
            await w.wait_seals()

    run(go())
    day = tmp_path / "wal" / DATE
    sealed = sorted(day.glob("*.jsonl.gz"))
    seen = [json.loads(l)["ts_recv_ns"] for p in sealed for l in gz_lines(p)]
    assert len(sealed) > 1                             # rotation actually happened
    assert not list(day.glob("*.jsonl"))
    assert len(seen) == 2000 and len(set(seen)) == 2000


# ---------------------------------------------------------------------------
# Reading the output with wal_reader
# ---------------------------------------------------------------------------

def _line(i: int) -> bytes:
    return json.dumps({"ts_recv_ns": T0 + i, "asset_ids": ["111"], "raw": json.dumps({"i": i})}).encode() + b"\n"


def test_reader_mixed_old_and_new_layouts(tmp_path):
    day = tmp_path / "wal" / DATE
    (day / "gen0").mkdir(parents=True)
    (day / "gen0" / "1.jsonl").write_bytes(_line(0) + _line(4))            # legacy live shard
    with zipfile.ZipFile(day / "archive_000.zip", "w") as z:                # legacy archive
        z.writestr("1.jsonl", (_line(1) + _line(5)).decode())
    write_gz(day / "000000.jsonl.gz", _line(2) + _line(6))                 # new sealed segment
    (day / "000001.jsonl").write_bytes(_line(3) + _line(7) + b'{"ts_recv')  # new active segment, torn tail

    got = [r.ts_recv_ns - T0 for r in _records(_files(tmp_path, DATE), None, Dedup(0))]
    assert got == list(range(8))


def test_reader_skips_jsonl_while_its_gz_exists(tmp_path):
    # Window inside a seal: rename done, source not yet deleted.
    day = tmp_path / "wal" / DATE
    day.mkdir(parents=True)
    (day / "000000.jsonl").write_bytes(_line(0) + _line(1))
    write_gz(day / "000000.jsonl.gz", _line(0) + _line(1))

    files = _files(tmp_path, DATE)
    assert [Path(f).name for f in files] == ["000000.jsonl.gz"]
    got = [r.ts_recv_ns - T0 for r in _records(files, None, Dedup(0))]
    assert got == [0, 1]


def test_reader_falls_back_to_gz_if_segment_sealed_after_listing(tmp_path):
    day = tmp_path / "wal" / DATE
    day.mkdir(parents=True)
    (day / "000000.jsonl").write_bytes(_line(0))
    files = _files(tmp_path, DATE)                     # listed while still .jsonl
    write_gz(day / "000000.jsonl.gz", _line(0))        # then sealed...
    (day / "000000.jsonl").unlink()                    # ...and the source deleted

    got = [r.ts_recv_ns - T0 for r in _records(files, None, Dedup(0))]
    assert got == [0]


def test_reader_reads_real_writer_output(tmp_path):
    async def go():
        async with Walwriter(tmp_path, seal_on_exit=False, segment_max_bytes=300) as w:
            for i in range(10):
                await w.write(rec(i))
                await w.check_now()
            await w.wait_seals()

    run(go())
    day = tmp_path / "wal" / DATE
    assert len(list(day.glob("*.jsonl.gz"))) >= 2 and len(list(day.glob("*.jsonl"))) == 1
    got = [r.ts_recv_ns - T0 for r in _records(_files(tmp_path, DATE), None, Dedup(0))]
    assert got == list(range(10))


# ---------------------------------------------------------------------------
# Sealing on exit (default seal_on_exit=True)
# ---------------------------------------------------------------------------

def test_exit_seals_every_stream_and_date(tmp_path):
    next_day = T0 + 86_400 * 1_000_000_000

    async def go():
        async with Walwriter(tmp_path, segment_max_bytes=None) as w:
            await w.write(rec(0))
            await w.write(Rec(ts_recv_ns=next_day, asset_ids=("111",), raw="{}"))
            await w.write_market_snapshot(T0, [{"market_id": "m1", "raw": {"id": "m1"}}])
        return w.stats()

    st = run(go())
    assert names(tmp_path / "wal" / DATE) == ["000000.jsonl.gz"]
    assert names(tmp_path / "wal" / "2025-11-09") == ["000000.jsonl.gz"]
    assert names(tmp_path / "markets" / DATE) == ["000000.jsonl.gz"]
    assert json.loads(gz_lines(tmp_path / "markets" / DATE / "000000.jsonl.gz")[0])["raw"]["id"] == "m1"
    assert st["sealed"] == 3
    assert not list(tmp_path.glob("*/*/*.jsonl")) and not list(tmp_path.glob("*/*/*.tmp"))


def test_exit_without_writes_creates_nothing(tmp_path):
    async def go():
        async with Walwriter(tmp_path):
            pass

    run(go())
    assert not list(tmp_path.glob("*/*/*"))


def test_exit_waits_for_rotation_seal_and_seals_active(tmp_path):
    async def go():
        async with Walwriter(tmp_path, segment_max_bytes=200) as w:
            for i in range(5):
                await w.write(rec(i))
            await w.check_now()                  # rotation seal still in flight
            await w.write(rec(5))                # goes to the new active segment
        return w.stats()

    st = run(go())
    day = tmp_path / "wal" / DATE
    assert names(day) == ["000000.jsonl.gz", "000001.jsonl.gz"]
    assert [json.loads(l)["ts_recv_ns"] - T0 for p in sorted(day.iterdir()) for l in gz_lines(p)] == list(range(6))
    assert st["sealing"] == 0


def test_restart_after_exit_seal_continues_numbering(tmp_path):
    async def once(i):
        async with Walwriter(tmp_path, segment_max_bytes=None) as w:
            await w.write(rec(i))

    run(once(0))
    run(once(1))
    day = tmp_path / "wal" / DATE
    assert names(day) == ["000000.jsonl.gz", "000001.jsonl.gz"]
    assert json.loads(gz_lines(day / "000001.jsonl.gz")[0])["ts_recv_ns"] == T0 + 1


def test_exit_retries_a_seal_that_failed_earlier(tmp_path, monkeypatch):
    import src.wal.writer as writer_mod

    day = tmp_path / "wal" / DATE
    day.mkdir(parents=True)
    (day / "000000.jsonl").write_bytes(b'{"a":1}\n')

    real = writer_mod._seal_file
    calls = []

    def flaky(src):
        calls.append(src.name)
        if len(calls) == 1:
            raise OSError("disk hiccup")
        return real(src)

    monkeypatch.setattr(writer_mod, "_seal_file", flaky)

    async def go():
        async with Walwriter(tmp_path) as w:     # start-up seal fails...
            await w.wait_seals()
            assert (day / "000000.jsonl").exists()
        # ...and the exit pass retries it

    run(go())
    assert calls == ["000000.jsonl", "000000.jsonl"]
    assert names(day) == ["000000.jsonl.gz"]


def test_seal_on_exit_false_keeps_active_jsonl(tmp_path):
    async def go():
        async with Walwriter(tmp_path, seal_on_exit=False) as w:
            await w.write(rec(0))

    run(go())
    assert names(tmp_path / "wal" / DATE) == ["000000.jsonl"]


def test_upload_markers_count_for_numbering(tmp_path):
    # The uploader deletes the .gz and leaves NNNNNN.jsonl.gz.uploaded; the
    # writer must still treat that number as used.
    day = tmp_path / "wal" / DATE
    day.mkdir(parents=True)
    (day / "000000.jsonl.gz.uploaded").touch()
    (day / "000001.jsonl.gz.uploaded").touch()

    async def go():
        async with Walwriter(tmp_path, seal_on_exit=False) as w:
            await w.write(rec(0))

    run(go())
    assert "000002.jsonl" in names(day)
