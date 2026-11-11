"""
src/ingestor/bronze.py 的测试，用本地 Spark 跑。

输入文件按 collector 的真实流程生成：真的 Walwriter 写入、真的 Uploader 改名
上传到临时 landing 目录。所以 writer / uploader 的输出格式一变，这里就会失败。

本地 Spark 没有 Databricks 专有的部分，这些测不到：
Auto Loader（cloudFiles）、rescuedDataColumn、Unity Catalog 三层表名、
cloud_files_state。读取改用同一个 schema 的批量读取。

需要 pyspark 和 Java；没有就整个文件跳过。
"""
from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from datetime import datetime, timezone
from functools import partial
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from src.common.utilities import Rec  # noqa: E402
from src.ingestor import bronze as b  # noqa: E402
from src.wal.uploader import Uploader, put_file  # noqa: E402
from src.wal.writer import Walwriter  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]


def ns(iso: str) -> int:
    """'2026-11-10T23:59:59.900' (UTC) -> epoch 纳秒"""
    dt = datetime.fromisoformat(iso).replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000) * 1000


# 跨 UTC 午夜：ws 会落在两个日期目录里
T_BEFORE_MIDNIGHT = ns("2026-11-10T23:59:59.900")
T_AFTER_MIDNIGHT = ns("2026-11-11T00:00:00.100")

GAMMA_MARKET = {
    "id": "3215007",
    "question": "Will there be no change in Fed interest rates after the December 2026 meeting?",
    "volume": "5025714.820925999",
    "volume24hr": 1312057.5232129998,
    "acceptingOrders": True,
    "outcomes": "[\"Yes\", \"No\"]",
    "clobTokenIds": "[\"428657\", \"807735\"]",
    "events": [{"id": "770450", "ticker": "fed-decision-in-december"}],
    "feeSchedule": {"exponent": 1, "rate": 0.05, "takerOnly": True},
}

# 和 orchestrator 实际写出的形状一致（见 writer.write_market_snapshot 的 docstring）
MARKET_RECORDS = [
    {"market_id": "3215007", "source": "top", "rank": 1,
     "sub": {"action": "added", "reason": None, "missing_strikes": 0}, "raw": GAMMA_MARKET},
    {"market_id": "999", "source": "lookup", "rank": None,
     "sub": {"action": "removed", "reason": "not_found", "missing_strikes": 3}, "raw": None},
]

EVENTS = [
    {"conn_id": 0, "event": "connected", "extra": {"assets": ["428657", "807735"], "asset_count": 2}},
    {"conn_id": 0, "event": "closed", "extra": {}},
]

METRICS_SAMPLE = {
    "t_rel_s": 1.0, "msgs_total": 10, "msgs_per_sec_window": 10, "bytes_per_sec_window": 4096,
    "subscribed_markets": 1, "subscribed_tokens": 2,
    "lat_window_ms": {"n": 10, "mean": 9.5, "median": 9.0, "p95": 15.0, "max": 20.0},
    "errors": {"json_decode": 0, "missing_timestamp": 0, "disconnects": 0, "reconnects": 0, "watchdog": 0},
    "errors_per_conn": {"0": 0}, "msgs_per_conn": {"0": 10},
}

WS_MESSAGES = [
    Rec(ts_recv_ns=T_BEFORE_MIDNIGHT, asset_ids=("428657",),
        raw='[{"event_type":"book","asset_id":"428657"}]'),
    Rec(ts_recv_ns=T_AFTER_MIDNIGHT, asset_ids=("428657", "807735"),
        raw='{"event_type":"price_change","price_changes":[]}'),
    Rec(ts_recv_ns=T_AFTER_MIDNIGHT + 1, asset_ids=(), raw='{"event_type":"unknown"}'),
]

EXPECTED_ROWS = {"ws_raw": 3, "markets_raw": 2, "events_raw": 2, "metrics_raw": 1}


# =============================================================================
# fixtures
# =============================================================================

@pytest.fixture(scope="module")
def landing(tmp_path_factory) -> tuple[Path, str]:
    """跑一遍 Walwriter + Uploader，返回 (landing 目录, source_id)。"""
    root = tmp_path_factory.mktemp("bronze")
    data_dir, landing_dir = root / "data", root / "landing"

    async def produce() -> str:
        put = partial(put_file, dest_root=landing_dir)
        # 嵌套顺序和 collector 一致：writer 先退出（封存），uploader 再把剩下的全部上传
        async with Uploader(data_dir, put, "test-host") as up:
            async with Walwriter(data_dir=data_dir) as wal:
                for rec in WS_MESSAGES:
                    await wal.write(rec)
                await wal.write_market_snapshot(T_AFTER_MIDNIGHT, MARKET_RECORDS)
                for i, e in enumerate(EVENTS):
                    await wal.write_event(T_AFTER_MIDNIGHT + i, e)
                await wal.write_metrics(T_AFTER_MIDNIGHT, METRICS_SAMPLE)
            return up.source_id

    source_id = asyncio.run(produce())
    return landing_dir, source_id


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    try:
        session = (
            SparkSession.builder.master("local[1]")
            .appName("test_bronze")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
            # 故意用非 UTC 时区：recv_date 必须仍按 UTC 算
            .config("spark.sql.session.timeZone", "America/Los_Angeles")
            .getOrCreate()
        )
    except Exception as e:  # 没有 Java 等
        pytest.skip(f"cannot start local Spark: {e}")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def read_like_bronze(spark, landing_dir: Path, table: str):
    """
    本地替代 read_stream()：同一个 schema 读同一批文件，再走真的 add_columns()。
    本地没有 rescuedDataColumn，所以手动补一个全 NULL 的 _rescued_data。
    """
    df = (
        spark.read.schema(b.SCHEMAS[table])
        .json(f"{landing_dir}/{b.TABLE_DIRS[table]}/*/*.jsonl.gz")
        .select("*", "_metadata")
        .withColumn("_rescued_data", F.lit(None).cast("string"))
    )
    return b.add_columns(df, table)


@pytest.fixture(scope="module")
def tables(spark, landing) -> dict[str, list]:
    """按 DDL 建表、写入，返回每张表读回来的所有行。"""
    landing_dir, _ = landing
    spark.sql("CREATE DATABASE IF NOT EXISTS test_bronze")
    out = {}
    for t in b.TABLE_DIRS:
        spark.sql(f"DROP TABLE IF EXISTS test_bronze.{t}")
        spark.sql(f"CREATE TABLE test_bronze.{t} ({b.DDL[t]}) USING parquet CLUSTER BY (recv_date)")
        read_like_bronze(spark, landing_dir, t).write.insertInto(f"test_bronze.{t}")
        out[t] = spark.table(f"test_bronze.{t}").collect()
    return out


# =============================================================================
# 不需要 Spark 的部分
# =============================================================================

def test_import_has_no_side_effects():
    # 新进程里 import：不应该创建 Spark session（也就不会建表、启动流）
    code = (
        "import src.ingestor.bronze\n"
        "from pyspark.sql import SparkSession\n"
        "assert SparkSession.getActiveSession() is None\n"
    )
    subprocess.run([sys.executable, "-c", code], cwd=REPO_ROOT, check=True, timeout=120)


def test_names():
    assert b.fq("ws_raw") == f"{b.CATALOG}.{b.SCHEMA}.ws_raw"
    assert b.source_dir("ws_raw") == f"{b.LANDING_ROOT}/ws/"
    assert b.checkpoint_dir("ws_raw") == f"{b.CHECKPOINT_ROOT}/bronze_ws_raw/"


@pytest.mark.parametrize("path, expected", [
    ("dbfs:/Volumes/p/raw/landing/ws/2026-11-11/MacBook-Pro-86-local-72c8cfd5-2026-11-11-000003.jsonl.gz",
     ("2026-11-11", "MacBook-Pro-86-local-72c8cfd5", "000003")),
    ("/landing/ws/2026-11-11/host.local-2026-11-11-000003.jsonl.gz", None),   # source_id 里有 '.'
    ("/landing/ws/2026-11-11/host-2026-11-11-3.jsonl.gz", None),              # 编号不是 6 位
    ("/landing/markets/2026-11-11/host-2026-11-11-000003.jsonl.gz", None),    # 目录不对
    ("/landing/ws/2026-11-11/host-2026-11-11-000003.jsonl", None),            # 没有 .gz
])
def test_path_re(path, expected):
    import re
    m = re.search(b.path_re("ws"), path)
    assert (m.groups() if m else None) == expected


class FakeQuery:
    def __init__(self, err: Exception | None = None, rows=()):
        self.err = err
        self.recentProgress = [{"numInputRows": r} for r in rows]
        self.waited = False

    def awaitTermination(self):
        self.waited = True
        if self.err:
            raise self.err


def test_wait_all_sums_batches(capsys):
    b.wait_all({"ws_raw": FakeQuery(rows=[100, 50, 0])})
    assert "rows=150" in capsys.readouterr().out


def test_wait_all_waits_for_every_stream_then_raises():
    boom = ValueError("boom")
    qs = {"ws_raw": FakeQuery(err=boom), "markets_raw": FakeQuery(rows=[1]), "events_raw": FakeQuery(rows=[2])}
    with pytest.raises(RuntimeError, match="ws_raw") as exc:
        b.wait_all(qs)
    assert exc.value.__cause__ is boom
    assert all(q.waited for q in qs.values())


# =============================================================================
# 本地 Spark：读取 schema、add_columns、DDL
# =============================================================================

def test_landing_has_every_stream(landing):
    landing_dir, _ = landing
    for d in b.TABLE_DIRS.values():
        assert list(landing_dir.glob(f"{d}/*/*.jsonl.gz")), f"no files for {d}"


@pytest.mark.parametrize("table", list(b.TABLE_DIRS))
def test_ddl_matches_add_columns_output(spark, landing, table):
    # 表结构（DDL）和写入的 DataFrame：列名、顺序、类型完全一致
    out = read_like_bronze(spark, landing[0], table)
    spark.sql("CREATE DATABASE IF NOT EXISTS ddl_check")
    spark.sql(f"DROP TABLE IF EXISTS ddl_check.{table}")
    spark.sql(f"CREATE TABLE ddl_check.{table} ({b.DDL[table]}) USING parquet")
    expected = [(f.name, f.dataType) for f in spark.table(f"ddl_check.{table}").schema]
    assert [(f.name, f.dataType) for f in out.schema] == expected


@pytest.mark.parametrize("table", list(b.TABLE_DIRS))
def test_rows_and_source_columns(tables, landing, table):
    _, source_id = landing
    rows = tables[table]
    assert len(rows) == EXPECTED_ROWS[table]
    for r in rows:
        assert r.source_id == source_id
        assert r.wal_date != ""                       # 路径能被 path_re 解析
        assert isinstance(r.wal_seg, int)
        assert r.wal_date == r.recv_date.isoformat()  # 目录日期 = 行的 UTC 日期
        assert r._source_file.endswith(".jsonl.gz")
        assert r._rescued_data is None


def test_recv_date_is_utc_across_midnight(tables):
    # session 时区是洛杉矶，但 recv_date 必须按 UTC：午夜前后落在不同的日期
    by_ns = {r.ts_recv_ns: r for r in tables["ws_raw"]}
    assert by_ns[T_BEFORE_MIDNIGHT].recv_date.isoformat() == "2026-11-10"
    assert by_ns[T_AFTER_MIDNIGHT].recv_date.isoformat() == "2026-11-11"
    assert {r.wal_date for r in tables["ws_raw"]} == {"2026-11-10", "2026-11-11"}


def test_ws_raw_kept_as_received(tables):
    assert "ts_recv_iso" not in tables["ws_raw"][0].asDict()   # 读进来后被丢掉
    by_ns = {r.ts_recv_ns: r for r in tables["ws_raw"]}
    for rec in WS_MESSAGES:
        row = by_ns[rec.ts_recv_ns]
        assert row.raw == rec.raw                       # 原文一个字符都不改
        assert tuple(row.asset_ids) == rec.asset_ids    # 空数组 = 未能路由


def test_markets_raw_object_stored_as_json_text(tables):
    rows = {r.market_id: r for r in tables["markets_raw"]}

    top = rows["3215007"]
    assert isinstance(top.raw, str)
    assert json.loads(top.raw) == GAMMA_MARKET      # 内容不变
    assert '": ' not in top.raw                      # 紧凑格式，空格被去掉
    assert (top.source, top.rank) == ("top", 1)
    assert top.sub.asDict() == {"action": "added", "reason": None, "missing_strikes": 0}

    lookup = rows["999"]
    assert lookup.raw is None                        # 查不到的市场
    assert lookup.rank is None
    assert lookup.sub.reason == "not_found"


def test_events_extra_stored_as_json_text(tables):
    rows = sorted(tables["events_raw"], key=lambda r: r.ts_ns)
    assert [r.event for r in rows] == ["connected", "closed"]
    assert json.loads(rows[0].extra) == EVENTS[0]["extra"]
    assert rows[1].extra == "{}"


def test_metrics_nested_fields(tables):
    (row,) = tables["metrics_raw"]
    assert row.subscribed_tokens == 2
    assert row.assets_seen is None                   # 旧版字段，新版不写
    assert row.lat_window_ms.p95 == 15.0
    assert row.errors.disconnects == 0
    assert row.msgs_per_conn == {"0": 10}
