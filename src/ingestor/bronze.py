# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion
# MAGIC
# MAGIC landing volume 里的 WAL 文件 → bronze Delta 表（Auto Loader，`availableNow`：把还没导入的文件处理完就结束）。
# MAGIC
# MAGIC | landing 目录 | bronze 表 |
# MAGIC |---|---|
# MAGIC | `ws/` | `bronze.ws_raw` |
# MAGIC | `markets/` | `bronze.markets_raw` |
# MAGIC | `events/` | `bronze.events_raw` |
# MAGIC | `metrics/` | `bronze.metrics_raw`（`INCLUDE_METRICS = True` 时） |
# MAGIC
# MAGIC - 在 `scripts/notebook/run_collector` 之后运行。重跑不会重复导入：每张表的 checkpoint 记着哪些文件已经导入过。
# MAGIC - 这个文件**同时是普通 Python 模块**：本地 `python -m src.ingestor.bronze` 也能跑，
# MAGIC   `tests/ingestor/test_bronze.py` import 它做测试。import 不会建表、也不会启动流；只有 `main()` 才会（最后一个 cell）。
# MAGIC - 所以这里**不要用 `dbutils`、`display()`、`%pip`**：本地没有这些，import 会失败。配置改下面的常量。

# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, LongType, MapType, StringType, StructField, StructType,
)

CATALOG = "polymarket"
SCHEMA = "bronze"
LANDING_ROOT = "/Volumes/polymarket/raw/landing"
CHECKPOINT_ROOT = "/Volumes/polymarket/raw/checkpoints"
INCLUDE_METRICS = True


# bronze 表名 -> landing 下的目录名
TABLE_DIRS = {"ws_raw": "ws", "markets_raw": "markets", "events_raw": "events"}
if INCLUDE_METRICS:
    TABLE_DIRS["metrics_raw"] = "metrics"

TABLE_COMMENT = {
    "ws_raw": "Polymarket WebSocket 原始消息，一行 WAL = 一行",
    "markets_raw": "每轮 market discovery 的结果，每个市场一行，Gamma 原始数据存成 JSON 文本",
    "events_raw": "WebSocket 连接事件（连接、订阅、退订、断线），用来找数据缺口",
    "metrics_raw": "采集端每秒监控快照，只用于监控",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 表结构（DDL）

# COMMAND ----------

# 四张表共用的来源列
SOURCE_COLS_DDL = """
    wal_date        STRING        COMMENT '路径里的日期目录（writer 按本行时间戳的 UTC 日期分目录）',
    source_id       STRING        COMMENT '文件名里的 source_id：哪个 data dir / 哪台机器（同一个 data dir 多次运行共用）',
    wal_seg         INT           COMMENT '文件名里的 segment 编号（同一 source、同一流内递增）',
    _source_file    STRING        COMMENT '来源文件完整路径',
    _source_mtime   TIMESTAMP     COMMENT '来源文件修改时间',
    _ingested_at    TIMESTAMP     COMMENT '写入 bronze 的时间',
    _rescued_data   STRING        COMMENT '解析失败或 schema 里没有的字段，正常应为 NULL'
"""

DDL = {
    "ws_raw": f"""
    ts_recv_ns      BIGINT        COMMENT '本机收到消息的时间，epoch 纳秒（WAL 原值）',
    ts_recv         TIMESTAMP     COMMENT 'ts_recv_ns 转成的时间戳（微秒精度）',
    recv_date       DATE          COMMENT 'ts_recv_ns 的 UTC 日期',
    asset_ids       ARRAY<STRING> COMMENT '消息涉及的 asset_id（采集端路由用）；空数组 = 未能路由',
    raw             STRING        COMMENT 'WebSocket 原始消息体，不解析',
    {SOURCE_COLS_DDL}""",

    "markets_raw": f"""
    ts_recv_ns      BIGINT        COMMENT '这一轮 discovery 的时间，epoch 纳秒；同一轮所有市场相同（WAL 原值）',
    ts_recv         TIMESTAMP     COMMENT 'ts_recv_ns 转成的时间戳（微秒精度）',
    recv_date       DATE          COMMENT 'ts_recv_ns 的 UTC 日期',
    market_id       STRING        COMMENT 'Gamma 市场 id（不是 conditionId）',
    source          STRING        COMMENT 'top = 本轮 top-N 列表里拿到的；lookup = 不在 top-N，单独查状态拿到的',
    rank            INT           COMMENT 'top-N 里的名次（1 起）；lookup 时为 NULL',
    sub             STRUCT<action: STRING, reason: STRING, missing_strikes: INT>
                                  COMMENT '订阅决定。action: added/kept/removed；reason: NULL/ended/cap/not_found/lookup_failed；missing_strikes: 连续几轮不在 top-N',
    raw             STRING        COMMENT 'Gamma 返回的市场对象，JSON 文本，不解析；查不到时为 NULL',
    {SOURCE_COLS_DDL}""",

    "events_raw": f"""
    ts_ns           BIGINT        COMMENT '事件发生的本机时间，epoch 纳秒（WAL 原值）',
    ts              TIMESTAMP     COMMENT 'ts_ns 转成的时间戳（微秒精度）',
    recv_date       DATE          COMMENT 'ts_ns 的 UTC 日期',
    conn_id         INT           COMMENT '采集端第几条 WebSocket 连接（0 起）',
    event           STRING        COMMENT 'connected/subscribed/unsubscribed/disconnected/reconnecting/watchdog_timeout/closed',
    extra           STRING        COMMENT '事件附带信息，JSON 文本，不解析。键随事件不同：assets, asset_count, operation, attempt, backoff_sec, idle_sec',
    {SOURCE_COLS_DDL}""",

    "metrics_raw": f"""
    ts_ns                BIGINT   COMMENT '快照时间，epoch 纳秒（WAL 原值）',
    ts                   TIMESTAMP COMMENT 'ts_ns 转成的时间戳（微秒精度）',
    recv_date            DATE     COMMENT 'ts_ns 的 UTC 日期',
    t_rel_s              DOUBLE   COMMENT '距采集端启动多少秒',
    msgs_total           BIGINT   COMMENT '启动以来收到的 WS 消息总数',
    msgs_per_sec_window  BIGINT   COMMENT '上一个快照以来收到的消息数（约等于每秒消息数）',
    bytes_per_sec_window BIGINT   COMMENT '上一个快照以来收到的字节数',
    subscribed_markets   INT      COMMENT '当前订阅的市场数（新版采集端）',
    subscribed_tokens    INT      COMMENT '当前订阅的 token 数（新版采集端）',
    assets_seen          INT      COMMENT '旧版采集端字段：收到过消息的 asset 数；新版为 NULL',
    lat_window_ms        STRUCT<n: BIGINT, mean: DOUBLE, median: DOUBLE, p95: DOUBLE, max: DOUBLE>
                                  COMMENT '这个窗口内 本机收到时间 - 消息 timestamp 的延迟（毫秒）；n=0 时其余为 NULL',
    errors               STRUCT<json_decode: BIGINT, missing_timestamp: BIGINT, disconnects: BIGINT, reconnects: BIGINT, watchdog: BIGINT>
                                  COMMENT '启动以来的累计错误计数',
    errors_per_conn      MAP<STRING, BIGINT> COMMENT 'conn_id -> 累计错误次数（断线 + watchdog）',
    msgs_per_conn        MAP<STRING, BIGINT> COMMENT 'conn_id -> 累计消息数',
    {SOURCE_COLS_DDL}""",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## WAL 文件的读取 schema

# COMMAND ----------

SCHEMAS = {
    "ws_raw": StructType([
        StructField("ts_recv_ns", LongType()),
        StructField("ts_recv_iso", StringType()),   # 只是方便人看，下面会丢掉
        StructField("asset_ids", ArrayType(StringType())),
        StructField("raw", StringType()),
    ]),
    "markets_raw": StructType([
        StructField("ts_recv_ns", LongType()),
        StructField("market_id", StringType()),
        StructField("source", StringType()),
        StructField("rank", IntegerType()),
        StructField("sub", StructType([
            StructField("action", StringType()),
            StructField("reason", StringType()),
            StructField("missing_strikes", IntegerType()),
        ])),
        StructField("raw", StringType()),           # 文件里是对象，存成 JSON 文本
    ]),
    "events_raw": StructType([
        StructField("ts_ns", LongType()),
        StructField("conn_id", IntegerType()),
        StructField("event", StringType()),
        StructField("extra", StringType()),         # 文件里是对象，存成 JSON 文本
    ]),
    "metrics_raw": StructType([
        StructField("ts_ns", LongType()),
        StructField("t_rel_s", DoubleType()),
        StructField("msgs_total", LongType()),
        StructField("msgs_per_sec_window", LongType()),
        StructField("bytes_per_sec_window", LongType()),
        StructField("subscribed_markets", IntegerType()),
        StructField("subscribed_tokens", IntegerType()),
        StructField("assets_seen", IntegerType()),  # 旧版采集端
        StructField("lat_window_ms", StructType([
            StructField("n", LongType()),
            StructField("mean", DoubleType()),
            StructField("median", DoubleType()),
            StructField("p95", DoubleType()),
            StructField("max", DoubleType()),
        ])),
        StructField("errors", StructType([
            StructField("json_decode", LongType()),
            StructField("missing_timestamp", LongType()),
            StructField("disconnects", LongType()),
            StructField("reconnects", LongType()),
            StructField("watchdog", LongType()),
        ])),
        StructField("errors_per_conn", MapType(StringType(), LongType())),
        StructField("msgs_per_conn", MapType(StringType(), LongType())),
    ]),
}

# 每张表的 (纳秒时间戳列, 派生的 TIMESTAMP 列)
TS_COLS = {
    "ws_raw": ("ts_recv_ns", "ts_recv"),
    "markets_raw": ("ts_recv_ns", "ts_recv"),
    "events_raw": ("ts_ns", "ts"),
    "metrics_raw": ("ts_ns", "ts"),
}

# 数据质量：核心字段缺失的条件
CORE_NOT_NULL = {
    "ws_raw": "ts_recv_ns IS NULL OR raw IS NULL",
    "markets_raw": "ts_recv_ns IS NULL OR market_id IS NULL OR sub.action IS NULL",
    "events_raw": "ts_ns IS NULL OR event IS NULL",
    "metrics_raw": "ts_ns IS NULL",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 纯函数
# MAGIC
# MAGIC 不碰 Spark session，可以直接 import 来测试。

# COMMAND ----------

def fq(table: str) -> str:
    """fully qualified 表名：catalog.schema.table，例如 polymarket.bronze.ws_raw"""
    return f"{CATALOG}.{SCHEMA}.{table}"


def source_dir(table: str) -> str:
    """landing 下这张表对应的目录，例如 /Volumes/polymarket/raw/landing/ws/"""
    return f"{LANDING_ROOT}/{TABLE_DIRS[table]}/"


def checkpoint_dir(table: str) -> str:
    # ws_raw 的 checkpoint 仍是 .../checkpoints/bronze_ws_raw/，和旧版本一致，不会重新导入
    return f"{CHECKPOINT_ROOT}/bronze_{table}/"


def path_re(table_dir: str) -> str:
    # {dir}/{date}/{source_id}-{date}-{NNNNNN}.jsonl.gz   ->   1 = date, 2 = source_id, 3 = segment number
    return rf"/{table_dir}/(\d{{4}}-\d{{2}}-\d{{2}})/([A-Za-z0-9_-]+)-\d{{4}}-\d{{2}}-\d{{2}}-(\d{{6}})\.jsonl\.gz$"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark 步骤

# COMMAND ----------

def get_spark() -> SparkSession:
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    return spark


def create_tables(spark: SparkSession) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    for t in TABLE_DIRS:
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {fq(t)} ({DDL[t]})
        CLUSTER BY (recv_date)
        COMMENT '{TABLE_COMMENT[t]}'
        """)


def read_stream(spark: SparkSession, table: str) -> DataFrame:
    return (
        spark.readStream
        # [发现] cloudFiles = Auto Loader。没有这一行就是普通的 Spark 文件流
        .format("cloudFiles")
        # [读取] 找到的文件交给 Spark 的 JSON 读取器。.jsonl 也用 json（每行一个对象）
        .option("cloudFiles.format", "json")
        # [发现] 只认 .jsonl.gz。目录下本来也只有 uploader rename 进来的完整文件，这里再保险一层
        .option("pathGlobFilter", "*.jsonl.gz")
        # [发现] 第一次启动时，目录里已有的文件也要导入（默认 true，这里写明）
        .option("cloudFiles.includeExistingFiles", "true")
        # [发现] 用列目录的方式找新文件（默认）。文件很多时可改成 true，用云存储的事件通知
        .option("cloudFiles.useNotifications", "false")
        # [发现] 同一路径的文件被覆盖后不重新读（默认 false）。uploader 重传时文件名和内容都相同，不需要重读
        .option("cloudFiles.allowOverwrites", "false")
        # [发现] 每批最多处理多少数据，避免第一次补历史时一批太大
        .option("cloudFiles.maxBytesPerTrigger", "10g")
        # [读取] 解析失败或不在 schema 里的字段放进 _rescued_data，不直接丢
        .option("rescuedDataColumn", "_rescued_data")
        # [读取] 显式 schema，不推断
        .schema(SCHEMAS[table])
        # [发现] 监控这个目录（递归找子目录 {date}/）
        .load(source_dir(table))
    )


def add_columns(df: DataFrame, table: str) -> DataFrame:
    ns_col, ts_col = TS_COLS[table]
    pattern = path_re(TABLE_DIRS[table])
    wal_cols = [f.name for f in SCHEMAS[table].fields if f.name != "ts_recv_iso"]
    return (
        df
        .withColumn(ts_col, F.expr(f"timestamp_micros({ns_col} DIV 1000)"))
        # 直接从纳秒算 UTC 日期，和 writer 分日期目录的规则一致；
        # 不用 to_date(ts)，那个结果取决于 Spark session 时区
        .withColumn("recv_date", F.expr(f"date_from_unix_date(CAST({ns_col} DIV 86400000000000 AS INT))"))
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_mtime", F.col("_metadata.file_modification_time"))
        .withColumn("wal_date", F.regexp_extract("_source_file", pattern, 1))
        .withColumn("source_id", F.regexp_extract("_source_file", pattern, 2))
        .withColumn("wal_seg", F.regexp_extract("_source_file", pattern, 3).cast("int"))
        .withColumn("_ingested_at", F.current_timestamp())
        # 列顺序和 DDL 一致：WAL 原值里的时间戳，派生时间，其余 WAL 原值，来源信息
        .select(
            ns_col, ts_col, "recv_date", *[c for c in wal_cols if c != ns_col],
            "wal_date", "source_id", "wal_seg",
            "_source_file", "_source_mtime", "_ingested_at", "_rescued_data",
        )
    )


def start_streams(spark: SparkSession) -> dict[str, StreamingQuery]:
    """每张表启动一个流，各自一个 checkpoint；四个一起跑。"""
    queries: dict[str, StreamingQuery] = {}
    for t in TABLE_DIRS:
        queries[t] = (
            add_columns(read_stream(spark, t), t)
            .writeStream
            .queryName(f"bronze_{t}")
            .option("checkpointLocation", checkpoint_dir(t))   # [记账]
            .trigger(availableNow=True)
            .toTable(fq(t))
        )
    return queries


def wait_all(queries: dict[str, StreamingQuery]) -> None:
    """
    等每个流都跑完，打印每张表导入的行数。

    一个流失败不会中断对其他流的等待；全部结束后，有失败就统一抛出。
    """
    failures: dict[str, Exception] = {}
    for t, q in queries.items():
        try:
            q.awaitTermination()
        except Exception as e:
            failures[t] = e
            print(f"{t:12s} FAILED: {e}")
            continue
        # 没处理任何 batch 的进度事件（例如 availableNow 下没有新文件）里
        # numInputRows 可能是 None，按 0 算。
        rows = sum(p["numInputRows"] or 0 for p in q.recentProgress)
        print(f"{t:12s} rows={rows}")

    if failures:
        raise RuntimeError(f"bronze streams failed: {sorted(failures)}") from next(iter(failures.values()))


def show_checks(spark: SparkSession) -> None:
    # 最近 50 个被处理的文件（来自 checkpoint 里 Auto Loader 的文件清单）
    spark.sql(" UNION ALL ".join(
        f"""SELECT '{t}' AS tbl, path, size, create_time, commit_time
            FROM cloud_files_state('{checkpoint_dir(t)}')"""
        for t in TABLE_DIRS
    ) + " ORDER BY commit_time DESC LIMIT 50").show(50, truncate=False)

    # 每张表、每天、每个 source 导入了多少行、多少个文件。
    # ws_raw：如果这个 source 当天只跑了一次，n_rows 应该等于 run_collector 输出的 stats["wal"]["messages"]。
    # n_files 应该等于 max_seg - min_seg + 1（中间没有缺编号）
    spark.sql(" UNION ALL ".join(
        f"""SELECT '{t}' AS tbl, wal_date, source_id,
                   COUNT(*)                     AS n_rows,
                   COUNT(DISTINCT _source_file) AS n_files,
                   MIN(wal_seg)                 AS min_seg,
                   MAX(wal_seg)                 AS max_seg,
                   MIN({TS_COLS[t][1]})         AS first_row,
                   MAX({TS_COLS[t][1]})         AS last_row
            FROM {fq(t)}
            GROUP BY wal_date, source_id"""
        for t in TABLE_DIRS
    ) + " ORDER BY wal_date DESC, tbl, first_row DESC").show(200, truncate=False)

    # 数据质量：这几项正常都应该是 0
    spark.sql(" UNION ALL ".join(
        f"""SELECT '{t}' AS tbl,
              COUNT_IF(_rescued_data IS NOT NULL)             AS n_rescued,       -- 解析异常或多出来的字段
              COUNT_IF({CORE_NOT_NULL[t]})                    AS n_null_core,     -- 核心字段缺失
              COUNT_IF(wal_date = '')                         AS n_bad_path,      -- 路径不符合 {{dir}}/{{date}}/{{source_id}}-{{date}}-NNNNNN.jsonl.gz
              COUNT_IF(wal_date <> CAST(recv_date AS STRING)) AS n_date_mismatch  -- 目录日期和行日期不一致。writer 按每行自己的时间戳（UTC）分目录，必须为 0
            FROM {fq(t)}"""
        for t in TABLE_DIRS
    )).show(truncate=False)


def main() -> None:
    spark = get_spark()
    create_tables(spark)
    wait_all(start_streams(spark))
    show_checks(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 运行
# MAGIC
# MAGIC 建表 → 四个流一起跑到处理完 → 打印检查结果。任何一个流失败，这个 cell 都会失败（Job 也会失败）。
# MAGIC
# MAGIC 在 notebook 里 `__name__ == "__main__"`，所以 Run all 会执行 `main()`；本地被 import 时不会执行。

# COMMAND ----------

if __name__ == "__main__":
    main()
