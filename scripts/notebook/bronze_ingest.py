# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion：WAL → `bronze.ws_raw`
# MAGIC
# MAGIC 把采集端（`run_collector`）上传到 landing volume 的 WS 消息文件增量导入 Delta 表：
# MAGIC
# MAGIC ```
# MAGIC {landing}/ws/{date}/{source_id}-{date}-{NNNNNN}.jsonl.gz
# MAGIC ```
# MAGIC
# MAGIC - `source_id`：哪一次运行 / 哪台机器（`{host_id}-{8位随机}`），同一个 source 内编号 `NNNNNN` 递增
# MAGIC - 一行 WAL = 一行 bronze，内容原样保留（`raw` 不解析），只补充时间和来源信息
# MAGIC
# MAGIC ## 打个比方：公司收发室
# MAGIC
# MAGIC `ws/` 目录就像一个**快递架**，uploader 不停往上放新包裹（`.jsonl.gz` 文件）。
# MAGIC
# MAGIC | 角色 | 是谁 | 做什么 |
# MAGIC |---|---|---|
# MAGIC | **收发员** | Auto Loader | ① **看架子**：哪些包裹是新来的（发现文件）<br>② **登记本**：拿走一个就记一笔，下次不再拿（记账） |
# MAGIC | **拆包裹的人** | Spark | 把包裹拆开（解压 `.gz`），把里面的东西一行一行摆成表格的样子（按 schema 解析） |
# MAGIC | **仓库** | Delta 表 `bronze.ws_raw` | 摆好的东西最后放进这里，长期保存 |
# MAGIC
# MAGIC 收发员自己从来不拆包裹。
# MAGIC
# MAGIC ## 对应到代码
# MAGIC
# MAGIC ```python
# MAGIC spark.readStream
# MAGIC     .format("cloudFiles")                   # 叫收发员来（这一行就是"用 Auto Loader"）
# MAGIC     .option("pathGlobFilter", "*.jsonl.gz") # 收发员只拿 .jsonl.gz 的包裹
# MAGIC     .load(SOURCE_PATH)                      # 收发员盯着这个架子
# MAGIC     #   ↑ 第 1 件事：发现新文件
# MAGIC
# MAGIC     .option("cloudFiles.format", "json")    # 告诉拆包的人：里面是 JSON
# MAGIC     .schema(WAL_SCHEMA)                     # 告诉拆包的人：每行有哪几个字段、什么类型
# MAGIC     #   ↑ 交给 Spark 怎么拆（解压不用写，看到 .gz 后缀自己会解压）
# MAGIC
# MAGIC .writeStream
# MAGIC     .option("checkpointLocation", CHECKPOINT_PATH)  # 登记本放在哪
# MAGIC     #   ↑ 第 2 件事：记账
# MAGIC     .toTable(TABLE)                         # 摆好的东西放进仓库（Delta 表）
# MAGIC ```
# MAGIC
# MAGIC 下面第 3 节代码里其余的 option 都是可选的细节调整。
# MAGIC
# MAGIC ## 按 schema 解析成什么？
# MAGIC
# MAGIC **解析出来的是 DataFrame，不是 table。**
# MAGIC
# MAGIC - **DataFrame** = 拆包的人摆在桌上的东西：在内存里、有行有列，程序停了就没了
# MAGIC - **table** = 仓库：存在磁盘上的 Delta 表，只有最后 `.toTable(TABLE)` 那一步才写进去
# MAGIC
# MAGIC 拿一行 WAL 举例：
# MAGIC
# MAGIC **① 包裹里的原样（`.jsonl.gz` 解压后的一行文本）**
# MAGIC
# MAGIC ```json
# MAGIC {"ts_recv_ns": 1762473600123456789, "ts_recv_iso": "2026-11-07T00:00:00.123456Z", "asset_ids": ["0123…"], "raw": "{\"event_type\":\"book\", …}"}
# MAGIC ```
# MAGIC
# MAGIC **② Spark 按 `WAL_SCHEMA` 解析后：DataFrame 里的一行（还在内存里）**
# MAGIC
# MAGIC | ts_recv_ns (BIGINT) | ts_recv_iso (STRING) | asset_ids (ARRAY&lt;STRING&gt;) | raw (STRING) |
# MAGIC |---|---|---|---|
# MAGIC | 1762473600123456789 | 2026-11-07T00:00:00.123456Z | ["0123…"] | `{"event_type":"book", …}` |
# MAGIC
# MAGIC schema 的作用就是告诉 Spark：JSON 里哪个键变成哪一列、是什么类型。
# MAGIC `raw` 声明成 STRING，所以它只是一整段文本，不会被继续拆开。
# MAGIC
# MAGIC **③ 第 4 节补列后，第 5 节写进 Delta 表 `bronze.ws_raw`（存到磁盘）**
# MAGIC
# MAGIC 丢掉 `ts_recv_iso`，补上 `ts_recv`、`recv_date`、`wal_date`、`source_id`、`wal_seg`、`_source_file` 等列，然后写入。
# MAGIC 到这一步才有 table。
# MAGIC
# MAGIC ```
# MAGIC  .jsonl.gz 文件 ──Auto Loader 发现──▶ Spark 解压 + 解析 ──▶ DataFrame ──补列──▶ toTable ──▶ Delta 表
# MAGIC     (包裹)          (收发员)          (拆包的人)          (桌上)                    (仓库)
# MAGIC ```
# MAGIC
# MAGIC ## 前提
# MAGIC
# MAGIC 架子上只会出现完整的文件：uploader 先把文件复制到 `{landing}/_staging/`，
# MAGIC 复制完、检查过大小后再一步 rename 到 `ws/...`，所以收发员不会拿到复制到一半的文件。
# MAGIC `_staging/` 和 `_runs/` 都不在 `ws/` 下面，不会被导入。
# MAGIC
# MAGIC ⚠️ 如果之前用旧版本（`wal/{date}/gen{N}/...`）运行过这个 notebook，表结构已经变了：
# MAGIC 先 `DROP TABLE` 旧表，并删除旧的 checkpoint 目录，再运行。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. 参数

# COMMAND ----------

dbutils.widgets.text("catalog", "polymarket")
dbutils.widgets.text("schema", "bronze")
dbutils.widgets.text("source_path", "/Volumes/polymarket/raw/landing_dev/ws/")
dbutils.widgets.text("checkpoint_path", "/Volumes/polymarket/raw/checkpoints/bronze_ws_raw/")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
SOURCE_PATH = dbutils.widgets.get("source_path")
CHECKPOINT_PATH = dbutils.widgets.get("checkpoint_path")
TABLE = f"{CATALOG}.{SCHEMA}.ws_raw"

print(f"source     = {SOURCE_PATH}")
print(f"checkpoint = {CHECKPOINT_PATH}")
print(f"table      = {TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 目标表
# MAGIC
# MAGIC 先用 DDL 建表，表结构和聚簇方式由我们控制，不交给 streaming 自动建。
# MAGIC 回测基本都按日期、按 asset 查，所以按 `recv_date` 做 liquid clustering。

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    ts_recv_ns      BIGINT        COMMENT '本机收到消息的时间，epoch 纳秒（WAL 原值）',
    ts_recv         TIMESTAMP     COMMENT 'ts_recv_ns 转成的时间戳（微秒精度）',
    recv_date       DATE          COMMENT 'ts_recv 的 UTC 日期',
    asset_ids       ARRAY<STRING> COMMENT '消息涉及的 asset_id；空数组 = 未能路由',
    raw             STRING        COMMENT 'WebSocket 原始消息体，不解析',
    -- 来源信息
    wal_date        STRING        COMMENT '路径里的日期目录',
    source_id       STRING        COMMENT '文件名里的 source_id：哪一次运行 / 哪台机器',
    wal_seg         INT           COMMENT '文件名里的 segment 编号（同一 source 内递增）',
    _source_file    STRING        COMMENT '来源文件完整路径',
    _source_mtime   TIMESTAMP     COMMENT '来源文件修改时间',
    _ingested_at    TIMESTAMP     COMMENT '写入 bronze 的时间',
    _rescued_data   STRING        COMMENT '解析失败或多出来的字段，正常应为 NULL'
)
CLUSTER BY (recv_date)
COMMENT 'Polymarket WebSocket 原始消息，一行 WAL = 一行'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. WAL 行的 schema
# MAGIC
# MAGIC 和 `Walwriter.write()`（`src/wal/writer.py`）写出的 wrapper 一一对应：
# MAGIC
# MAGIC ```json
# MAGIC {"ts_recv_ns": 1762473600123456789, "ts_recv_iso": "2026-11-07T00:00:00.123456Z",
# MAGIC  "asset_ids": ["0123…"], "raw": "{\"event_type\":\"book\", …}"}
# MAGIC ```
# MAGIC
# MAGIC 显式给出 schema，Spark 就不用先扫一遍数据去推断。
# MAGIC `raw` 声明成 STRING，所以它会保持原样，不会被拆成嵌套结构。

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

WAL_SCHEMA = StructType([
    StructField("ts_recv_ns", LongType()),
    StructField("ts_recv_iso", StringType()),   # 只是方便人看，下面会丢掉
    StructField("asset_ids", ArrayType(StringType())),
    StructField("raw", StringType()),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Auto Loader：发现 + 记账 → 交给 Spark 读
# MAGIC
# MAGIC 下面每个 option 都标了它属于哪一部分：
# MAGIC
# MAGIC - **[发现]** Auto Loader 怎么找新文件
# MAGIC - **[读取]** 交给 Spark JSON 读取器的参数
# MAGIC - **[记账]** 在 `writeStream` 的 `checkpointLocation` 里

# COMMAND ----------

raw_stream = (
    spark.readStream
    # [发现] cloudFiles = Auto Loader。没有这一行就是普通的 Spark 文件流
    .format("cloudFiles")
    # [读取] 找到的文件交给 Spark 的 JSON 读取器。.jsonl 也用 json（每行一个对象）
    .option("cloudFiles.format", "json")
    # [发现] 只认 .jsonl.gz。ws/ 下本来也只有 uploader rename 进来的完整文件，这里再保险一层
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
    .schema(WAL_SCHEMA)
    # [发现] 监控这个目录（递归找子目录 {date}/）
    .load(SOURCE_PATH)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 补列
# MAGIC
# MAGIC 只做不改变原始内容的补充：时间换算、从路径解析来源、加入库时间。
# MAGIC `_metadata` 是 Spark 文件读取器自带的隐藏列，可以拿到来源文件路径和修改时间。

# COMMAND ----------

# ws/{date}/{source_id}-{date}-{NNNNNN}.jsonl.gz   ->   1 = date, 2 = source_id, 3 = segment number
PATH_RE = r"/ws/(\d{4}-\d{2}-\d{2})/([A-Za-z0-9_-]+)-\d{4}-\d{2}-\d{2}-(\d{6})\.jsonl\.gz$"

bronze_df = (
    raw_stream
    .withColumn("ts_recv", F.expr("timestamp_micros(ts_recv_ns DIV 1000)"))
    .withColumn("recv_date", F.to_date(F.col("ts_recv")))
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_source_mtime", F.col("_metadata.file_modification_time"))
    .withColumn("wal_date", F.regexp_extract("_source_file", PATH_RE, 1))
    .withColumn("source_id", F.regexp_extract("_source_file", PATH_RE, 2))
    .withColumn("wal_seg", F.regexp_extract("_source_file", PATH_RE, 3).cast("int"))
    .withColumn("_ingested_at", F.current_timestamp())
    .select(
        "ts_recv_ns", "ts_recv", "recv_date", "asset_ids", "raw",
        "wal_date", "source_id", "wal_seg",
        "_source_file", "_source_mtime", "_ingested_at", "_rescued_data",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 写入 + 记账
# MAGIC
# MAGIC `checkpointLocation` 就是 Auto Loader 的账本：
# MAGIC
# MAGIC - 一批数据**成功写进 Delta 之后**，这批文件才在 checkpoint 里标记为已处理
# MAGIC - 中途失败：checkpoint 没更新，下次重跑会重新处理这批文件，Delta 的事务保证不会写入半批数据
# MAGIC - 所以每个文件只会进表一次，不重复也不丢
# MAGIC
# MAGIC ⚠️ 不要删 checkpoint 目录。删了 Auto Loader 就忘了读过哪些文件，下次会把所有文件重新导入一遍。
# MAGIC
# MAGIC `trigger(availableNow=True)`：把当前所有新文件处理完就停。适合配成定时 Job（比如每 15 分钟一次），
# MAGIC 不用让集群一直开着。想实时导入的话，改成 `trigger(processingTime="1 minute")`。

# COMMAND ----------

query = (
    bronze_df.writeStream
    .option("checkpointLocation", CHECKPOINT_PATH)   # [记账]
    .trigger(availableNow=True)
    .toTable(TABLE)
)

query.awaitTermination()
print(query.lastProgress)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 查账本：Auto Loader 记下了哪些文件
# MAGIC
# MAGIC `cloud_files_state` 可以直接读 checkpoint，列出 Auto Loader 处理过的每个文件。
# MAGIC 这就是第 2 件事"记账"记下来的内容。

# COMMAND ----------

display(spark.sql(f"""
SELECT path, size, create_time, commit_time
FROM cloud_files_state('{CHECKPOINT_PATH}')
ORDER BY commit_time DESC
LIMIT 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. 检查结果

# COMMAND ----------

# 每天、每个 source（每次运行 / 每台机器）导入了多少行、多少个 segment。
# n_rows 应该等于 run_collector 输出的 stats["wal"]["messages"]；
# n_files 应该等于 max_seg - min_seg + 1（中间没有缺编号）
display(spark.sql(f"""
SELECT wal_date, source_id,
       COUNT(*)                     AS n_rows,
       COUNT(DISTINCT _source_file) AS n_files,
       MIN(wal_seg)                 AS min_seg,
       MAX(wal_seg)                 AS max_seg,
       MIN(ts_recv)                 AS first_msg,
       MAX(ts_recv)                 AS last_msg
FROM {TABLE}
GROUP BY wal_date, source_id
ORDER BY wal_date DESC, first_msg DESC
"""))

# COMMAND ----------

# 数据质量：这几项正常都应该是 0
display(spark.sql(f"""
SELECT
  COUNT_IF(_rescued_data IS NOT NULL)               AS n_rescued,      -- 解析异常
  COUNT_IF(ts_recv_ns IS NULL OR raw IS NULL)       AS n_null_core,    -- 核心字段缺失
  COUNT_IF(wal_date = '')                           AS n_bad_path,     -- 路径不符合 ws/{{date}}/{{source_id}}-{{date}}-NNNNNN.jsonl.gz
  COUNT_IF(wal_date <> CAST(recv_date AS STRING))   AS n_date_mismatch -- 目录日期和消息日期不一致（跨午夜的少量延迟消息属正常）
FROM {TABLE}
"""))
