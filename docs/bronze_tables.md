# Bronze 层：表说明和字段

由 [scripts/notebook/bronze_ingest.py](../scripts/notebook/bronze_ingest.py) 生成。WS 原始消息里每个字段的含义（`raw` 里面有什么）见 [data_fields.md](data_fields.md)。

## 1. 作用和原则

Bronze 存的是采集端写出的原始数据：**原样保存，只追加，以后都能从这里重建**。

- **不解析**：Polymarket / Gamma 给的原始内容（`raw`、`extra`）保持文本原样，解析留给 silver
- **不去重、不删**：每个文件只导入一次（由 checkpoint 保证），文件里有什么就存什么
- **只补列**：时间换算、从文件路径解析来源、入库时间，不改原始字段

silver 写错了，随时可以从 bronze 重跑。landing 上的 `.jsonl.gz` 以后可以按保留期清理，bronze 才是长期保存的那一份。

## 2. 四张表

| landing 目录 | bronze 表 | 一行是什么 | 用途 |
|---|---|---|---|
| `ws/` | `ws_raw` | 一条 WebSocket 消息 | 订单簿、成交，回测的主数据 |
| `markets/` | `markets_raw` | 一轮 discovery 里的一个市场 | 市场元数据（token、tick size、费率、奖励），订阅变化 |
| `events/` | `events_raw` | 一个连接事件 | 找数据缺口：哪段时间哪些 token 没有数据 |
| `metrics/` | `metrics_raw` | 每秒一次的采集端快照 | 只用于监控，可以关掉（`include_metrics=false`） |

表名都是 `{catalog}.{schema}.{表}`，默认 `polymarket.bronze.*`。

### 来源文件

```
{landing}/{ws|markets|events|metrics}/{date}/{source_id}-{date}-{NNNNNN}.jsonl.gz
```

- `{date}`：每一行自己时间戳（`ts_recv_ns` / `ts_ns`）的 **UTC 日期**，writer 按它分目录
- `source_id`：`{host_id}-{8位随机}`，代表一个采集目录（data dir）。保存在 `{data_dir}/.source_id`，同一个 data dir 多次运行共用；Databricks Job 每次用新的临时目录，所以每次运行会是一个新的 source_id
- `NNNNNN`：segment 编号，同一个 source、同一个流内递增

uploader 先把文件复制到 `{landing}/_staging/`，检查大小后再 rename 到流目录，所以 bronze 不会读到不完整的文件。

## 3. 共用的列

每张表的列分三组：

1. **WAL 原值**：文件里每行的字段，原名原值
2. **派生时间**：从纳秒时间戳换算，不改原值
3. **来源信息**：从文件路径和 Auto Loader 的 `_metadata` 拿到，四张表完全相同

### 派生时间

| 列 | 类型 | 说明 |
|---|---|---|
| `ts_recv` 或 `ts` | TIMESTAMP | 纳秒时间戳换算成的时间，微秒精度。ws / markets 叫 `ts_recv`，events / metrics 叫 `ts` |
| `recv_date` | DATE | 纳秒时间戳的 UTC 日期。直接用整数除法算（`ns DIV 86400000000000`），**和 Spark session 时区无关**，和 writer 分目录的规则一致。四张表都按它做 liquid clustering |

注意：`ts_recv` / `ts` 显示出来的时间会随 session 时区变化（同一个值在 UTC 下显示 `19:15`，在 Asia/Shanghai 下显示次日 `03:15`），`recv_date` 不会。按日期过滤一律用 `recv_date`。

### 来源信息

| 列 | 类型 | 说明 | 例子 |
|---|---|---|---|
| `wal_date` | STRING | 路径里的日期目录 | `2026-11-10` |
| `source_id` | STRING | 文件名里的 source_id | `MacBook-Pro-86-local-8dca32a4` |
| `wal_seg` | INT | 文件名里的 segment 编号 | `3` |
| `_source_file` | STRING | 来源文件完整路径 | `/Volumes/polymarket/raw/landing/ws/2026-11-10/MacBook-Pro-86-local-8dca32a4-2026-11-10-000003.jsonl.gz` |
| `_source_mtime` | TIMESTAMP | 来源文件修改时间 | |
| `_ingested_at` | TIMESTAMP | 写入 bronze 的时间 | |
| `_rescued_data` | STRING | 解析失败或 schema 里没有的字段（JSON 文本），正常为 NULL | |

## 4. ws_raw

一条 WebSocket 消息一行。

| 列 | 类型 | 说明 | 例子 |
|---|---|---|---|
| `ts_recv_ns` | BIGINT | 本机收到消息的时间，epoch 纳秒 | `1794338129903452000` |
| `ts_recv` | TIMESTAMP | 换算后的时间 | `2026-11-10 19:15:29.903452` |
| `recv_date` | DATE | UTC 日期 | `2026-11-10` |
| `asset_ids` | ARRAY&lt;STRING&gt; | 消息涉及的 token（采集端路由用）；空数组 = 未能路由 | `["34732988…", "63915287…"]` |
| `raw` | STRING | WebSocket 原始消息，不解析 | 见下 |
| 来源信息 | | 见第 3 节 | |

`raw` 的例子（一条 `price_change`，YES / NO 两边镜像）：

```json
{"market":"0x1c4c6a69…","price_changes":[
  {"asset_id":"34732988…","price":"0.61","size":"11372.39","side":"BUY","hash":"8f79d263…","best_bid":"0.68","best_ask":"0.69"},
  {"asset_id":"63915287…","price":"0.39","size":"11372.39","side":"SELL","hash":"9a077d6c…","best_bid":"0.31","best_ask":"0.32"}],
 "timestamp":"…","event_type":"price_change"}
```

`raw` 可能是 `book`、`price_change`、`last_trade_price`、`tick_size_change` 等消息，第一次订阅时还可能是一个 book 数组。每种消息的字段见 [data_fields.md](data_fields.md)。

WAL 里的 `ts_recv_iso` 只是方便人看，和 `ts_recv_ns` 重复，不进表。

## 5. markets_raw

每轮 market discovery 里的每个市场一行。同一轮的所有市场 `ts_recv_ns` 相同。

| 列 | 类型 | 说明 | 例子 |
|---|---|---|---|
| `ts_recv_ns` | BIGINT | 这一轮 discovery 的时间，epoch 纳秒 | `1794338129502492000` |
| `ts_recv` | TIMESTAMP | 换算后的时间 | `2026-11-10 19:15:29.502492` |
| `recv_date` | DATE | UTC 日期 | `2026-11-10` |
| `market_id` | STRING | Gamma 市场 id（**不是** conditionId） | `3215007` |
| `source` | STRING | `top` = 来自本轮 top-N 列表；`lookup` = 不在 top-N，单独查的状态 | `top` |
| `rank` | INT | 在 top-N 里的名次（1 起）；`lookup` 时为 NULL | `1` |
| `sub` | STRUCT | 本轮的订阅决定，见下 | `{added, NULL, 0}` |
| `raw` | STRING | Gamma 返回的市场对象，JSON 文本，不解析；查不到时为 NULL | 见下 |
| 来源信息 | | 见第 3 节 | |

### `sub` 的字段

| 字段 | 取值 | 说明 |
|---|---|---|
| `action` | `added` / `kept` / `removed` | 本轮新订阅 / 继续订阅 / 退订 |
| `reason` | NULL / `ended` / `cap` / `not_found` / `lookup_failed` | 为什么这样决定。`ended`：市场结束了；`cap`：超出订阅上限，被挤掉；`not_found`：Gamma 查不到；`lookup_failed`：查询失败 |
| `missing_strikes` | 整数 | 连续几轮不在 top-N |

### `raw`

`raw` 在 WAL 文件里是一个 JSON 对象。schema 里把它声明成 STRING，Spark 就把整个对象存成一段 JSON 文本（内容不变，空格被去掉）。一个市场大约 8 KB。

常用的键（完整说明见 [data_fields.md](data_fields.md) 的 markets 部分）：

| 键 | 例子 | 用途 |
|---|---|---|
| `question` | `Will there be no change in Fed interest rates after the December 2026 meeting?` | 市场问题 |
| `conditionId` | `0x723822eb…` | 和 ws 消息里的 `market` 对应 |
| `clobTokenIds` | `["…", "…"]`（字符串形式的数组） | YES / NO 两个 token，和 ws 的 `asset_id` 对应 |
| `endDate` | `2026-12-09T23:59:00Z` | 结束时间 |
| `liquidity` | `816948.09147` | 流动性 |
| tick size、费率、奖励参数等 | | 回测的成交规则和奖励估算 |

在 Databricks 里取字段：

```sql
SELECT market_id,
       raw:question::string    AS question,
       raw:conditionId::string AS condition_id
FROM polymarket.bronze.markets_raw
```

## 6. events_raw

一个 WebSocket 连接事件一行。

| 列 | 类型 | 说明 | 例子 |
|---|---|---|---|
| `ts_ns` | BIGINT | 事件发生的本机时间，epoch 纳秒 | `1794338129697565000` |
| `ts` | TIMESTAMP | 换算后的时间 | `2026-11-10 19:15:29.697565` |
| `recv_date` | DATE | UTC 日期 | `2026-11-10` |
| `conn_id` | INT | 采集端的第几条 WebSocket 连接（0 起） | `0` |
| `event` | STRING | 事件类型，见下 | `connected` |
| `extra` | STRING | 事件附带信息，JSON 文本，不解析 | `{"assets":["1127…",…],"asset_count":200}` |
| 来源信息 | | 见第 3 节 | |

### `event` 和 `extra`

| event | 什么时候 | extra 的键 |
|---|---|---|
| `connected` | 连接建立 | `assets`（这条连接负责的 token）、`asset_count` |
| `subscribed` | 发出订阅消息 | `operation`（`initial` = 连接后的首次订阅；`subscribe` = 之后追加）、`assets`（这次订阅的 token）、`asset_count`（订阅后的总数） |
| `unsubscribed` | 发出退订消息 | `assets`（这次退订的 token）、`asset_count`（退订后的总数） |
| `disconnected` | 断线，准备重连 | `{}` |
| `reconnecting` | 开始重连 | `attempt`（第几次）、`backoff_sec`（等待秒数） |
| `watchdog_timeout` | 太久没收到数据，强制重连 | `idle_sec` |
| `closed` | 连接最终关闭，不再重试 | `{}` |

silver 用它算每个 token 的订阅区间和数据缺口。比如一条连接 `disconnected` 到下一次 `connected` 之间，它负责的所有 token 都没有数据，重连后收到的第一条 `book` 是新的快照。

## 7. metrics_raw

采集端每秒一个监控快照，只用于监控。metrics 是采集端自己的格式，字段固定，所以直接拆成列方便查询。

| 列 | 类型 | 说明 | 例子 |
|---|---|---|---|
| `ts_ns` | BIGINT | 快照时间，epoch 纳秒 | `1794338150246816000` |
| `ts` | TIMESTAMP | 换算后的时间 | |
| `recv_date` | DATE | UTC 日期 | |
| `t_rel_s` | DOUBLE | 距采集端启动多少秒 | `21.11` |
| `msgs_total` | BIGINT | 启动以来收到的 WS 消息总数 | `17128` |
| `msgs_per_sec_window` | BIGINT | 上一个快照以来收到的消息数（约等于每秒消息数） | `841` |
| `bytes_per_sec_window` | BIGINT | 上一个快照以来收到的字节数 | `525600` |
| `subscribed_markets` | INT | 当前订阅的市场数（新版采集端） | |
| `subscribed_tokens` | INT | 当前订阅的 token 数（新版采集端） | |
| `assets_seen` | INT | 旧版采集端字段：收到过消息的 asset 数；新版为 NULL | `400` |
| `lat_window_ms` | STRUCT | 这个窗口内「本机收到时间 − 消息 timestamp」的延迟，毫秒：`n, mean, median, p95, max`。`n=0` 时其余为 NULL | `{841, 15.2, 14.2, 23.9, 44.6}` |
| `errors` | STRUCT | 启动以来的累计错误数：`json_decode, missing_timestamp, disconnects, reconnects, watchdog` | `{0, 0, 0, 0, 0}` |
| `errors_per_conn` | MAP&lt;STRING, BIGINT&gt; | conn_id → 累计错误次数（断线 + watchdog） | `{}` |
| `msgs_per_conn` | MAP&lt;STRING, BIGINT&gt; | conn_id → 累计消息数 | `{0 -> 6334, 1 -> 10794}` |
| 来源信息 | | 见第 3 节 | |

## 8. 为什么存成 TABLE

数据量：ws 的 gzip 压缩比约 8 倍，每天 80 GB 解压数据 ≈ 10 GB `.jsonl.gz`。写成 Delta 估计每天 10 GB 左右，一年 3–4 TB（估算）。

| 存法 | 适合 bronze 吗 | 原因 |
|---|---|---|
| `VIEW` / `TEMP VIEW` | 不适合 | 每次查询都要把所有 `.gz` 重新解压、解析；gzip 文件不能拆开并行读；landing 文件一清理数据就没了 |
| `MATERIALIZED VIEW` | 不适合 | 源头是文件不是 Delta 表，刷新基本只能全量重算，越来越慢、越来越贵。适合 gold 的聚合 |
| `STREAMING TABLE` | 可以 | 只追加新文件。但 full refresh 会清空表再从源文件重导，landing 清理后会丢历史（可用 `pipelines.reset.allowed = false` 防住）；只能由它的 pipeline 写 |
| **`TABLE` + Auto Loader Job** | **采用** | 每次运行只追加新文件，机制和 STREAMING TABLE 一样，存储也是 Delta；只有手动 DROP 才会丢；表结构、注释、聚簇由自己的 DDL 控制；任何 Job 都能写 |

建议的组合：bronze 用 TABLE（现在这样）；silver 用 STREAMING TABLE 或 TABLE + 流式读 bronze，只处理新增的行；gold 用 MATERIALIZED VIEW。

## 9. 怎么跑

- 配成一个定时 Job（比如每 15 分钟），**Max concurrent runs = 1**
- 每次运行：四个流各自启动一个 Auto Loader stream（`trigger(availableNow=True)`），把新文件处理完就停
- 每个流有自己的 checkpoint：`{checkpoint_root}/bronze_{表名}/`。一批数据写进 Delta 成功后，这批文件才在 checkpoint 里记为已处理，所以每个文件只进表一次
- **不要删 checkpoint 目录**，删了对应的表会把所有文件重新导入一遍（重复）

notebook 参数：

| 参数 | 默认值 | 说明 |
|---|---|---|
| `catalog` | `polymarket` | |
| `schema` | `bronze` | |
| `landing_root` | `/Volumes/polymarket/raw/landing/` | 四个流目录的上一级 |
| `checkpoint_root` | `/Volumes/polymarket/raw/checkpoints/` | |
| `include_metrics` | `true` | 是否导入 metrics |

## 10. 检查

notebook 最后会对每张表跑两组检查。

**导入量**：每张表、每天、每个 source 的行数、文件数、最小 / 最大编号。`n_files` 应该等于 `max_seg - min_seg + 1`，否则中间缺了文件。ws_raw 的 `n_rows` 应该等于采集端输出的 `stats["wal"]["messages"]`（这个 source 当天只跑了一次时）。

**数据质量**，正常都应该是 0：

| 检查 | 含义 |
|---|---|
| `n_rescued` | 有解析异常或多出来的字段 |
| `n_null_core` | 核心字段缺失。ws：`ts_recv_ns` / `raw`；markets：`ts_recv_ns` / `market_id` / `sub.action`；events：`ts_ns` / `event`；metrics：`ts_ns` |
| `n_bad_path` | 文件路径不符合 `{dir}/{date}/{source_id}-{date}-NNNNNN.jsonl.gz` |
| `n_date_mismatch` | 目录日期和行日期不一致。writer 按每行自己的时间戳（UTC）分目录，必须为 0 |

在 fake_volume（2026-11-10，21 秒数据）上的结果：ws_raw 17170 行、markets_raw 200 行、events_raw 8 行、metrics_raw 21 行，四项检查全部为 0（UTC 和 Asia/Shanghai 两个时区下都一样）。
