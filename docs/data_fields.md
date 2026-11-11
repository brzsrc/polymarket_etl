# 采集数据字段说明

采集端（`src/collector.py`）会上传四个 stream 到 landing：

```
{landing}/ws/{date}/{source_id}-{date}-{NNNNNN}.jsonl.gz        WebSocket 行情消息
{landing}/markets/{date}/{source_id}-{date}-{NNNNNN}.jsonl.gz   每轮 discovery 的市场状态
{landing}/events/{date}/{source_id}-{date}-{NNNNNN}.jsonl.gz    WS 连接生命周期事件
{landing}/metrics/{date}/{source_id}-{date}-{NNNNNN}.jsonl.gz   每秒运行指标
```

每个文件是 gzip 压缩的 JSON Lines，一行一条记录。

四个 stream 的共同规律：外层字段是采集程序自己加的（时间、路由、状态），`raw` 里是 Polymarket 发来的原文，完全不改。

文中的例子都取自 `data/fake_volume`（2026-11-10 本地运行约 21 秒的数据）。太长的值用 `…` 截断。每条说明后面标了依据：

- **[文档]**：Polymarket 官方文档有明确说明
- **[数据]**：官方文档没写，由 fake_volume 的数据验证得出
- **[代码]**：本仓库代码定义的字段
- **[推断]**：官方文档只有字段名没有说明，含义是从字段名和数值推断的，使用前要自己核实

---

## 目录

1. [ws：WebSocket 行情消息](#1-wswebsocket-行情消息)
2. [price_change 详解](#2-price_change-详解)
3. [一笔成交的消息顺序](#3-一笔成交的消息顺序)
4. [markets：每轮 discovery 的市场状态](#4-markets每轮-discovery-的市场状态)
5. [events：WS 连接生命周期事件](#5-eventsws-连接生命周期事件)
6. [metrics：每秒运行指标](#6-metrics每秒运行指标)
7. [四个 stream 怎么关联](#7-四个-stream-怎么关联)
8. [对 silver / replay engine / 策略的影响](#8-对-silver--replay-engine--策略的影响)
9. [参考资料](#9-参考资料)

---

## 1. ws：WebSocket 行情消息

来源：`wss://ws-subscriptions-clob.polymarket.com/ws/market`，按 token（asset）订阅。fake_volume 里有 17170 行。

### 1.1 外层（WAL 包装，`Walwriter.write()` 写入）[代码]

```json
{"ts_recv_ns": 1794338129896376000,
 "ts_recv_iso": "2026-11-10T19:15:29.896376000Z",
 "asset_ids": ["3934127181…", "2179537692…", "…"],
 "raw": "[{\"market\":\"0x72827c…\",\"asset_id\":…}]"}
```

| 字段 | 类型 | 作用 |
|---|---|---|
| `ts_recv_ns` | int | 本机收到这条消息的时间（epoch 纳秒）。回测里的 `local_ts` 就是它，决定策略什么时候「看到」这条数据；它的 UTC 日期也决定写进哪个日期目录 |
| `ts_recv_iso` | str | 同一个时间的可读格式，只方便人看，bronze 会丢掉 |
| `asset_ids` | list[str] | 这条消息涉及哪些 token。初始订阅时是一整批（例如 200 个），平时是 1–2 个。用来不解析 `raw` 就能按 asset 过滤 |
| `raw` | str | Polymarket 发来的原始 JSON 文本。通常是一个对象；**订阅时收到的初始 book 是一个数组**，一个数组装着这条连接上所有 token 的 book |

### 1.2 `raw` 的消息类型

| `event_type` | 什么时候推 | fake_volume 条数 |
|---|---|---|
| `book` | 订阅时（每个 token 一条），以及成交之后 [文档] | 520 |
| `price_change` | 某个价位的挂单总量变了：下单、撤单 [文档]，以及成交 [数据] | 16988 |
| `last_trade_price` | 发生一笔成交 [文档] | 60 |
| `tick_size_change` | 价格超过 0.96 或低于 0.04 时，tick size 改变 [文档] | 0 |
| `best_bid_ask` / `new_market` / `market_resolved` | 只有订阅时设置 `custom_feature_enabled: true` 才会推 [文档]。现在的采集端**没有开** | 0 |

### 1.3 `book`：某个 token 的完整盘口快照

```json
{"event_type": "book",
 "market": "0x72827c3ffa…e268",
 "asset_id": "3934127181…7764",
 "timestamp": "1794338129648",
 "hash": "18740b68957eac242f228d6dbc3ab30c0551d274",
 "bids": [{"price": "0.001", "size": "11741.59"}, {"price": "0.002", "size": "137.5"}, "…共 10 档"],
 "asks": [{"price": "0.999", "size": "17000"}, {"price": "0.998", "size": "106"}, "…共 62 档"],
 "tick_size": "0.001",
 "last_trade_price": "0.983"}
```

| 字段 | 作用 | 依据 |
|---|---|---|
| `event_type` | 固定是 `"book"` | [文档] |
| `market` | 市场的 conditionId（0x 开头）。等于 Gamma 的 `conditionId`，**不是** Gamma 的数字 `id` | [文档] |
| `asset_id` | token id，也就是某个 outcome（比如 YES）的 id。一个二元市场有 2 个 token，各自有一本盘口 | [文档] |
| `timestamp` | 交易所时间，Unix **毫秒**，字符串 | [文档] |
| `hash` | 这一时刻盘口内容的哈希。两条快照哈希相同，说明盘口没变 | [文档] |
| `bids` / `asks` | 买盘、卖盘各档，`price` 和 `size` 都是字符串。`size` 是该价位的**总量**。按官方 REST 文档，最优价排在数组**最后**（bids 价格升序，asks 价格降序）；为保险起见，代码里最好用 max/min 求最优价，不要依赖顺序 | [文档] |
| `tick_size` | 最小价格单位（`0.01` 或 `0.001`）。只有初始订阅返回的 book 里有。tick size 会变，见 `tick_size_change` | [文档] |
| `last_trade_price` | 最近一次成交价。只有初始订阅返回的 book 里有 | [数据] |

两种 book 要分开处理 [数据]：

- **初始订阅返回的 book**（400 条 = 2 条连接 × 200 个 token）：装在数组里，带 `tick_size` 和 `last_trade_price`。它们的 `timestamp` 是上次成交的时间，**不是发送时间**，所以不能拿来算延迟。回测用它们建立初始盘口。
- **成交后推送的 book**（120 条 = 60 笔成交 × YES、NO 两个 token）：单独推送，不带 `tick_size`。和同一笔成交的 `price_change` 共用同一个 `timestamp`，见[第 3 节](#3-一笔成交的消息顺序)。

有 21 条 book 的 `bids` 是空的，表示那一侧没有挂单，属于正常情况。

### 1.4 `price_change`

见[第 2 节](#2-price_change-详解)。

### 1.5 `last_trade_price`：一笔成交

```json
{"event_type": "last_trade_price",
 "market": "0xefa17dee3a…2ed2",
 "asset_id": "3333979837…9665",
 "price": "0.002", "size": "5", "side": "BUY",
 "fee_rate_bps": "0",
 "timestamp": "1794338129885",
 "transaction_hash": "0xd70142ff…c92"}
```

| 字段 | 作用 | 依据 |
|---|---|---|
| `price` / `size` | 成交价、成交量。`size` 可能有 6 位小数（例如 `"111.111112"`） | [文档]，[数据] |
| `side` | **从 taker（主动方）的视角**。`BUY` 表示有人主动买，吃掉了卖单。回测里被动方的队列会被消耗，你在那个价位的挂单才有机会成交 | [文档] |
| `fee_rate_bps` | 这笔成交的手续费率（基点）。文档没有说明，费用的实际公式见[第 4.2 节](#42-raw-gamma-市场对象)的手续费部分 | [推断] |
| `timestamp` | 成交时间（毫秒）。**比同一笔成交引起的盘口更新晚 15–30ms** | [数据] |
| `transaction_hash` | 链上交易哈希（Polygon），可以去区块浏览器核对，也可以用来去重 | [文档] |

### 1.6 `tick_size_change`（这份数据里没有）

```json
{"event_type": "tick_size_change", "market": "0x…", "asset_id": "…",
 "old_tick_size": "0.01", "new_tick_size": "0.001", "timestamp": "…"}
```

价格超过 0.96 或低于 0.04 时触发 [文档]。之后的报价必须用新的 tick size，否则订单会被拒绝。Gamma 的 `orderPriceMinTickSize` 只是抓取那一刻的值，所以 silver 要以这条消息为准，按时间维护每个 token 的 tick size。

---

## 2. price_change 详解

### 2.1 一句话

**某个价位上的挂单总量变了，`price` 本身没变。** `price` 只用来指明是哪一档，`size` 是这一档变化后的**新总量**，`"0"` 表示这一档被清空。

官方原文：触发条件是 "Delta update to orderbook price levels when an order is placed or cancelled"；`size` 是 "New aggregate size (0 means level removed)" [文档]。

### 2.2 例子

```json
{"event_type": "price_change", "market": "0x1c4c6a…", "timestamp": "1794338129747",
 "price_changes": [
   {"asset_id": "347329…", "price": "0.61", "size": "11372.39", "side": "BUY",
    "best_bid": "0.68", "best_ask": "0.69", "hash": "8f79d2…"},
   {"asset_id": "639152…", "price": "0.39", "size": "11372.39", "side": "SELL",
    "best_bid": "0.31", "best_ask": "0.32", "hash": "9a077d…"}]}
```

意思是：YES 这个 token 在 **0.61 这一档买盘的挂单总量现在是 11372.39 股**。是多了还是少了，要和你自己维护的盘口里这一档原来的量比较才知道。

### 2.3 字段

| 字段 | 含义 | 依据 |
|---|---|---|
| `market` | 市场的 conditionId | [文档] |
| `timestamp` | 交易所时间（毫秒），整条消息共用 | [文档] |
| `price_changes[]` | 这次变化的列表 | [文档] |
| ↳ `asset_id` | 变化发生在哪个 token | [文档] |
| ↳ `price` | 哪一档价格 | [文档] |
| ↳ `size` | 这一档变化后的**总量**，不是增量。`"0"` = 这一档清空（数据里有 3978 条） | [文档] |
| ↳ `side` | `BUY` = 买盘那一侧，`SELL` = 卖盘那一侧 | [数据]，见下 |
| ↳ `best_bid` / `best_ask` | **变化之后**这个 token 的最优买价、卖价。某一侧没有挂单时显示为 `"0"` / `"1"` | [数据]，见下 |
| ↳ `hash` | **引起这次变化的那笔订单**的哈希 | [文档] |

**`side` 的验证** [数据]：16988 条消息里，BUY 的价格都 ≤ `best_bid`，SELL 的价格都 ≥ `best_ask`。例外有 380 条，**全部**是 `size=0`：最优价那一档被清空了，所以变化后的 `best_bid` 自然比它低。这同时说明 `best_bid` / `best_ask` 是变化**之后**的值。

**`best_bid` / `best_ask` 的用途**：做对账。你自己维护的盘口在应用完这条变化后，算出来的最优价必须和它一致；不一致就说明漏了消息或处理错了。

### 2.4 YES 和 NO 互为镜像

fake_volume 里的 16988 条 `price_change` **全部**是同一个结构：恰好 2 条变化，两个不同的 token，价格加起来等于 1，量相同，方向相反 [数据]。

原因：二元市场的 YES 和 NO 在**同一本订单簿**里撮合。$0.60 买 YES 的订单可以和 $0.40 买 NO 的订单成交，因为两者加起来是 $1；每一对 YES + NO 由 $1 抵押品支持 [文档]。所以「YES 0.61 的买单」和「NO 0.39 的卖单」是**同一笔流动性**，在两个 token 的视角下各显示一次。

推论：

```
NO 的卖价 = 1 − YES 的买价
NO 的买价 = 1 − YES 的卖价
YES 卖价 + NO 卖价 = 1 + 价差 > 1
```

### 2.5 什么时候会推

| 情况 | 依据 |
|---|---|
| 新挂单（这一档量增加，或出现新的一档） | [文档] |
| 撤单（这一档量减少，或 `size=0` 清空） | [文档] |
| **成交**（被吃掉的那一档量减少） | [数据]：60 笔成交里有 59 笔，在成交价那一档先收到一条 `price_change`。官方文档只写了下单和撤单 |

---

## 3. 一笔成交的消息顺序

fake_volume 里实际看到的顺序（时间是相对于 `last_trade_price` 到达的时刻）[数据]：

```
 -17.2ms  price_change      ts=…965  YES BUY 0.91 size=25304.65 | NO SELL 0.09 size=25304.65   ← 被吃掉的那档量减少
 -17.2ms  book              ts=…965  YES 完整盘口                                               ← 成交后重发快照
 -17.2ms  book              ts=…965  NO  完整盘口
   0.0ms  last_trade_price  ts=…984  NO BUY 0.09 x111.11                                       ← 成交通知晚约 15–30ms
```

要点：

1. 盘口先更新（`price_change` + YES、NO 两条 `book`，三者 `timestamp` 相同），成交通知后到。
2. `last_trade_price` 的 `timestamp` 比盘口更新晚 15–30ms，到达时间也晚差不多这么多。
3. 60 笔成交 × 2 个 token = 120 条成交后的 book，数量正好对上。

---

## 4. markets：每轮 discovery 的市场状态

orchestrator 每轮 discovery（间隔 `DISCOVERY_INTERVAL_SEC`）给每个相关市场写一行。fake_volume 里只有 1 轮、200 行。

### 4.1 外层 [代码]

```json
{"ts_recv_ns": 1794338129502492000,
 "market_id": "3215007",
 "source": "top",
 "rank": 1,
 "sub": {"action": "added", "reason": null, "missing_strikes": 0},
 "raw": {"…Gamma 返回的完整市场对象…": "…"}}
```

| 字段 | 作用 |
|---|---|
| `ts_recv_ns` | 这一轮 discovery 的时间。同一轮所有行都一样，可以用它把一轮的结果分组 |
| `market_id` | Gamma 的数字 id（字符串） |
| `source` | 这条数据从哪来。`top`：本轮按 24h 成交量排进了前 N（`TOP_N_MARKETS`）；`lookup`：已订阅、但掉出前 N 的市场，按 id 单独查回来的 |
| `rank` | 在本轮 top-N 里排第几（1 = 成交量最大）。`lookup` 的行是 `null` |
| `sub.action` | 本轮对这个市场做了什么：`added` 新订阅，`kept` 继续订阅，`removed` 退订 |
| `sub.reason` | 退订原因：`ended`（`closed`，或不再接受订单），`cap`（超过 `MAX_SUBSCRIBED_MARKETS`，按成交量淘汰），`not_found`（连续几轮查不到），`lookup_failed`。不退订时是 `null` |
| `sub.missing_strikes` | 已经连续几轮按 id 查不到。达到 `MISSING_STRIKES_TO_REMOVE`（3）就退订 |
| `raw` | Gamma `/markets/keyset` 返回的市场对象，是一个 **JSON 对象**，不是字符串。查不到时为 `null` |

fake_volume 只跑了一轮，所以 200 行都是 `top / added`。`kept`、`removed` 和 `lookup` 要跑多轮才会出现。

### 4.2 `raw`：Gamma 市场对象

共 105 个以上的字段，不同市场字段不完全相同（例如体育市场多出几个）。

**注意 1：官方文档对这些字段几乎没有说明。** Gamma API 的 schema 只列了字段名和类型。下面没有标 [文档] 的说明都是 [推断]。

**注意 2：好几个字段是「字符串里套 JSON」**，例如 `outcomes: "[\"Yes\", \"No\"]"`，要再解析一次（代码里是 `parse_json_list`）。数值也有两种格式：`volume: "6237542.2"` 是字符串，`volumeNum: 6237542.2` 是数字。

例子都来自第一行（「Fed 12 月议息后利率不变」这个市场）。

#### 身份与关联（silver 一定要用）

| 字段 | 例子 | 作用 |
|---|---|---|
| `id` | `"3215007"` | Gamma 数字 id，等于外层的 `market_id` |
| `conditionId` | `"0x723822eb…11c0"` | 链上市场 id，**等于 ws 消息里的 `market`**，用来把行情和市场关联起来 |
| `questionID` | `"0xa74f5328…d702"` | UMA 预言机里的问题 id |
| `clobTokenIds` | `"[\"4286572874…\", \"8077351030…\"]"` | 这个市场的 token id，**等于 ws 里的 `asset_id`**。顺序和 `outcomes` 一一对应 |
| `outcomes` | `"[\"Yes\", \"No\"]"` | 结果名称。第 1 个 token 是 Yes，第 2 个是 No |
| `slug` | `"will-there-be-no-change-in-fed-…"` | 网址里的名字 |
| `question` / `description` | `"Will there be no change in Fed…"` | 问题标题和判定规则 |
| `events` | `[{"id": "770450", "ticker": "fed-decision-in-december-…"}]` | 所属的父事件。一个事件下有多个市场，例如「12 月 Fed 决议」下有「不变 / 降 25bp / …」 |
| `groupItemTitle` / `groupItemThreshold` | `"No change"` / `"2"` | 这个市场在父事件里显示的名称和排序 |

#### 交易状态（决定订阅、退订）

| 字段 | 例子 | 作用 |
|---|---|---|
| `active` | `true` | 市场已上线 |
| `closed` | `false` | 已关闭。`true` 就退订（`ended`）。Gamma 默认只返回 `closed=false` 的市场 [文档] |
| `acceptingOrders` | `true` | 现在能否下单。`false` 就退订（`ended`），也用来过滤 top-N |
| `acceptingOrdersTimestamp` | `"2026-07-30T00:42:48Z"` | 开始接受订单的时间 |
| `enableOrderBook` | `true` | 有订单簿（CLOB）。只有 `true` 的市场才有 ws 行情 |
| `archived` / `restricted` | `false` / `true` | 已归档 / 有地区限制 |
| `startDate` / `endDate` | `"2026-07-30T00:43:48Z"` / `"2026-12-09T23:59:00Z"` | 开始和预计结束时间。**不用 `endDate` 判断市场是否结束**：体育市场的 `endDate` 是开赛时间，开赛后还会继续交易 |
| `startDateIso` / `endDateIso` | `"2026-07-30"` / `"2026-12-09"` | 只有日期部分 |
| `createdAt` / `updatedAt` | `"2026-07-30T00:36:57Z"` / `"2026-11-10T19:12:14Z"` | Gamma 里这条记录的创建和更新时间 |

#### 交易规则（回测必须用）

| 字段 | 例子 | 作用 |
|---|---|---|
| `orderPriceMinTickSize` | `0.001` | 最小价格单位，策略报价要按它取整。只是抓取那一刻的值，以 ws 的 `tick_size_change` 为准 |
| `orderMinSize` | `5` | 每笔订单最少几股 |
| `negRisk` | `true` | 属于多选一（negRisk）事件：同一事件下各选项互斥，所有选项的 YES 价格之和应该约等于 1 |
| `negRiskMarketID` / `negRiskRequestID` / `negRiskOther` | `"0xa74f…d700"` / … / `false` | negRisk 组的 id；`negRiskOther` 表示「其他」这个选项 |
| `secondsDelay` | （体育市场才有） | 体育比赛进行期间，**会立即成交的订单（taker）**要等这么多秒才能撮合；挂单不受影响 [文档]。回测体育市场时 taker 订单要按这个延迟处理 |
| `rfqEnabled` / `holdingRewardsEnabled` | `false` / `false` | 是否支持询价 / 是否有持仓奖励 |

#### 手续费

| 字段 | 例子 | 作用 |
|---|---|---|
| `feesEnabled` / `feeType` | `true` / `"economics_fees"` | 是否收手续费，以及收费类型 |
| `feeSchedule` | `{"rate": 0.05, "exponent": 1, "takerOnly": true, "rebateRate": 0.25}` | 手续费参数，含义见下 |
| `makerBaseFee` / `takerBaseFee` | `1000` / `1000` | 文档没有说明，含义不明 [推断] |

官方手续费规则 [文档]：

```
fee = C × feeRate × p × (1 − p)        C = 成交股数，p = 成交价
```

- **maker 从不收费，只有 taker 付费**，在撮合时扣除。
- feeRate 按市场类别不同，例如 Crypto 0.07、Sports 0.05。
- 收到的手续费用来给 maker 返佣，比例按类别不同：Crypto 20%、Sports 15%、Combos 0%，大部分其他类别 25%。

和 `feeSchedule` 的对应关系：`rate` = feeRate，`takerOnly` = 只收 taker，`rebateRate` = maker 返佣比例。这三个和文档吻合。`exponent` 文档没有说明 [推断]。

#### 流动性奖励（做市策略的额外收益）

| 字段 | 例子 | 作用 |
|---|---|---|
| `rewardsMinSize` | `200` | 挂单至少多少股才计入奖励 [文档] |
| `rewardsMaxSpread` | `4.5` | 挂单离中间价不超过多少**美分**才计入奖励 [文档] |
| `clobRewards` | （约 80 行有） | 奖励配置的详细内容 [推断] |

奖励低于 $1 不发放 [文档]。体育市场在比赛开始后，挂单要存在超过 `secondsDelay` 才计入奖励 [文档]。

#### 成交量与价格（Gamma 抓取那一刻的快照）

| 字段 | 例子 | 作用 |
|---|---|---|
| `volume24hr` | `2125447.8` | 24h 成交量。**top-N 按它排序**，cap 淘汰也按它比较 |
| `volume1wk` / `volume1mo` / `volume1yr` / `volume` / `volumeNum` | `3464179.9` / … / `6237542.2` | 不同时间窗口的成交量，以及累计成交量 |
| `volume24hrClob` 等 `*Clob` | 同上 | 只算 CLOB 订单簿上的成交量 |
| `liquidity` / `liquidityNum` / `liquidityClob` | `816948.09` | 盘口深度估计 |
| `bestBid` / `bestAsk` / `spread` / `lastTradePrice` | `0.035` / `0.036` / `0.001` / `0.036` | 抓取那一刻的最优价、价差、最新成交价。可以和 ws 的盘口对照检查 |
| `outcomePrices` | `"[\"0.0355\", \"0.9645\"]"` | 各结果的当前价格（Yes + No ≈ 1） |
| `oneHourPriceChange` / `oneDayPriceChange` / `oneWeekPriceChange` / `oneMonthPriceChange` / `oneYearPriceChange` | `-0.0005` / `-0.1395` / … | 价格变化，可以用来挑波动大的市场 |
| `competitive` | `0.8225` | Gamma 算的竞争度分数，越接近 1 表示盘口越接近五五开 |

#### 结算（UMA 预言机）

`resolutionSource`（判定依据的网址）、`resolvedBy`、`umaBond` / `umaReward`、`umaResolutionStatuses` / `umaResolutionStatus`、`customLiveness`、`resolutionMechanism`（例如 `"oracle"`）、`fallbackResolutionType`。

用途：知道市场最后怎么结算，回测时持仓到期按 0 或 1 结算。也可以改用 ws 的 `market_resolved` 消息（需要开启 `custom_feature_enabled`），它直接给出 `winning_asset_id` 和 `winning_outcome` [文档]。

#### 体育市场专有（约 56 行有）

`gameStartTime`、`gameId`、`sportsMarketType`、`line`（让分或大小盘的盘口线）、`secondsDelay`、`eventStartTime`。

#### 展示与内部状态（回测用不到）

`image`、`icon`、`seriesColor`、`new`、`featured`、`submitted_by`、`marketMakerAddress`、`hasReviewedDates`、`comboStatus`、`ready`、`funded`、`cyom`、`approved`、`automaticallyActive`、`manualActivation`、`clearBookOnStart`、`pendingDeployment`、`deploying`、`deployingTimestamp`、`showGmpSeries`、`showGmpOutcome`、`tradingEligible`、`version`、`positionIds`、`marketMetadata`。

---

## 5. events：WS 连接生命周期事件

每条 WS 连接的状态变化写一行 [代码]。fake_volume 里有 8 行。

```json
{"ts_ns": 1794338129697565000, "conn_id": 0, "event": "connected",
 "extra": {"assets": ["1127675323…", "…共 200 个"], "asset_count": 200}}
```

| 字段 | 作用 |
|---|---|
| `ts_ns` | 事件发生的时间（本机纳秒） |
| `conn_id` | 哪条 WS 连接（0、1、…）。每条连接最多订阅 `MAX_PER_CONNECTION`（200）个 token |
| `event` | 事件类型，见下表 |
| `extra` | 附加信息，内容因事件类型而异 |

| `event` | 什么时候出现 | `extra` |
|---|---|---|
| `connected` | 连接建立 | `assets`：这条连接负责的全部 token；`asset_count` |
| `subscribed` | 订阅消息已发出 | `operation`：`initial`（连上后第一次订阅）或 `subscribe`（之后增加订阅）；`assets`：本次订阅的 token；`asset_count` |
| `unsubscribed` | 退订消息已发出 | 本次退订的 `assets`、`asset_count` |
| `disconnected` | 连接断开（之后会重连） | `{}` |
| `reconnecting` | 准备重连 | `attempt`：第几次重试；`backoff_sec`：等多少秒再试 |
| `watchdog_timeout` | 太久没收到数据，强制断开重连 | `idle_sec`：已经多少秒没数据 |
| `closed` | 连接彻底关闭，不再重连（程序退出时） | `{}` |

fake_volume 的 8 行：两条连接各有 `connected → subscribed(initial)`，约 21 秒后程序退出，各有 `disconnected → closed`。

用途：silver 的 `subscription_intervals` 从这些事件推出来。某个 token 从 `subscribed` 到 `disconnected` / `unsubscribed` 之间的数据才可信；中间的空档要么是断线，要么是没订阅，不能当成「盘口没变化」。

---

## 6. metrics：每秒运行指标

orchestrator 每秒写一条，只用于监控 [代码]。fake_volume 里有 21 行。

```json
{"ts_ns": 1794338150246816000, "t_rel_s": 21.11,
 "msgs_total": 17128, "msgs_per_sec_window": 841, "bytes_per_sec_window": 525600,
 "assets_seen": 400,
 "lat_window_ms": {"n": 841, "mean": 15.2, "median": 14.2, "p95": 23.9, "max": 44.6},
 "errors": {"json_decode": 0, "missing_timestamp": 0, "disconnects": 0, "reconnects": 0, "watchdog": 0},
 "errors_per_conn": {},
 "msgs_per_conn": {"0": 6334, "1": 10794}}
```

| 字段 | 作用 |
|---|---|
| `ts_ns` | 采样时间 |
| `t_rel_s` | 程序启动后第几秒 |
| `msgs_total` | 启动以来累计收到的消息数 |
| `msgs_per_sec_window` / `bytes_per_sec_window` | 过去这 1 秒收到的消息数和字节数 |
| `subscribed_markets` / `subscribed_tokens` | 当前订阅的市场数和 token 数（新版字段） |
| `assets_seen` | 收到过行情的 token 数（**旧版**字段，新版已经去掉） |
| `lat_window_ms` | 过去 1 秒的延迟统计：`ts_recv` 减去消息的 `timestamp`，单位毫秒。`n` 是样本数，另有 mean / median / p95 / max。**不包含初始 book**，因为它们的 timestamp 不是发送时间 |
| `errors.json_decode` | 解析失败的消息数 |
| `errors.missing_timestamp` | 没有 `timestamp`，或 `timestamp` 不是数字的消息数 |
| `errors.disconnects` / `reconnects` / `watchdog` | 断线、重连、看门狗超时的累计次数 |
| `errors_per_conn` | 每条连接各出错几次（断线 + 看门狗） |
| `msgs_per_conn` | 每条连接各收到多少消息，可以看负载是否均衡 |

注意：fake_volume 里的 metrics 是旧版代码写的，有 `assets_seen`，没有 `subscribed_markets` / `subscribed_tokens`。现在的代码（`src/orchestrator.py` 里的 `snapshot()`）写的是新版字段。以后如果要导入 metrics，schema 要按新版写。

---

## 7. 四个 stream 怎么关联

```
markets.raw.conditionId   ==  ws.raw.market
markets.raw.clobTokenIds  ==  ws.raw.asset_id
                          ==  ws.raw.price_changes[].asset_id
                          ==  ws.asset_ids 里的元素
events.extra.assets       ==  同一批 token id（判断某个时刻某个 token 是否在订阅中）
```

`clobTokenIds` 的顺序和 `outcomes` 一致，所以可以知道每个 `asset_id` 是 Yes 还是 No。

---

## 8. 对 silver / replay engine / 策略的影响

### 8.1 silver

- `raw` 是 JSON 数组时（初始订阅），要拆成多行。
- `price_change` 一条消息里有 YES、NO 两条镜像变化。两个 token 的盘口在 engine 里是分开维护的，所以两条都要保留，但要记住它们是**同一笔流动性**。
- 初始订阅的 book 要标记出来（例如 `is_initial`），统计延迟时排除。
- tick size 以 `tick_size_change` 为准，按时间维护，不能只用 Gamma 的快照值。
- 某一侧没有挂单时，`best_bid` / `best_ask` 是 `"0"` / `"1"`，整数化时不要当成真实价格。

### 8.2 replay engine

- **成交会被重复计算。** 成交时盘口先被 `price_change` 减量，15–30ms 后才收到 `last_trade_price`。如果队列模型在 depth 减少时推进一次队列位置，收到 trade 时又推进一次，你的挂单会成交得比实际更快。需要在 silver 或队列模型里处理，例如把 trade 和之前同价位的减量配对。
- **手续费模型要改。** `stats.py` 里的 `BpsFee` 按成交金额 × 基点计费，和官方公式 `C × feeRate × p × (1 − p)` 不一样。应该改成：maker 免费，taker 按 `p(1−p)` 计费，maker 另有返佣。
- 体育市场的 taker 订单要加上 `secondsDelay` 的延迟。

### 8.3 策略

- **同一个二元市场里不存在 YES/NO 套利。** YES 卖价 + NO 卖价 = 1 + 价差，永远大于 1。
- 有可能出现套利的是 **negRisk 多选一事件**：同一事件下的多个市场是各自独立的订单簿，所有选项的 YES 价格之和偏离 1 时才有机会。前提是同一事件的所有市场都要订阅。
- 做市策略要把流动性奖励（`rewardsMinSize`、`rewardsMaxSpread`）和 maker 返佣算进收益。

### 8.4 采集端可选的改进

订阅时设置 `custom_feature_enabled: true`，就能额外收到：

- `new_market`：新市场上线。发现新市场比 Gamma 每轮轮询更及时。
- `market_resolved`：市场结算，带 `winning_asset_id` / `winning_outcome`，回测可以直接用。
- `best_bid_ask`：最优价变化。

---

## 9. 参考资料

- [Market channel – API reference](https://docs.polymarket.com/api-reference/wss/market.md)：`price_change`、`book`、`last_trade_price`、`tick_size_change` 的触发条件和字段说明
- [Market channel – 字段定义](https://docs.polymarket.com/market-data/websocket/market-channel.md)：订阅参数（`custom_feature_enabled`、`initial_dump`）和所有消息类型的字段
- [Orderbook](https://docs.polymarket.com/trading/orderbook)：REST 盘口的排序、`hash`
- [Prices & orderbook](https://docs.polymarket.com/concepts/prices-orderbook)：YES/NO 互补撮合
- [CLOB 概览](https://docs.polymarket.com/developers/CLOB)：unified order book
- [How positions work](https://docs.polymarket.com/trading/positions/how-positions-work)：每对 YES + NO 由 $1 抵押品支持
- [CTF 概览](https://docs.polymarket.com/developers/CTF/overview)：split / merge / redeem
- [Fees](https://docs.polymarket.com/trading/fees)：手续费公式、只收 taker、maker 返佣
- [Market details](https://docs.polymarket.com/market-data/market-details)：每个市场的手续费参数
- [Liquidity rewards](https://docs.polymarket.com/programs/liquidity-rewards)：`rewards_min_size`、`rewards_max_spread`
- [Place orders](https://docs.polymarket.com/trading/orders/create)：体育市场下单延迟
- [Order lifecycle](https://docs.polymarket.com/concepts/order-lifecycle)
- [List markets](https://docs.polymarket.com/api-reference/markets/list-markets.md)：Gamma 市场字段 schema（只有名字和类型）
