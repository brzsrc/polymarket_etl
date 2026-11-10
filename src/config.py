"""
Collector 可调参数的默认值，只在这里定义一次。

各模块的构造函数用这些值做默认参数；每次运行要改的参数由 collector
（CLI / notebook）显式传入。协议常量（URL、接口上限、文件名格式等）
不是可调参数，留在各自模块里。
"""

# --- discovery（src/orchestrator.py）---------------------------------------

# 每轮按 Gamma 24h 成交量取前多少个市场（= 新订阅哪些市场）。
TOP_N_MARKETS = 200

# 同时订阅的市场数上限。每个市场约 2 个 token，每条 WS 连接 200 个 token，
# 1000 个市场约 10 条连接。必须 >= TOP_N_MARKETS（本轮 top-N 不会被淘汰）。
MAX_SUBSCRIBED_MARKETS = 1000

# discovery 间隔。新市场靠 Gamma 轮询发现，10 分钟对 Gamma 的压力也合理。
# DISCOVERY_INTERVAL_SEC = 600.0
DISCOVERY_INTERVAL_SEC = 30.0

# 已订阅的市场连续几轮按 id 查不到才退订（防止 Gamma 临时出错）。
MISSING_STRIKES_TO_REMOVE = 3

# --- WS 连接池（src/ws/pool.py）--------------------------------------------

# 每条 WS 连接最多订阅多少个 token。Polymarket 上限是 500，用 200：
# 留出余量；有人反馈 ~250 以上容易出现假死连接；和 nautilus_trader 的默认值一致。
MAX_PER_CONNECTION = 200

# --- WAL writer（src/wal/writer.py）----------------------------------------

# 活动 segment 达到这个大小（未压缩）就关闭、封存。
# SEGMENT_MAX_BYTES = 50 * 1024 * 1024  # 50 MiB
SEGMENT_MAX_BYTES = 1024 * 1024 * 1024  # 1 GiB

# fsync 很贵（1-10 ms），所以批量做：崩溃时最多丢这么多秒的数据。
FSYNC_INTERVAL_SEC = 1.0

# 后台多久检查一次 segment 大小、重试失败的封存。
# 按 ~7 MB/s 的写入速度，一个 segment 最多超出上限约 200 MB。
CHECK_INTERVAL_SEC = 30.0

# UTC 零点过后再等这么多秒，才关闭并封存前一天的 segment，
# 让零点前收到、还在队列里的消息先写进去。之后再迟到的写入会开一个新段号。
DAY_CLOSE_GRACE_SEC = 60.0

# --- uploader（src/wal/uploader.py）----------------------------------------

# 多久扫描一次本地已封存、待上传的文件。
UPLOAD_SCAN_INTERVAL_SEC = 15.0

# 退出时最多再花多少秒把剩下的文件传完；超时还没传完的会留在本地。
UPLOAD_EXIT_TIMEOUT_SEC = 300.0
