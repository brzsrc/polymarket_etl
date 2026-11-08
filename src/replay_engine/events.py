"""统一事件模型。价格/数量全部整数化 (hftbacktest 同样用 tick 整数), 消灭 1e-12 比较。"""
from __future__ import annotations
import msgspec

TICK = 1_000        # 内部价格网格: 1 tick = 0.001。样本里 0.01 市场 (0.7/0.71) 和 0.001 市场 (0.886/0.011) 都落在这个格上
QTY_SCALE = 100     # 内部数量单位: 0.01 股。"27020.01" -> 2702001

def px_of(s: str) -> int:  return round(float(s) * TICK)
def qty_of(s: str) -> int: return round(float(s) * QTY_SCALE)

class Level(msgspec.Struct, frozen=True):
    px: int
    qty: int

class Delta(msgspec.Struct, frozen=True, kw_only=True, tag=True):
    asset_id: str
    exch_ts: int        # ns。Polymarket 只给 ms -> 同一 ms 内的多条事件靠到达顺序 (seq) 定序
    local_ts: int       # ns。WAL 的 ts_recv_ns (+ 时钟校正偏移)
    side: str           # "B" | "A"
    px: int
    qty: int            # 该价位的绝对新量, 不是增量
    best_bid: int       # 报文自带 top-of-book: 每条 delta 一次免费对账
    best_ask: int
    eom: bool = True    # 本报文中该 asset 的最后一条变化; 只在此时做 top-of-book 对账

class Snapshot(msgspec.Struct, frozen=True, kw_only=True, tag=True):
    asset_id: str
    exch_ts: int
    local_ts: int
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]

class Trade(msgspec.Struct, frozen=True, kw_only=True, tag=True):
    asset_id: str
    exch_ts: int
    local_ts: int
    px: int
    qty: int
    aggressor: str      # taker 方向 "B"/"A"; 被动方 = 反向, 队列消耗看被动方

MarketEvent = Delta | Snapshot | Trade