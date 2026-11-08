from __future__ import annotations
from dataclasses import dataclass
from enum import Enum

class St(Enum):  NEW = 1; PARTIAL = 2; FILLED = 3; CANCELED = 4        # 订单状态 (hftbacktest Status)
class Req(Enum): NONE = 0; NEW = 1; CANCEL = 2                         # 在途请求 (hftbacktest Order.req), 与状态分离

@dataclass
class Order:
    oid: int; asset_id: str; side: str; px: int; qty: int
    filled: int = 0
    st: St = St.NEW
    req: Req = Req.NEW
    front: int = 0       # queue model 的状态槽 (hftbacktest Order.q): 我前面排着的量
    traded: int = 0      # 上次簿量变化以来在我价位上发生的成交量 (用于去重, 见 ProbQueue.depth)
    @property
    def remaining(self) -> int: return self.qty - self.filled
    @property
    def active(self) -> bool: return self.st in (St.NEW, St.PARTIAL)

@dataclass(frozen=True)
class Report:
    """交易所 -> 本地的回报, 经 response latency 送达。本地订单只靠它更新 (不共享对象, 否则策略会提前看到成交)。"""
    kind: str            # "ack" | "fill" | "canceled" | "rejected"
    oid: int
    exch_ts: int
    st: St
    filled: int          # 累计成交
    fill_qty: int = 0
    px: int = 0
    maker: bool = True