"""ROI-vector 簿 (hftbacktest ROIVectorMarketDepth 的特例)。Polymarket 价格 ∈ (0,1)、网格 0.001 -> 全域 999 档,
两个定长数组就是整本簿。update() 原子返回旧量/新量/旧最优/新最优 (= hftbacktest update_bid_depth 的返回值),
模拟器不再依赖"先判 fill 后 apply"的调用顺序。"""
from __future__ import annotations
import numpy as np
from .events import TICK, Delta, Snapshot

NO_BID, NO_ASK = 0, TICK   # 空簿哨兵 (= hftbacktest INVALID_MIN / INVALID_MAX); 真实价格 ∈ [1, 999]

class Book:
    __slots__ = ("bid", "ask", "best_bid", "best_ask", "last_snapshot_ts")

    def __init__(self) -> None:
        self.bid = np.zeros(TICK + 1, dtype=np.int64)
        self.ask = np.zeros(TICK + 1, dtype=np.int64)
        self.best_bid, self.best_ask = NO_BID, NO_ASK
        self.last_snapshot_ts = 0

    def arr(self, side: str): return self.bid if side == "B" else self.ask
    def qty(self, side: str, px: int) -> int: return int(self.arr(side)[px])
    def has_tob(self) -> bool: return self.best_bid != NO_BID and self.best_ask != NO_ASK
    def mid(self) -> float: return (self.best_bid + self.best_ask) / 2

    def update(self, side: str, px: int, qty: int) -> tuple[int, int, int, int]:
        """-> (prev_qty, new_qty, prev_best, new_best)"""
        arr = self.arr(side)
        prev = int(arr[px]); arr[px] = qty
        if side == "B":
            prev_best = self.best_bid
            if qty > 0:
                if px > self.best_bid: self.best_bid = px
            elif px == self.best_bid:
                while self.best_bid > NO_BID and arr[self.best_bid] == 0: self.best_bid -= 1
            return prev, qty, prev_best, self.best_bid
        prev_best = self.best_ask
        if qty > 0:
            if px < self.best_ask: self.best_ask = px
        elif px == self.best_ask:
            while self.best_ask < NO_ASK and arr[self.best_ask] == 0: self.best_ask += 1
        return prev, qty, prev_best, self.best_ask

    def apply(self, ev) -> list[tuple]:
        """-> 逐档变化 [(side, px, prev, new, prev_best, new_best)]。Trade 不改簿 (成交后的量变化由后续 price_change 体现)。
        Snapshot 走 diff (hftbacktest DiffOrderBookSnapshot 的思路): 只有真变化的档产生记录, 挂单在未变档位上的队列状态得以保留;
        变化档数就是 snapshot 对账指标。"""
        if isinstance(ev, Delta):
            return [(ev.side, ev.px, *self.update(ev.side, ev.px, ev.qty))]
        if isinstance(ev, Snapshot):
            out = []
            for side, levels in (("B", ev.bids), ("A", ev.asks)):
                arr, want = self.arr(side), {l.px: l.qty for l in levels}
                for px in sorted({int(p) for p in np.flatnonzero(arr)} | set(want)):
                    new = want.get(px, 0)
                    if int(arr[px]) != new:
                        out.append((side, px, *self.update(side, px, new)))
            self.last_snapshot_ts = ev.exch_ts
            return out
        return []