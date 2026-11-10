"""交易所时间线上的撮合模拟器 (= hftbacktest proc/partialfillexchange.rs)。
影子单假设: 我们的订单不出现在行情里、不改簿 (无市场冲击); 连吃单也只读簿 —— 原骨架 _take 会减簿, 那是给自己加冲击, 且与绝对量 delta 打架。
成交规则 (hftbacktest 文件头的三条, 对所有 queue model 无条件生效):
  买单: (1) 最优卖价 <= 我的价 -> 全成 (触价/交叉)   (2) 卖方主动成交价 < 我的价 -> 全成 (穿透)
        (3) 成交价 == 我的价 -> queue_model.trade() 决定成交量。卖单对称。三档 fill model 只在 (3) 上分歧。"""
from __future__ import annotations
from typing import Callable
from .book import Book
from .events import Delta, Snapshot, Trade, MarketEvent
from .orders import Order, Report, Req, St
from .queue_models import QueueModel

class ExchangeSim:
    def __init__(self, queue_model: QueueModel, emit: Callable[[Report], None]):
        self.qm, self.emit = queue_model, emit
        self.books: dict[str, Book] = {}
        self.orders: dict[int, Order] = {}                      # 交易所侧的订单副本
        self.by_level: dict[tuple[str, str, int], set[int]] = {}
        self.tob_mismatch = 0           # delta 自带 best_bid/best_ask 与重建簿不一致的次数
        self.snapshot_diff_levels = 0   # snapshot 对账: 与重建簿不一致的档数

    def book(self, aid: str) -> Book: return self.books.setdefault(aid, Book())

    # ---- 订单请求 (已含 entry latency, 由 engine 在到达时刻调用) ----
    def on_new(self, o: Order, ts: int) -> None:
        o.req = Req.NONE
        b = self.book(o.asset_id)
        self.emit(Report("ack", o.oid, ts, St.NEW, 0))
        opp = b.best_ask if o.side == "B" else b.best_bid
        if (o.px >= opp) if o.side == "B" else (o.px <= opp):
            self._take(o, b, ts)                                # marketable: 当 taker 沿对手盘逐档吃
        if o.remaining > 0:
            self.qm.new_order(o, b.qty(o.side, o.px))           # 排队基线 = 到达时刻该价位的量
            self.orders[o.oid] = o
            self.by_level.setdefault((o.asset_id, o.side, o.px), set()).add(o.oid)

    def on_cancel(self, oid: int, ts: int) -> None:
        o = self.orders.pop(oid, None)
        if o is None:                                           # 已成交/不存在 -> 拒绝 (= hftbacktest ack_cancel 的 Rejected)
            self.emit(Report("rejected", oid, ts, St.NEW, 0)); return
        self.by_level[(o.asset_id, o.side, o.px)].discard(oid)
        o.st = St.CANCELED
        self.emit(Report("canceled", oid, ts, St.CANCELED, o.filled))

    def _take(self, o: Order, b: Book, ts: int) -> None:
        arr = b.arr("A" if o.side == "B" else "B")
        levels = range(b.best_ask, o.px + 1) if o.side == "B" else range(b.best_bid, o.px - 1, -1)
        for px in levels:
            if o.remaining == 0: break
            avail = int(arr[px])
            if avail: self._fill(o, min(avail, o.remaining), px, ts, maker=False)

    # ---- 市场事件 (exch_ts 时刻) ----
    def on_market(self, ev: MarketEvent, ts: int) -> None:
        if isinstance(ev, Trade):
            self._on_trade(ev, ts); return
        b = self.book(ev.asset_id)
        changes = b.apply(ev)
        if isinstance(ev, Snapshot): self.snapshot_diff_levels += len(changes)
        for side, px, prev, new, prev_best, new_best in changes:
            for oid in list(self.by_level.get((ev.asset_id, side, px), ())):
                self.qm.depth(self.orders[oid], prev, new)      # 同侧同价挂单: 队列状态更新 (拿到原子的 prev/new)
            if side == "B" and new_best > prev_best:            # 最优买价上移 -> 价格 <= 新最优买价的卖单被吃
                self._cross(ev.asset_id, "A", new_best, ts)
            elif side == "A" and new_best < prev_best:          # 最优卖价下移 -> 价格 >= 新最优卖价的买单被吃
                self._cross(ev.asset_id, "B", new_best, ts)
        if isinstance(ev, Delta) and ev.eom and (b.best_bid, b.best_ask) != (ev.best_bid, ev.best_ask):
            self.tob_mismatch += 1

    def _cross(self, aid: str, side: str, best: int, ts: int) -> None:
        hit = [o for o in self.orders.values() if o.asset_id == aid and o.side == side
               and ((o.px <= best) if side == "A" else (o.px >= best))]
        for o in hit: self._fill(o, o.remaining, o.px, ts, maker=True)

    def _on_trade(self, ev: Trade, ts: int) -> None:
        passive = "A" if ev.aggressor == "B" else "B"
        for o in [o for o in self.orders.values() if o.asset_id == ev.asset_id and o.side == passive]:
            if (o.px > ev.px) if o.side == "B" else (o.px < ev.px):
                self._fill(o, o.remaining, o.px, ts, maker=True)       # 穿透价: 无条件全成
            elif o.px == ev.px:
                q = self.qm.trade(o, ev.qty)                            # 同价: 三档在此分歧
                if q: self._fill(o, q, o.px, ts, maker=True)

    def _fill(self, o: Order, qty: int, px: int, ts: int, maker: bool) -> None:
        o.filled += qty
        o.st = St.FILLED if o.remaining == 0 else St.PARTIAL
        if o.st is St.FILLED and self.orders.pop(o.oid, None) is not None:
            self.by_level[(o.asset_id, o.side, o.px)].discard(o.oid)
        self.emit(Report("fill", o.oid, ts, o.st, o.filled, qty, px, maker))