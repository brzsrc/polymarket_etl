from __future__ import annotations
from .events import QTY_SCALE

class Strategy:
    def _bind(self, engine): self.e = engine
    def on_market(self, ev, book, ts): ...
    def on_report(self, rep, order, ts): ...

class NaiveQuoter(Strategy):
    """双边 join the touch。价位没变就不动 —— 原骨架每条簿变都撤挂, 永远排在队尾, queue model 会如实惩罚它。demo 用。"""
    def __init__(self, asset_id: str, qty: float = 10.0, max_pos: float = 50.0):
        self.aid, self.qty, self.max_pos = asset_id, int(qty * QTY_SCALE), int(max_pos * QTY_SCALE)
        self.live: dict[str, int] = {}

    def on_market(self, ev, book, ts):
        if ev.asset_id != self.aid or not book.has_tob() or book.best_ask - book.best_bid < 2: return
        pos = self.e.state.position
        for side, px in (("B", book.best_bid), ("A", book.best_ask)):
            oid = self.live.get(side)
            if oid is not None:
                o = self.e.orders[oid]
                if o.active:
                    if o.px != px: self.e.cancel(oid)     # 在途请求时返回 False, 下条行情再试
                    continue
                self.live.pop(side)                        # 已成交/已撤: 释放该边
            if (side == "B" and pos < self.max_pos) or (side == "A" and pos > -self.max_pos):
                self.live[side] = self.e.submit(self.aid, side, px, self.qty)

class Scripted(Strategy):
    """开环: 按脚本在指定时刻下单/撤单, 不看行情。用于三档支配性测试与确定性测试。"""
    def __init__(self, script):   # [(ts, "submit", (key, aid, side, px, qty)) | (ts, "cancel", key)]
        self.script, self.i, self.oids, self.reports = sorted(script, key=lambda x: x[0]), 0, {}, []
    def on_market(self, ev, book, ts):
        while self.i < len(self.script) and self.script[self.i][0] <= ts:
            _, act, arg = self.script[self.i]; self.i += 1
            if act == "submit":
                key, aid, side, px, qty = arg; self.oids[key] = self.e.submit(aid, side, px, qty)
            else:
                self.e.cancel(self.oids[arg])
    def on_report(self, rep, order, ts):
        self.reports.append((ts, rep.kind, rep.oid, rep.px, rep.fill_qty))