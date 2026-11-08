"""主循环: 单一事件堆 + 双时间线。堆和 tie-break 是 SimPy Environment.schedule 的 (time, seq, ...) 方案;
双时间线是 hftbacktest 的 local/exch 两 processor 取 min, 这里折叠成一个堆: 每个市场事件入堆两次。"""
from __future__ import annotations
import copy, heapq, itertools
from typing import Iterable
from .book import Book
from .events import MarketEvent
from .exchange_sim import ExchangeSim
from .latency import LatencyModel
from .orders import Order, Report, Req, St
from .queue_models import QueueModel
from .stats import FeeModel, Recorder, State, ZeroFee

EXCH, ORDER, LOCAL, REPORT = 0, 1, 2, 3

class Engine:
    def __init__(self, events: Iterable[MarketEvent], strategy, latency: LatencyModel, queue_model: QueueModel, *,
                 fee: FeeModel | None = None, lmax_ns: int = 5_000_000_000, recorder: Recorder | None = None):
        self._src = iter(events); self._peek = next(self._src, None)
        self._heap: list = []
        self._seq, self._oid = itertools.count(), itertools.count(1)   # 同 ts 的确定性 tie-break: 先入堆先出
        self.lat, self.lmax = latency, lmax_ns
        self.sim = ExchangeSim(queue_model, self._on_report)
        self.books_local: dict[str, Book] = {}          # 策略只看延迟后的世界
        self.orders: dict[int, Order] = {}              # 本地视角的订单, 只靠 Report 更新
        self.now, self.last_feed_lat = 0, 0
        self._req_arrival = self._rep_arrival = 0       # 请求/回报各走一条 FIFO 连接: 到达顺序不可乱
        self.state, self.recorder = State(fee or ZeroFee()), recorder
        self.strategy = strategy; strategy._bind(self)

    def _push(self, ts, kind, payload): heapq.heappush(self._heap, (ts, next(self._seq), kind, payload))

    def _pump(self) -> None:
        """堆顶时刻 T 之前的事件必须都已在堆里。源按 local_ts 有序而堆按 exch_ts 弹, 二者相差一个 feed latency,
        所以只要源的下一条 local_ts <= T + lmax 就继续灌 (lmax >= 全天最大 feed latency, 取 wal_stats 的 suggested_lmax_ns)。
        原骨架的"预灌 64 条"在延迟尖峰时会乱序。"""
        while self._peek is not None and (not self._heap or self._peek.local_ts <= self._heap[0][0] + self.lmax):
            ev, self._peek = self._peek, next(self._src, None)
            self._push(ev.exch_ts, EXCH, ev)
            self._push(ev.local_ts, LOCAL, ev)

    # ---- 策略动作 ----
    def submit(self, asset_id: str, side: str, px: int, qty: int) -> int:
        o = Order(next(self._oid), asset_id, side, px, qty)
        self.orders[o.oid] = o
        self._request("new", copy.copy(o))              # 交易所拿副本, 本地这份只靠回报更新
        return o.oid

    def cancel(self, oid: int) -> bool:
        o = self.orders.get(oid)
        if o is None or not o.active or o.req is not Req.NONE: return False   # 有在途请求时不能再发
        o.req = Req.CANCEL
        self._request("cancel", oid)
        return True

    def _request(self, kind: str, payload) -> None:
        self._req_arrival = max(self._req_arrival, self.now + self.lat.entry_ns(self.now, kind, self))
        self._push(self._req_arrival, ORDER, (kind, payload))

    def _on_report(self, rep: Report) -> None:
        self._rep_arrival = max(self._rep_arrival, rep.exch_ts + self.lat.response_ns(rep.exch_ts, self))
        self._push(self._rep_arrival, REPORT, rep)

    def run(self) -> None:
        while True:
            self._pump()
            if not self._heap: break
            ts, _, kind, p = heapq.heappop(self._heap)
            self.now = ts
            if kind == EXCH:
                self.sim.on_market(p, ts)
            elif kind == ORDER:
                what, x = p
                self.sim.on_new(x, ts) if what == "new" else self.sim.on_cancel(x, ts)
            elif kind == LOCAL:
                self.last_feed_lat = p.local_ts - p.exch_ts
                book = self.books_local.setdefault(p.asset_id, Book())
                book.apply(p)
                self.strategy.on_market(p, book, ts)
                if self.recorder is not None: self.recorder.maybe(ts, book, self.state)
            else:
                o = self.orders[p.oid]
                if p.kind in ("fill", "canceled"): o.st, o.filled = p.st, p.filled
                if p.kind != "fill": o.req = Req.NONE
                if p.kind == "fill": self.state.on_fill(o.side, p.px, p.fill_qty, p.maker, ts)
                self.strategy.on_report(p, o, ts)