"""接口 = hftbacktest QueueModel trait (new_order / trade / depth)。
trade() 返回这笔同价成交里轮到我的量 (partial-fill 语义)。穿透价与触价成交不经过这里, 由 ExchangeSim 无条件处理。"""
from __future__ import annotations
from .orders import Order

class QueueModel:
    def new_order(self, o: Order, level_qty: int) -> None:
        o.front, o.traded = level_qty, 0                 # 到达时刻该价位的全部量都在我前面
    def trade(self, o: Order, qty: int) -> int:
        eat = min(qty, o.front)
        o.front -= eat; o.traded += qty
        return min(qty - eat, o.remaining)               # 穿过队列轮到我的量
    def depth(self, o: Order, prev: int, new: int) -> None:
        raise NotImplementedError

class HeadOfQueue(QueueModel):
    """上界: 永远在队首, 同价成交全额轮到我。"""
    def new_order(self, o, level_qty): o.front, o.traded = 0, 0
    def depth(self, o, prev, new): o.front, o.traded = 0, 0

class RiskAverse(QueueModel):
    """hftbacktest RiskAverseQueueModel: 撤单/改单一律发生在我身后, 队列只靠同价成交推进 (封顶于新量)。"""
    def depth(self, o, prev, new): o.front, o.traded = min(o.front, new), 0

class ProbQueue(QueueModel):
    """hftbacktest ProbQueueModel: 无成交的减量按 p = f(back)/(f(back)+f(front)) 归到我身后, 其余归到我前面。
    n=1 (f(x)=x) 就是 pro-rata —— 原骨架的 front *= new/prev; n=2,3 更激进; f 可换 log(1+x)。"""
    def __init__(self, n: float = 1.0): self.n = n
    def f(self, x: float) -> float: return x ** self.n
    def depth(self, o, prev, new):
        t, o.traded = o.traded, 0
        chg = prev - new - t                              # 撤单量 = 总减量 - 已由 Trade 事件推进过的量 (不重复计数)
        if chg <= 0:                                      # 增量: 我前面不变, 只需封顶
            o.front = min(o.front, new); return
        front = o.front
        back = max(prev - t - front, 0)                   # hftbacktest 用 prev - front; 扣掉 t 更准, 交叉验证时可切回
        fb, ff = self.f(back), self.f(front)
        p = fb / (fb + ff) if fb + ff > 0 else 1.0
        est = front - (1 - p) * chg + min(back - p * chg, 0)   # 归到身后的量超过 back 时, 超出部分必然来自我前面
        o.front = int(round(max(0.0, min(est, new))))   # round 不是 int: 1999.9999 截成 1999 会凭空多成 1 单位

class ThroughOnly(QueueModel):
    """下界 (原骨架 PESSIMISTIC): 同价成交永远不算我的, 只有穿透价/触价成交。比 RiskAverse 更悲观。"""
    def trade(self, o, qty): o.traded += qty; return 0
    def depth(self, o, prev, new): o.traded = 0