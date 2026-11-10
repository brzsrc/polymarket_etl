"""entry: 本地 -> 交易所 (下单/撤单请求); response: 交易所 -> 本地 (回报)。= hftbacktest LatencyModel 的两个方法。"""
from __future__ import annotations
import random

class LatencyModel:
    def entry_ns(self, ts: int, req: str, eng) -> int: raise NotImplementedError   # req: "new" | "cancel"
    def response_ns(self, ts: int, eng) -> int: raise NotImplementedError

class ConstantLatency(LatencyModel):
    def __init__(self, entry_ms: float = 50.0, response_ms: float = 50.0, cancel_ms: float | None = None):
        self.e, self.r = int(entry_ms * 1e6), int(response_ms * 1e6)
        self.c = self.e if cancel_ms is None else int(cancel_ms * 1e6)   # nautilus 把 cancel 延迟单列: 撤单通常比下单轻
    def entry_ns(self, ts, req, eng): return self.c if req == "cancel" else self.e
    def response_ns(self, ts, eng): return self.r

class FeedLatency(LatencyModel):
    """hftbacktest v1 FeedLatency 一族: 下单延迟 = 当时实测的 feed latency × mult + add。
    确定性、无 RNG, 且自带真实的自相关与尖峰 (你的采集指标: 中位数 16~24 ms, 窗口最大值可到 3.4 s)。推荐做主档。"""
    def __init__(self, mult: float = 1.0, add_ms: float = 5.0):
        self.mult, self.add = mult, int(add_ms * 1e6)
    def entry_ns(self, ts, req, eng): return int(eng.last_feed_lat * self.mult) + self.add
    def response_ns(self, ts, eng): return int(eng.last_feed_lat * self.mult) + self.add

class LognormalLatency(LatencyModel):
    """压力测试用。确定性依赖 RNG 按事件顺序消费: 引擎单线程、堆序固定 -> 成立; 别并行化。"""
    def __init__(self, seed: int, median_ms: float = 50.0, sigma: float = 0.5):
        self._rng, self._mu, self._sigma = random.Random(seed), median_ms * 1e6, sigma
    def _draw(self): return int(self._rng.lognormvariate(0, self._sigma) * self._mu)
    def entry_ns(self, ts, req, eng): return self._draw()
    def response_ns(self, ts, eng): return self._draw()