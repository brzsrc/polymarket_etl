"""
rule_engine.py - Polymarket 实时流式异常检测规则引擎.

设计目标:
    1. 低延迟: WS 事件 → 规则评估 → 告警, p99 < 5ms
    2. 可测试: 录制/回放, 规则行为可复现
    3. 可扩展: 新规则继承基类, 不改引擎核心
    4. 可观测: 内置延迟统计, alert 速率, 状态指标

文件分层:
    数据结构层  → SlidingWindow
    状态层      → MarketState
    规则层      → DetectionRule + 4 个具体规则
    引擎层      → RuleEngine
    输出层      → AlertSink, AlertCorrelator
    观测层      → LatencyTracker
    驱动层      → live / record / replay 三种模式

启动方式:
    python rule_engine.py --live                       # 实时接 WS
    python rule_engine.py --record events.jsonl 300    # 录 5 分钟数据
    python rule_engine.py --replay events.jsonl        # 回放检测
    python rule_engine.py --replay events.jsonl --rules aggressive  # 换规则集

依赖: pip install websockets httpx
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import signal
import sys
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Iterator
import httpx
import websockets


# ============================================================
# 时间工具 - 全部用 ns. 单一时间源, 避免 time.time() / monotonic 混用.
# ============================================================

def now_ns() -> int:
    return time.time_ns()


def fmt_ts(ns: int) -> str:
    return time.strftime("%H:%M:%S", time.localtime(ns / 1e9))


# ============================================================
# 数据结构层: SlidingWindow
# ============================================================

@dataclass
class WindowSample:
    """一个时间序列样本"""
    ts_ns: int
    value: float


class SlidingWindow:
    """基于时间的滑动窗口.

    选型说明 (面试时讲):
        - 用 collections.deque 而不是 list, 因为 popleft() 是 O(1), 而 list 是 O(n)
        - deque 是 C 实现, 对 demo 量级 (< 10K 样本) 性能完全够
        - 极致场景 (HFT, 单机百万 ops/s) 才需要自己写 ring buffer + numpy.
          但那种场景一般是 C++/Rust 不是 Python.

    时序保证:
        evict() 严格按 ts_ns 单调递增. 输入的 ts_ns 必须递增, 否则会被显式拒绝
        (打 "out_of_order" 计数), 而不是悄悄破坏统计.

    支持的查询:
        - len() / size: 当前样本数
        - mean / stddev: 增量计算? 不, demo 用直接遍历 (O(n))
          想优化可以用 Welford 在线算法, 但增加复杂度, demo 不值得.
        - max / min / first / last
    """

    def __init__(self, window_ms: int, max_samples: int = 10000):
        self.window_ns = window_ms * 1_000_000
        self.max_samples = max_samples
        self._buf: deque[WindowSample] = deque()
        self.out_of_order_count = 0
        self.last_ts_ns: int = -1

    def push(self, ts_ns: int, value: float) -> bool:
        """压入新样本. 返回 True=成功, False=被拒(乱序)."""
        if ts_ns < self.last_ts_ns:
            self.out_of_order_count += 1
            return False
        self.last_ts_ns = ts_ns
        self._buf.append(WindowSample(ts_ns, value))
        self._evict(ts_ns)
        # 防止异常情况内存爆炸 (理论上 evict 已经控制, 但加个保险)
        while len(self._buf) > self.max_samples:
            self._buf.popleft()
        return True

    def _evict(self, now: int):
        """淘汰窗口外的旧样本"""
        cutoff = now - self.window_ns
        while self._buf and self._buf[0].ts_ns < cutoff:
            self._buf.popleft()

    def __len__(self) -> int:
        return len(self._buf)

    def values(self) -> list[float]:
        return [s.value for s in self._buf]

    def first(self) -> WindowSample | None:
        return self._buf[0] if self._buf else None

    def last(self) -> WindowSample | None:
        return self._buf[-1] if self._buf else None

    def mean(self) -> float | None:
        if not self._buf:
            return None
        return sum(s.value for s in self._buf) / len(self._buf)

    def stddev(self) -> float | None:
        """样本标准差. 至少 2 个样本才有定义."""
        n = len(self._buf)
        if n < 2:
            return None
        m = self.mean()
        var = sum((s.value - m) ** 2 for s in self._buf) / (n - 1)
        return math.sqrt(var)


# ============================================================
# 状态层: MarketState (per-token)
# ============================================================

@dataclass
class MarketState:
    """单个 token 的最新状态 + 滑动窗口"""
    asset_id: str
    label: str = "?"     # 人类可读 (YES/NO)

    best_bid: float | None = None
    best_ask: float | None = None
    last_trade_price: float | None = None
    last_update_ns: int = 0

    # 滑动窗口: 跟踪 best_ask 在最近 30s 的变化
    ask_window: SlidingWindow = field(default_factory=lambda: SlidingWindow(window_ms=30_000))
    # spread 历史: 最近 5 分钟
    spread_window: SlidingWindow = field(default_factory=lambda: SlidingWindow(window_ms=300_000))

    @property
    def spread(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid

    @property
    def midpoint(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2

    def update(self, ts_ns: int, bid: float | None, ask: float | None):
        """从 WS 事件更新状态, 同时维护窗口"""
        if bid is not None:
            self.best_bid = bid
        if ask is not None:
            self.best_ask = ask
        self.last_update_ns = ts_ns
        if self.best_ask is not None:
            self.ask_window.push(ts_ns, self.best_ask)
        if self.spread is not None:
            self.spread_window.push(ts_ns, self.spread)


@dataclass
class CryptoState:
    """单个加密货币标的的最新现货价 (来自 RTDS)"""
    symbol: str
    last_price: float | None = None
    last_update_ns: int = 0
    # 30s 价格窗口
    price_window: SlidingWindow = field(default_factory=lambda: SlidingWindow(window_ms=30_000))

    def update(self, ts_ns: int, price: float):
        self.last_price = price
        self.last_update_ns = ts_ns
        self.price_window.push(ts_ns, price)


# ============================================================
# 告警
# ============================================================

@dataclass
class Alert:
    ts_ns: int
    rule_name: str
    severity: str          # "info" | "warning" | "critical"
    asset_id: str
    summary: str
    details: dict          # 触发时的具体数值, 用于事后归因


# ============================================================
# 规则层
# ============================================================

class DetectionRule(ABC):
    """规则基类. 每个具体规则实现 evaluate()."""

    name: str = "abstract"

    @abstractmethod
    def evaluate(
        self,
        state: MarketState,
        cryptos: dict[str, CryptoState],
        event_ts_ns: int,
    ) -> Alert | None:
        ...


class PriceJumpRule(DetectionRule):
    """价格跳跃: 在窗口内, best_ask 变化超过阈值.

    用途: 检测短时间内价格剧烈波动 (信息事件 / 大单冲击).

    阈值设计 (面试可讲的踩坑):
        最初用纯百分比阈值, 实测发现低价 token (~0.01) 上一格变化就是 90%+ 误报.
        改用 "百分比 AND 绝对值" 双条件: 必须同时超过两个阈值才触发.
        e.g. 5% 变化 + 至少 0.005 绝对值: 在 0.5 价位 → 0.025 移动才报,
             在 0.01 价位 → 不会因为 1 tick 跳到 0.011 就报 (10% 但绝对值 0.001).

    参数:
        window_ms: 比较多长时间内
        threshold_pct: 百分比阈值 (e.g. 0.05 = 5%)
        threshold_abs: 绝对值阈值 (e.g. 0.005 = 0.5 cents)
        min_samples: 窗口内至少 N 个样本才检测
    """
    name = "price_jump"

    def __init__(
        self,
        window_ms: int = 5000,
        threshold_pct: float = 0.05,
        threshold_abs: float = 0.005,
        min_samples: int = 5,
    ):
        self.window_ms = window_ms
        self.threshold_pct = threshold_pct
        self.threshold_abs = threshold_abs
        self.min_samples = min_samples

    def evaluate(self, state, cryptos, event_ts_ns):
        w = state.ask_window
        if len(w) < self.min_samples:
            return None
        first = w.first()
        last = w.last()
        if not first or not last or first.value == 0:
            return None
        abs_change = last.value - first.value
        pct_change = abs_change / first.value
        # 双条件: 必须同时超过百分比和绝对值阈值
        if abs(pct_change) < self.threshold_pct or abs(abs_change) < self.threshold_abs:
            return None
        direction = "↑" if pct_change > 0 else "↓"
        return Alert(
            ts_ns=event_ts_ns,
            rule_name=self.name,
            severity="warning" if abs(pct_change) < 0.1 else "critical",
            asset_id=state.asset_id,
            summary=(f"{state.label} ask {direction} {abs(pct_change):.1%} "
                     f"({first.value:.4f} → {last.value:.4f}) "
                     f"in {(last.ts_ns - first.ts_ns)/1e6:.0f}ms"),
            details={
                "from": first.value,
                "to": last.value,
                "pct_change": pct_change,
                "abs_change": abs_change,
                "samples": len(w),
            },
        )


class ZScoreAnomalyRule(DetectionRule):
    """自适应阈值: 用 z-score 检测异常价格变化.

    跟 PriceJumpRule 的区别 (面试可讲):
        - PriceJumpRule 用绝对阈值 (5%), 在波动大的市场会一直误报
        - ZScoreAnomalyRule 用统计阈值 (3σ), 自动适应市场波动度
        - 缺点: 需要足够样本 (min_samples) 才有统计意义, 冷启动期不工作
        - 实战中两个互补: 极端事件用绝对阈值, 渐进异常用 z-score
    """
    name = "zscore_anomaly"

    def __init__(self, sigma_threshold: float = 3.0, min_samples: int = 30):
        self.sigma_threshold = sigma_threshold
        self.min_samples = min_samples

    def evaluate(self, state, cryptos, event_ts_ns):
        w = state.ask_window
        if len(w) < self.min_samples:
            return None
        last = w.last()
        if not last:
            return None
        mean = w.mean()
        sd = w.stddev()
        if mean is None or sd is None or sd == 0:
            return None
        z = (last.value - mean) / sd
        if abs(z) >= self.sigma_threshold:
            return Alert(
                ts_ns=event_ts_ns,
                rule_name=self.name,
                severity="warning",
                asset_id=state.asset_id,
                summary=f"{state.label} ask z-score = {z:+.2f}σ (threshold {self.sigma_threshold}σ)",
                details={
                    "current": last.value,
                    "window_mean": mean,
                    "window_stddev": sd,
                    "z_score": z,
                    "samples": len(w),
                },
            )
        return None


class SpreadWideningRule(DetectionRule):
    """spread 突然扩大: 当前 spread / 历史中位数 > N 倍.

    用途: 流动性蒸发预警. 做市商集体撤单时 spread 会暴涨.
    通常发生在重大新闻前夕或 stale-quote 风险时.
    """
    name = "spread_widening"

    def __init__(self, multiplier: float = 3.0, min_samples: int = 20):
        self.multiplier = multiplier
        self.min_samples = min_samples

    def evaluate(self, state, cryptos, event_ts_ns):
        w = state.spread_window
        if len(w) < self.min_samples:
            return None
        current = state.spread
        if current is None:
            return None
        # 用中位数比 mean 更鲁棒 (不受单个异常 spike 影响)
        sorted_vals = sorted(w.values())
        median = sorted_vals[len(sorted_vals) // 2]
        if median <= 0:
            return None
        ratio = current / median
        if ratio >= self.multiplier:
            return Alert(
                ts_ns=event_ts_ns,
                rule_name=self.name,
                severity="warning",
                asset_id=state.asset_id,
                summary=f"{state.label} spread widened to {ratio:.1f}x median",
                details={
                    "current_spread": current,
                    "median_spread": median,
                    "ratio": ratio,
                    "samples": len(w),
                },
            )
        return None


class CrossSourceLagRule(DetectionRule):
    """跨源滞后: 加密现货价大幅波动, 但 Polymarket 上对应市场报价没动.

    场景: Polymarket 有"BTC 到 X 价吗"这类市场, 价格高度依赖 BTC 现货.
    现货跳了 1% 但 Polymarket 还没反应 → 旧报价被 picked off 的瞬间.

    这条规则展示 cross-source signal 的重要性.

    参数:
        symbol: 监控的加密货币 (e.g. "btcusdt")
        crypto_pct: 现货变化阈值
        market_max_pct: 市场报价变化必须小于此值 (即"没怎么动")
        within_ms: 在多长时间内观察现货变化
    """
    name = "cross_source_lag"

    def __init__(
        self,
        symbol: str = "btcusdt",
        crypto_pct: float = 0.01,
        market_max_pct: float = 0.005,
        within_ms: int = 30_000,
        min_samples: int = 5,
    ):
        self.symbol = symbol
        self.crypto_pct = crypto_pct
        self.market_max_pct = market_max_pct
        self.within_ms = within_ms
        self.min_samples = min_samples

    def evaluate(self, state, cryptos, event_ts_ns):
        crypto = cryptos.get(self.symbol)
        if not crypto or len(crypto.price_window) < self.min_samples:
            return None
        cw = crypto.price_window
        first_c, last_c = cw.first(), cw.last()
        if not first_c or not last_c or first_c.value == 0:
            return None
        crypto_change = (last_c.value - first_c.value) / first_c.value
        if abs(crypto_change) < self.crypto_pct:
            return None

        # 现货动了, 看市场报价动没动
        mw = state.ask_window
        if len(mw) < 2:
            return None
        first_m, last_m = mw.first(), mw.last()
        if not first_m or not last_m or first_m.value == 0:
            return None
        market_change = (last_m.value - first_m.value) / first_m.value

        if abs(market_change) < self.market_max_pct:
            return Alert(
                ts_ns=event_ts_ns,
                rule_name=self.name,
                severity="critical",
                asset_id=state.asset_id,
                summary=(f"{self.symbol} moved {crypto_change:+.2%} "
                         f"but {state.label} ask only {market_change:+.2%} "
                         f"(stale quote risk)"),
                details={
                    "symbol": self.symbol,
                    "crypto_change_pct": crypto_change,
                    "market_change_pct": market_change,
                    "crypto_first": first_c.value,
                    "crypto_last": last_c.value,
                },
            )
        return None


# ============================================================
# Alert correlation: 去重相邻告警
# ============================================================

class AlertCorrelator:
    """相同 (rule, asset) 在 cooldown 内只发一次. 避免 alert storm.

    实际生产更复杂 (alert grouping, severity 升级, downstream pipe), 这里做最小实现.
    """

    def __init__(self, cooldown_ms: int = 5_000):
        self.cooldown_ns = cooldown_ms * 1_000_000
        self._last_fired: dict[tuple[str, str], int] = {}
        self.suppressed = 0

    def should_fire(self, alert: Alert) -> bool:
        key = (alert.rule_name, alert.asset_id)
        last = self._last_fired.get(key, 0)
        if alert.ts_ns - last < self.cooldown_ns:
            self.suppressed += 1
            return False
        self._last_fired[key] = alert.ts_ns
        return True


# ============================================================
# 延迟监控
# ============================================================

class LatencyTracker:
    """记录每个事件从 enqueue → 规则评估完成的耗时, 算 p50/p99."""

    def __init__(self, max_samples: int = 10_000):
        self._samples: deque[float] = deque(maxlen=max_samples)

    def record(self, ms: float):
        self._samples.append(ms)

    def percentiles(self) -> dict[str, float]:
        if not self._samples:
            return {"p50": 0, "p99": 0, "max": 0, "count": 0}
        s = sorted(self._samples)
        n = len(s)
        return {
            "p50": s[n // 2],
            "p99": s[min(n - 1, int(n * 0.99))],
            "max": s[-1],
            "count": n,
        }


# ============================================================
# 输出层
# ============================================================

class AlertSink:
    """终端打印告警 (生产里换成 Kafka / Slack / 仪表板)"""

    SEVERITY_PREFIX = {"info": "ℹ️ ", "warning": "⚠️ ", "critical": "🚨"}

    def emit(self, alert: Alert):
        ts = fmt_ts(alert.ts_ns)
        sev = self.SEVERITY_PREFIX.get(alert.severity, "  ")
        print(f"[{ts}] {sev} [{alert.rule_name:18}] {alert.summary}")


# ============================================================
# 引擎层: 协调状态 + 规则 + 输出
# ============================================================

@dataclass
class RuleSet:
    """命名的规则集合, 方便 A/B 测试"""
    name: str
    rules: list[DetectionRule]


def default_ruleset() -> RuleSet:
    return RuleSet(name="default", rules=[
        PriceJumpRule(window_ms=5_000, threshold_pct=0.05, min_samples=5),
        ZScoreAnomalyRule(sigma_threshold=3.0, min_samples=30),
        SpreadWideningRule(multiplier=3.0, min_samples=20),
        CrossSourceLagRule(symbol="btcusdt", crypto_pct=0.01,
                           market_max_pct=0.005, within_ms=30_000),
    ])


def aggressive_ruleset() -> RuleSet:
    """更敏感的规则集. 用 --rules aggressive 切换, 演示 A/B 测试能力."""
    return RuleSet(name="aggressive", rules=[
        PriceJumpRule(window_ms=2_000, threshold_pct=0.02, min_samples=3),
        ZScoreAnomalyRule(sigma_threshold=2.0, min_samples=15),
        SpreadWideningRule(multiplier=2.0, min_samples=10),
    ])


class RuleEngine:
    def __init__(self, ruleset: RuleSet, sink: AlertSink, correlator: AlertCorrelator):
        self.ruleset = ruleset
        self.sink = sink
        self.correlator = correlator
        self.tracker = LatencyTracker()

        self.markets: dict[str, MarketState] = {}     # asset_id → state
        self.cryptos: dict[str, CryptoState] = {}     # symbol → state
        self.events_processed = 0
        self.alerts_fired = 0

    def register_market(self, asset_id: str, label: str):
        self.markets[asset_id] = MarketState(asset_id=asset_id, label=label)

    def process_event(self, event: dict, recv_ts_ns: int):
        """主入口. event 形如:
            {"source": "market", "type": "book"|"price_change"|...,  "data": {...}}
            {"source": "rtds",   "type": "crypto_prices",            "data": {...}}
        """
        t0 = time.perf_counter()
        try:
            source = event.get("source")
            if source == "market":
                self._handle_market(event, recv_ts_ns)
            elif source == "rtds":
                self._handle_rtds(event, recv_ts_ns)
        finally:
            self.events_processed += 1
            self.tracker.record((time.perf_counter() - t0) * 1000)

    def _handle_market(self, event: dict, recv_ts_ns: int):
        et = event.get("type")
        data = event.get("data", {})

        if et == "book":
            asset_id = data.get("asset_id", "")
            if asset_id not in self.markets:
                return
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            bid = float(bids[-1]["price"]) if bids else None
            ask = float(asks[0]["price"]) if asks else None
            self.markets[asset_id].update(recv_ts_ns, bid, ask)
            self._evaluate_rules(asset_id, recv_ts_ns)

        elif et == "price_change":
            # data 顶层有 market, 数组在 price_changes
            for change in data.get("price_changes", []):
                asset_id = change.get("asset_id", "")
                if asset_id not in self.markets:
                    continue
                bb = change.get("best_bid")
                ba = change.get("best_ask")
                bid = float(bb) if bb is not None else None
                ask = float(ba) if ba is not None else None
                self.markets[asset_id].update(recv_ts_ns, bid, ask)
                self._evaluate_rules(asset_id, recv_ts_ns)

        elif et == "last_trade_price":
            asset_id = data.get("asset_id", "")
            if asset_id in self.markets:
                price = data.get("price")
                if price is not None:
                    self.markets[asset_id].last_trade_price = float(price)

    def _handle_rtds(self, event: dict, recv_ts_ns: int):
        data = event.get("data", {})
        # RTDS 消息结构: {"topic": ..., "payload": {"symbol": ..., "value": ...}}
        topic = data.get("topic", "")
        if "crypto_prices" not in topic:
            return
        payload = data.get("payload", {})
        symbol = payload.get("symbol", "")
        value = payload.get("value")
        if not symbol or value is None:
            return
        if symbol not in self.cryptos:
            self.cryptos[symbol] = CryptoState(symbol=symbol)
        self.cryptos[symbol].update(recv_ts_ns, float(value))
        # crypto 更新触发所有市场重新评估 cross-source 规则
        for asset_id in self.markets:
            self._evaluate_rules(asset_id, recv_ts_ns, only_cross_source=True)

    def _evaluate_rules(self, asset_id: str, event_ts_ns: int, only_cross_source: bool = False):
        state = self.markets[asset_id]
        for rule in self.ruleset.rules:
            if only_cross_source and not isinstance(rule, CrossSourceLagRule):
                continue
            try:
                alert = rule.evaluate(state, self.cryptos, event_ts_ns)
            except Exception as e:
                print(f"[rule {rule.name}] error: {e}")
                continue
            if alert and self.correlator.should_fire(alert):
                self.alerts_fired += 1
                self.sink.emit(alert)

    def stats(self) -> dict:
        return {
            "events_processed": self.events_processed,
            "alerts_fired": self.alerts_fired,
            "alerts_suppressed": self.correlator.suppressed,
            "latency": self.tracker.percentiles(),
            "out_of_order_total": sum(
                m.ask_window.out_of_order_count + m.spread_window.out_of_order_count
                for m in self.markets.values()
            ),
        }


# ============================================================
# 录制 / 回放
# ============================================================

def write_event(f, event: dict, recv_ts_ns: int):
    f.write(json.dumps({"recv_ts_ns": recv_ts_ns, "event": event}) + "\n")
    f.flush()


def read_events(path: str) -> Iterator[tuple[int, dict]]:
    """读事件行 (跳过 meta). 每行 jsonl, 含 recv_ts_ns + event 的就是事件."""
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "recv_ts_ns" in obj and "event" in obj:
                yield obj["recv_ts_ns"], obj["event"]


# ============================================================
# WS 客户端 (从 polymarket_ws.py 简化复用)
# ============================================================

MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
RTDS_WS_URL = "wss://ws-live-data.polymarket.com"


async def market_loop(asset_ids: list[str], queue: asyncio.Queue):
    while True:
        try:
            async with websockets.connect(MARKET_WS_URL, ping_interval=None) as ws:
                await ws.send(json.dumps({"assets_ids": asset_ids, "type": "market"}))

                async def ping():
                    while True:
                        await asyncio.sleep(10)
                        await ws.send("PING")

                pt = asyncio.create_task(ping())
                try:
                    async for raw in ws:
                        if raw == "PONG":
                            continue
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        events = msg if isinstance(msg, list) else [msg]
                        for ev in events:
                            await queue.put({
                                "source": "market",
                                "type": ev.get("event_type", "?"),
                                "data": ev,
                            })
                finally:
                    pt.cancel()
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[market_ws] disconnected: {e}, reconnect in 2s")
            await asyncio.sleep(2)


async def rtds_loop(queue: asyncio.Queue):
    while True:
        try:
            async with websockets.connect(RTDS_WS_URL, ping_interval=None) as ws:
                await ws.send(json.dumps({
                    "action": "subscribe",
                    "subscriptions": [{"topic": "crypto_prices", "type": "*", "filters": ""}],
                }))

                async def ping():
                    while True:
                        await asyncio.sleep(5)
                        await ws.send("PING")

                pt = asyncio.create_task(ping())
                try:
                    async for raw in ws:
                        if raw == "PONG":
                            continue
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            # RTDS 偶发空消息 / 非 JSON, 跳过即可, 不要断开
                            continue
                        await queue.put({
                            "source": "rtds",
                            "type": "crypto_prices",
                            "data": msg,
                        })
                finally:
                    pt.cancel()
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[rtds_ws] disconnected: {e}, reconnect in 2s")
            await asyncio.sleep(2)


async def find_active_market() -> tuple[list[str], str]:
    async with httpx.AsyncClient(headers={"User-Agent": "rule-engine/0.1"}, timeout=15) as c:
        r = await c.get(
            "https://gamma-api.polymarket.com/markets",
            params={"limit": 30, "active": "true", "closed": "false",
                    "order": "volume24hr", "ascending": "false"},
        )
        r.raise_for_status()
        for m in r.json():
            bid, ask = m.get("bestBid"), m.get("bestAsk")
            if bid and ask and 0.05 < float(bid) < 0.95:
                tokens = json.loads(m["clobTokenIds"])
                return tokens, m["question"]
    raise RuntimeError("no suitable market")


# ============================================================
# 三种运行模式
# ============================================================

async def run_live(ruleset: RuleSet):
    asset_ids, market_name = await find_active_market()
    print(f"📡 LIVE  market: {market_name}")
    print(f"   YES: {asset_ids[0][:20]}...")
    print(f"   NO:  {asset_ids[1][:20]}...")
    print(f"   ruleset: {ruleset.name}\n")

    engine = RuleEngine(ruleset, AlertSink(), AlertCorrelator())
    engine.register_market(asset_ids[0], "YES")
    engine.register_market(asset_ids[1], "NO")

    queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)
    tasks = [
        asyncio.create_task(market_loop(asset_ids, queue)),
        asyncio.create_task(rtds_loop(queue)),
    ]

    async def stats_loop():
        while True:
            await asyncio.sleep(30)
            s = engine.stats()
            lat = s["latency"]
            print(f"\n--- stats: events={s['events_processed']}, "
                  f"alerts={s['alerts_fired']} (suppressed={s['alerts_suppressed']}), "
                  f"latency p50={lat['p50']:.2f}ms p99={lat['p99']:.2f}ms ---\n")
    tasks.append(asyncio.create_task(stats_loop()))

    # signal 处理
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: [t.cancel() for t in tasks])
        except NotImplementedError:
            pass

    try:
        while True:
            event = await queue.get()
            engine.process_event(event, now_ns())
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        for t in tasks:
            t.cancel()
        print(f"\nfinal: {engine.stats()}")


async def run_record(out_path: str, duration_sec: int):
    asset_ids, market_name = await find_active_market()
    print(f"⏺  RECORD market: {market_name}")
    print(f"   duration: {duration_sec}s, output: {out_path}\n")

    queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)
    tasks = [
        asyncio.create_task(market_loop(asset_ids, queue)),
        asyncio.create_task(rtds_loop(queue)),
    ]
    # 把 token 信息写到第一行作为元数据
    with open(out_path, "w") as f:
        meta = {"meta": {"market": market_name, "asset_ids": asset_ids,
                          "started_ns": now_ns()}}
        f.write(json.dumps(meta) + "\n")

        end = time.time() + duration_sec
        n = 0
        try:
            while time.time() < end:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=1)
                except asyncio.TimeoutError:
                    continue
                write_event(f, event, now_ns())
                n += 1
                if n % 50 == 0:
                    print(f"  recorded {n} events ({end - time.time():.0f}s left)")
        finally:
            for t in tasks:
                t.cancel()

    print(f"\n✅ recorded {n} events to {out_path}")


def run_replay(in_path: str, ruleset: RuleSet, speed: float = 0.0):
    """回放: speed=0 表示尽快跑完, speed=1.0 表示按原速复现 (用于演示)"""
    print(f"⏯  REPLAY {in_path}")
    print(f"   ruleset: {ruleset.name}")
    print(f"   speed: {'as-fast-as-possible' if speed == 0 else f'{speed}x'}\n")

    engine = RuleEngine(ruleset, AlertSink(), AlertCorrelator())

    # 读元数据 (第一行)
    with open(in_path) as f:
        meta_line = f.readline().strip()
    meta = json.loads(meta_line).get("meta", {})
    asset_ids = meta.get("asset_ids", [])
    if asset_ids:
        engine.register_market(asset_ids[0], "YES")
    if len(asset_ids) > 1:
        engine.register_market(asset_ids[1], "NO")

    # 跳过元数据行后读事件
    events = list(read_events(in_path))
    if not events:
        print("⚠️  no events in file")
        return
    # 第一行是 meta, 不是真事件 — read_events 会因为没 'recv_ts_ns' 报错, 所以 skip
    events = [(ts, ev) for ts, ev in events if ev]  # 已经 ok

    t0_real = time.time()
    t0_recorded = events[0][0]

    for recv_ts_ns, event in events:
        if speed > 0:
            elapsed_recorded = (recv_ts_ns - t0_recorded) / 1e9
            elapsed_real = time.time() - t0_real
            wait = elapsed_recorded / speed - elapsed_real
            if wait > 0:
                time.sleep(wait)
        engine.process_event(event, recv_ts_ns)

    print(f"\n--- replay done ---")
    print(json.dumps(engine.stats(), indent=2))


# ============================================================
# CLI
# ============================================================

def parse_args():
    p = argparse.ArgumentParser(description="Polymarket streaming rule engine")
    sub = p.add_subparsers(dest="mode", required=True)

    p_live = sub.add_parser("live", help="real-time WS + rule eval")
    p_live.add_argument("--rules", choices=["default", "aggressive"], default="default")

    p_rec = sub.add_parser("record", help="record WS data to file")
    p_rec.add_argument("output")
    p_rec.add_argument("duration", type=int, help="seconds")

    p_rep = sub.add_parser("replay", help="replay recorded data through rules")
    p_rep.add_argument("input")
    p_rep.add_argument("--rules", choices=["default", "aggressive"], default="default")
    p_rep.add_argument("--speed", type=float, default=0.0,
                       help="0 = max speed; 1.0 = real-time")

    return p.parse_args()


def get_ruleset(name: str) -> RuleSet:
    return aggressive_ruleset() if name == "aggressive" else default_ruleset()


def main():
    args = parse_args()
    if args.mode == "live":
        asyncio.run(run_live(get_ruleset(args.rules)))
    elif args.mode == "record":
        asyncio.run(run_record(args.output, args.duration))
    elif args.mode == "replay":
        run_replay(args.input, get_ruleset(args.rules), speed=args.speed)


if __name__ == "__main__":
    main()
