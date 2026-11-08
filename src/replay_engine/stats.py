"""持仓/现金/手续费簿记 + 采样记录 (= hftbacktest state.rs + recorder.rs)。"""
from __future__ import annotations
from .events import TICK, QTY_SCALE

class FeeModel:
    def fee(self, notional: float, maker: bool) -> float: raise NotImplementedError
class ZeroFee(FeeModel):
    def fee(self, notional, maker): return 0.0
class BpsFee(FeeModel):                       # Polymarket 部分市场有 taker fee (trade 报文里的 fee_rate_bps)
    def __init__(self, maker_bps: float = 0.0, taker_bps: float = 0.0): self.m, self.t = maker_bps, taker_bps
    def fee(self, notional, maker): return notional * (self.m if maker else self.t) / 1e4

class State:
    def __init__(self, fee: FeeModel):
        self.fee = fee
        self.position = 0            # 内部单位 (0.01 股)
        self.cash = self.fees = 0.0  # USDC
        self.n_fills = self.volume = 0
        self.fills: list[tuple] = []
    def on_fill(self, side: str, px: int, qty: int, maker: bool, ts: int) -> None:
        notional = px / TICK * qty / QTY_SCALE
        self.position += qty if side == "B" else -qty
        self.cash += -notional if side == "B" else notional
        f = self.fee.fee(notional, maker); self.fees += f; self.cash -= f
        self.n_fills += 1; self.volume += qty
        self.fills.append((ts, side, px, qty, maker))
    def equity(self, mid_px: float) -> float:
        return self.cash + self.position / QTY_SCALE * mid_px / TICK

class Recorder:
    def __init__(self, every_ns: int = 1_000_000_000): self.every, self.last, self.rows = every_ns, -1, []
    def maybe(self, ts: int, book, state: State) -> None:
        if ts - self.last < self.every or not book.has_tob(): return
        self.last = ts; mid = book.mid()
        self.rows.append((ts, mid / TICK, state.position / QTY_SCALE, state.cash, state.equity(mid)))
    def frame(self):
        import pandas as pd
        return pd.DataFrame(self.rows, columns=["ts", "mid", "position", "cash", "equity"])