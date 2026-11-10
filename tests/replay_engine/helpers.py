from src.replay_engine.engine import Engine
from src.replay_engine.events import TICK, Delta, Trade
from src.replay_engine.latency import ConstantLatency
from src.replay_engine.strategy import Scripted

A = "A"
def D(t, side, px, qty, lat=0):
    return Delta(asset_id=A, exch_ts=t, local_ts=t + lat, side=side, px=px, qty=qty, best_bid=0, best_ask=TICK, eom=False)
def T(t, px, qty, aggressor, lat=0):
    return Trade(asset_id=A, exch_ts=t, local_ts=t + lat, px=px, qty=qty, aggressor=aggressor)
def run(tape, script, qm, lat=None):
    s = Scripted(script)
    Engine(iter(tape), s, lat or ConstantLatency(0, 0), qm).run()
    return s
def fills(s):
    return [(ts, oid, px, q) for ts, k, oid, px, q in s.reports if k == "fill"]