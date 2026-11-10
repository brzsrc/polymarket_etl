import hashlib, random
from src.replay_engine.engine import Engine
from src.replay_engine.events import Delta, Trade
from src.replay_engine.latency import LognormalLatency
from src.replay_engine.queue_models import ProbQueue
from src.replay_engine.strategy import NaiveQuoter

def synthetic_tape(seed: int, n: int = 3000):
    r, t, bid, ask = random.Random(seed), 0, 500, 503
    mk = lambda t, lat, side, px, qty: Delta(asset_id="A", exch_ts=t, local_ts=t + lat, side=side, px=px, qty=qty, best_bid=bid, best_ask=ask, eom=False)
    evs = [mk(0, 20_000_000, "B", bid, 5000), mk(0, 20_000_000, "A", ask, 5000)]
    for _ in range(n):
        t += r.randint(1, 50) * 1_000_000; lat = r.randint(14, 40) * 1_000_000; side = r.choice("BA")
        if r.random() < 0.6:
            px = bid - r.randint(0, 3) if side == "B" else ask + r.randint(0, 3)
            evs.append(mk(t, lat, side, px, r.randint(0, 8000)))
        else:
            evs.append(Trade(asset_id="A", exch_ts=t, local_ts=t + lat, px=ask if side == "B" else bid, qty=r.randint(100, 4000), aggressor=side))
    evs.sort(key=lambda e: e.local_ts)          # replay() 的输出顺序: local_ts, 同 local_ts 保持到达序
    return evs

def run_hash(seed_tape=1, seed_lat=7):
    fl = []
    class Rec(NaiveQuoter):
        def on_report(self, rep, o, ts):
            if rep.kind == "fill": fl.append((ts, rep.oid, rep.px, rep.fill_qty))
    Engine(iter(synthetic_tape(seed_tape)), Rec("A", qty=10, max_pos=50), LognormalLatency(seed_lat), ProbQueue(1.0)).run()
    return hashlib.sha256(repr(fl).encode()).hexdigest(), len(fl)

def test_deterministic():
    (h1, n1), (h2, n2) = run_hash(), run_hash()
    assert h1 == h2 and n1 > 0
def test_latency_seed_matters():
    assert run_hash(seed_lat=7)[0] != run_hash(seed_lat=8)[0]