from src.replay_engine.queue_models import HeadOfQueue, ProbQueue, RiskAverse, ThroughOnly
from .helpers import A, D, T, run, fills

SUB = [(0, "submit", ("b", A, "B", 500, 1000))]

def tape_queue():   # 同价成交: 先喂队列, 溢出才是我的; 随后的 delta 减量已由成交解释 -> 不重复推进
    return [D(0,"B",500,3000), D(0,"A",510,3000), T(10,500,2000,"A"), D(11,"B",500,1000), T(20,500,1500,"A"), D(21,"B",500,0)]

def test_prob_queue_no_double_count():
    assert fills(run(tape_queue(), SUB, ProbQueue(1.0))) == [(20, 1, 500, 500)]
def test_head_of_queue():
    assert fills(run(tape_queue(), SUB, HeadOfQueue())) == [(10, 1, 500, 1000)]
def test_through_only_ignores_same_price():
    assert fills(run(tape_queue(), SUB, ThroughOnly())) == []

def tape_cancel():  # 无成交减量: pro-rata (front 4000 -> 4000*3000/6000 = 2000) vs 风险厌恶 (front = min(4000, 3000) = 3000)
    return [D(0,"B",500,4000), D(0,"A",510,1000), D(5,"B",500,6000), D(10,"B",500,3000), T(20,500,2500,"A")]

def test_pro_rata_vs_risk_averse():
    assert fills(run(tape_cancel(), SUB, ProbQueue(1.0))) == [(20, 1, 500, 500)]
    assert fills(run(tape_cancel(), SUB, RiskAverse())) == []

def test_through_and_cross_fill_every_model():
    for qm in (HeadOfQueue(), ProbQueue(2.0), RiskAverse(), ThroughOnly()):
        assert fills(run([D(0,"B",500,1000), D(0,"A",510,1000), T(10,490,100,"A")], SUB, qm)) == [(10, 1, 500, 1000)]  # 穿透
        assert fills(run([D(0,"B",500,1000), D(0,"A",510,1000), D(10,"A",500,200)], SUB, qm)) == [(10, 1, 500, 1000)]  # 触价

def test_take_walks_levels_without_touching_book():
    tape = [D(0,"B",490,100), D(0,"A",510,300), D(0,"A",511,500), D(1,"B",490,100)]
    s = run(tape, [(0, "submit", ("b", A, "B", 511, 600))], ProbQueue())
    assert fills(s) == [(0, 1, 510, 300), (0, 1, 511, 300)]
    b = s.e.sim.book(A)
    assert (b.qty("A", 510), b.qty("A", 511)) == (300, 500)      # 影子单不改簿

def test_dominance_chain():
    """固定订单流 (开环) 下逐时刻累计成交量: Head >= Prob(n) >= RiskAverse >= ThroughOnly。闭环策略下 PnL 排序不保证。"""
    tape = tape_cancel() + [D(21,"B",500,500), T(30,500,400,"A"), D(31,"B",500,100), T(40,500,3000,"A")]
    cum = {}
    for name, qm in (("head", HeadOfQueue()), ("p1", ProbQueue(1.0)), ("p3", ProbQueue(3.0)), ("ra", RiskAverse()), ("to", ThroughOnly())):
        tot, series = 0, {}
        for ts, _, _, q in fills(run(tape, SUB, qm)):
            tot += q; series[ts] = tot
        cum[name] = series
    at = lambda s, t: max([v for k, v in s.items() if k <= t], default=0)
    for t in (10, 20, 30, 40, 50):
        assert at(cum["head"], t) >= at(cum["p1"], t) >= at(cum["ra"], t) >= at(cum["to"], t)
        assert at(cum["head"], t) >= at(cum["p3"], t) >= at(cum["ra"], t)