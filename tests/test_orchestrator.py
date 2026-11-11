"""
Orchestrator discovery-cycle tests with a fake Gamma, pool and WAL (no network).

Run from the repo root:  .venv/bin/python -m pytest tests/test_orchestrator.py -q -p no:anyio
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

import src.orchestrator as orchestrator
from src import config
from src.market.gamma_client import GammaError, MarketInfo
from src.orchestrator import Orchestrator
from src.ws.connection import ConnectionEvent


def mk(mid: str, volume: float = 100.0, closed: bool = False, accepting: bool = True) -> MarketInfo:
    raw = {
        "id": mid,
        "conditionId": f"0x{mid}",
        "clobTokenIds": json.dumps([f"{mid}-yes", f"{mid}-no"]),
        "volume24hr": volume,
        "closed": closed,
        "acceptingOrders": accepting,
    }
    return MarketInfo.from_raw(raw)


class FakeGamma:
    """
    Scripted Gamma. Set `top` (list or exception) and `status` (dict id ->
    MarketInfo, or exception) before each cycle. Records every by-id lookup.
    """

    def __init__(self):
        self.top: list[MarketInfo] | Exception = []
        self.status: dict[str, MarketInfo] | Exception = {}
        self.lookups: list[list[str]] = []

    def __call__(self):            # GammaClient() -> client
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return None

    async def top_markets_by_volume24h(self, n=None):
        if isinstance(self.top, Exception):
            raise self.top
        return list(self.top)[:n]

    async def markets_by_ids(self, ids):
        self.lookups.append(list(ids))
        if isinstance(self.status, Exception):
            raise self.status
        return {i: self.status[i] for i in ids if i in self.status}


class FakePool:
    def __init__(self):
        self.tokens: set[str] = set()
        self.calls: list[tuple[str, list[str]]] = []

    async def add_subscriptions(self, ids):
        self.calls.append(("add", list(ids)))
        self.tokens |= set(ids)

    async def remove_subscriptions(self, ids):
        self.calls.append(("remove", list(ids)))
        self.tokens -= set(ids)


class FakeWal:
    def __init__(self):
        self.snapshots: list[tuple[int, list[dict]]] = []
        self.events: list[tuple[int, dict]] = []
        self.metrics: list[tuple[int, dict]] = []
        self.fail_metrics = 0          # fail the next N write_metrics calls

    async def write_market_snapshot(self, ts, records):
        self.snapshots.append((ts, records))

    async def write_event(self, ts, event):
        self.events.append((ts, event))

    async def write_metrics(self, ts, sample):
        if self.fail_metrics:
            self.fail_metrics -= 1
            raise OSError("disk full")
        self.metrics.append((ts, sample))

    def last_records(self) -> dict[str, dict]:
        return {r["market_id"]: r for r in self.snapshots[-1][1]}

    def last(self, action: str) -> dict[str, str | None]:
        """Last cycle's markets with this action -> reason."""
        return {mid: r["sub"]["reason"] for mid, r in self.last_records().items()
                if r["sub"]["action"] == action}


@pytest.fixture
def gamma(monkeypatch):
    g = FakeGamma()
    monkeypatch.setattr(orchestrator, "GammaClient", g)
    return g


def make_orch(tmp_path: Path, **kw) -> tuple[Orchestrator, FakePool, FakeWal]:
    wal = FakeWal()
    orch = Orchestrator(wal=wal, **kw)
    pool = FakePool()
    orch._pool = pool
    return orch, pool, wal


def cycle(orch):
    return asyncio.run(orch.run_one_cycle())


def tokens(*mids: str) -> set[str]:
    return {t for m in mids for t in (f"{m}-yes", f"{m}-no")}


# --- construction ------------------------------------------------------------

def test_n_markets_must_fit_under_cap(tmp_path):
    with pytest.raises(ValueError, match="max_subscribed_markets"):
        Orchestrator(wal=FakeWal(), n_markets=11, max_subscribed_markets=10)
    Orchestrator(wal=FakeWal(), n_markets=10, max_subscribed_markets=10)


def test_default_limits_are_consistent():
    assert config.TOP_N_MARKETS <= config.MAX_SUBSCRIBED_MARKETS == 1000
    # the defaults actually used by Orchestrator() must pass its own check
    Orchestrator(wal=FakeWal())


# --- subscription rules --------------------------------------------------------

def test_first_cycle_subscribes_top_n_and_records_rank(tmp_path, gamma):
    orch, pool, wal = make_orch(tmp_path, n_markets=2, max_subscribed_markets=10)
    gamma.top = [mk("A"), mk("B")]

    assert cycle(orch) is True

    assert pool.tokens == tokens("A", "B")
    assert wal.last("added") == {"A": None, "B": None} and wal.last("removed") == {}
    assert orch.subscribed_markets == {"A", "B"}
    assert orch.current_subscriptions == tokens("A", "B")
    assert gamma.lookups == []                          # nothing to look up yet
    recs = wal.last_records()
    assert recs["A"]["source"] == "top" and recs["A"]["rank"] == 1
    assert recs["B"]["rank"] == 2
    assert recs["A"]["sub"] == {"action": "added", "reason": None, "missing_strikes": 0}
    assert recs["A"]["raw"]["id"] == "A"


def test_dropped_but_still_trading_is_kept(tmp_path, gamma):
    orch, pool, wal = make_orch(tmp_path, n_markets=2, max_subscribed_markets=10)
    gamma.top = [mk("A"), mk("B")]
    cycle(orch)

    gamma.top = [mk("A"), mk("C")]                       # B fell out of the top-N
    gamma.status = {"B": mk("B", volume=5)}
    cycle(orch)

    assert gamma.lookups == [["B"]]
    assert pool.tokens == tokens("A", "B", "C")
    assert not any(kind == "remove" for kind, _ in pool.calls)
    recs = wal.last_records()
    assert recs["B"]["source"] == "lookup" and recs["B"]["rank"] is None
    assert recs["B"]["sub"]["action"] == "kept"
    assert recs["A"]["sub"]["action"] == "kept" and recs["C"]["sub"]["action"] == "added"
    assert orch._subs["B"].info.volume24hr == 5         # volume refreshed from the lookup


@pytest.mark.parametrize("ended", [mk("B", closed=True), mk("B", accepting=False)])
def test_dropped_and_ended_is_removed(tmp_path, gamma, ended):
    orch, pool, wal = make_orch(tmp_path, n_markets=2, max_subscribed_markets=10)
    gamma.top = [mk("A"), mk("B")]
    cycle(orch)

    gamma.top = [mk("A")]
    gamma.status = {"B": ended}
    cycle(orch)

    assert wal.last("removed") == {"B": "ended"}
    kind, ids = pool.calls[-1]
    assert kind == "remove" and set(ids) == tokens("B")
    assert pool.tokens == tokens("A")
    assert orch.subscribed_markets == {"A"}
    rec = wal.last_records()["B"]
    assert rec["sub"]["action"] == "removed" and rec["sub"]["reason"] == "ended"
    assert rec["raw"] is ended.raw


def test_not_found_removed_after_strikes_and_reset_when_found(tmp_path, gamma):
    orch, pool, wal = make_orch(tmp_path, n_markets=2, max_subscribed_markets=10,
                                missing_strikes_to_remove=3)
    gamma.top = [mk("A"), mk("B")]
    cycle(orch)
    gamma.top = [mk("A")]

    gamma.status = {}                                    # B missing: strike 1
    cycle(orch)
    assert wal.last_records()["B"]["sub"] == {"action": "kept", "reason": "not_found",
                                              "missing_strikes": 1}
    gamma.status = {"B": mk("B")}                        # found again: reset
    cycle(orch)
    assert orch._subs["B"].missing_strikes == 0

    gamma.status = {}
    cycle(orch)                                          # strike 1
    cycle(orch)                                          # strike 2
    assert "B" in orch.subscribed_markets
    cycle(orch)                                          # strike 3 -> removed
    assert wal.last("removed") == {"B": "not_found"}
    assert pool.tokens == tokens("A")
    assert wal.last_records()["B"]["sub"]["missing_strikes"] == 3


def test_lookup_failure_adds_but_removes_nothing(tmp_path, gamma):
    orch, pool, wal = make_orch(tmp_path, n_markets=2, max_subscribed_markets=10)
    gamma.top = [mk("A"), mk("B")]
    cycle(orch)

    gamma.top = [mk("C")]
    gamma.status = GammaError("boom")
    assert cycle(orch) is True

    assert wal.last("added") == {"C": None} and wal.last("removed") == {}
    assert pool.tokens == tokens("A", "B", "C")
    recs = wal.last_records()
    assert recs["A"]["sub"] == {"action": "kept", "reason": "lookup_failed", "missing_strikes": 0}
    assert recs["A"]["raw"] is None


def test_top_fetch_failure_changes_nothing(tmp_path, gamma):
    orch, pool, wal = make_orch(tmp_path, n_markets=2, max_subscribed_markets=10)
    gamma.top = [mk("A")]
    cycle(orch)
    calls_before = list(pool.calls)

    gamma.top = GammaError("down")
    assert cycle(orch) is False
    assert pool.calls == calls_before
    assert orch.subscribed_markets == {"A"}
    assert len(wal.snapshots) == 1                      # nothing written for the failed cycle


def test_cap_evicts_lowest_volume_outside_top(tmp_path, gamma):
    orch, pool, wal = make_orch(tmp_path, n_markets=2, max_subscribed_markets=3)
    gamma.top = [mk("A", 10), mk("B", 50)]
    cycle(orch)

    # C, D take the top; A and B still trade. 4 markets > cap 3 -> evict A (lowest volume).
    gamma.top = [mk("C", 1), mk("D", 2)]                 # top markets may have low volume:
    gamma.status = {"A": mk("A", 10), "B": mk("B", 50)}  # they are never evicted
    cycle(orch)

    assert wal.last("removed") == {"A": "cap"}
    assert orch.subscribed_markets == {"B", "C", "D"}
    assert pool.tokens == tokens("B", "C", "D")
    rec = wal.last_records()["A"]
    assert rec["sub"]["action"] == "removed" and rec["sub"]["reason"] == "cap"
    assert rec["raw"] is not None                        # we still record what Gamma said


def test_cap_not_enforced_when_lookup_fails(tmp_path, gamma):
    orch, pool, wal = make_orch(tmp_path, n_markets=2, max_subscribed_markets=3)
    gamma.top = [mk("A"), mk("B")]
    cycle(orch)

    gamma.top = [mk("C"), mk("D")]
    gamma.status = GammaError("boom")
    cycle(orch)
    assert wal.last("removed") == {}
    assert len(orch.subscribed_markets) == 4            # temporarily over the cap

    gamma.status = {"A": mk("A", 1), "B": mk("B", 2)}   # next good cycle trims it
    cycle(orch)
    assert wal.last("removed") == {"A": "cap"}
    assert len(orch.subscribed_markets) == 3


def test_every_subscribed_market_gets_one_record_per_cycle(tmp_path, gamma):
    orch, pool, wal = make_orch(tmp_path, n_markets=2, max_subscribed_markets=10)
    gamma.top = [mk("A"), mk("B")]
    cycle(orch)
    gamma.top = [mk("B"), mk("C")]
    gamma.status = {"A": mk("A")}
    cycle(orch)

    ts, records = wal.snapshots[-1]
    assert sorted(r["market_id"] for r in records) == ["A", "B", "C"]
    assert [r["rank"] for r in records if r["source"] == "top"] == [1, 2]


# --- connection events -----------------------------------------------------------

def test_connection_events_go_to_wal(tmp_path):
    orch, _pool, wal = make_orch(tmp_path)
    extra = {"assets": ["a", "b"], "asset_count": 2}
    asyncio.run(orch._on_event(3, ConnectionEvent.CONNECTED, extra))

    (ts, event), = wal.events
    assert ts > 0
    assert event == {"conn_id": 3, "event": "connected", "extra": extra}


def test_sampler_writes_metrics_to_wal_and_survives_errors(tmp_path, monkeypatch):
    monkeypatch.setattr(orchestrator, "METRICS_SAMPLE_INTERVAL_SEC", 0.01)
    orch, _, wal = make_orch(tmp_path)
    wal.fail_metrics = 2               # first two writes fail; the sampler must keep going

    async def go():
        task = asyncio.create_task(orch._sampler_loop())
        await asyncio.sleep(0.15)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(go())
    assert len(wal.metrics) >= 3
    ts, sample = wal.metrics[-1]
    assert ts > 0 and "msgs_total" in sample and "errors" in sample
    assert [t for t, _ in wal.metrics] == sorted(t for t, _ in wal.metrics)


def test_metrics_report_current_subscriptions(tmp_path, gamma, monkeypatch, caplog):
    monkeypatch.setattr(orchestrator, "METRICS_SAMPLE_INTERVAL_SEC", 0.01)
    orch, _, wal = make_orch(tmp_path, n_markets=2, max_subscribed_markets=2)

    async def sample_once():
        n = len(wal.metrics)
        task = asyncio.create_task(orch._sampler_loop())
        while len(wal.metrics) == n:
            await asyncio.sleep(0.005)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        return wal.metrics[-1][1]

    sample = asyncio.run(sample_once())
    assert sample["subscribed_markets"] == 0 and sample["subscribed_tokens"] == 0
    assert "assets_seen" not in sample

    gamma.top = [mk("A"), mk("B")]
    cycle(orch)
    with caplog.at_level("INFO", logger="src.orchestrator"):
        sample = asyncio.run(sample_once())
    assert sample["subscribed_markets"] == 2 and sample["subscribed_tokens"] == 4
    assert "subs=2/4" in caplog.text

    # B drops out and is evicted to stay under the cap: counts go down, not up.
    gamma.top = [mk("A"), mk("C", volume=1000)]
    gamma.status = {"B": mk("B", volume=5)}
    cycle(orch)
    sample = asyncio.run(sample_once())
    assert sample["subscribed_markets"] == 2 and sample["subscribed_tokens"] == 4
    assert orch.subscribed_markets == {"A", "C"}
