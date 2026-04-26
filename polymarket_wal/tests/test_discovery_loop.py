"""
Tests for ``DiscoveryLoop`` — diff calculation and 3-strike removal.

We mock both the GammaClient (so we can control what each cycle "sees")
and the WSPool (so we can verify what add/remove calls were made).
"""

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from polymarket_wal.discovery_loop import DiscoveryLoop
from polymarket_wal.gamma_client import GammaError
from polymarket_wal.market_discovery import DiscoveryResult
from polymarket_wal.models import Market


def make_market(market_id: int) -> Market:
    """A minimal Market record for diff testing."""
    return Market(
        id=str(market_id),
        condition_id=f"0x{market_id:064x}",
        slug=f"m{market_id}",
        question=f"q{market_id}",
        token_ids=(f"{market_id}001", f"{market_id}002"),
        outcomes=("Yes", "No"),
        active=True,
        closed=False,
        archived=False,
        accepting_orders=True,
        enable_order_book=True,
        end_date=None,
        start_date=None,
        tick_size=0.01,
        min_order_size=5,
        neg_risk=False,
    )


def make_result(market_ids: list[int]) -> DiscoveryResult:
    markets = [make_market(i) for i in market_ids]
    token_ids = set()
    for m in markets:
        token_ids.add(m.token_ids[0])
        token_ids.add(m.token_ids[1])
    now = datetime.now(timezone.utc)
    return DiscoveryResult(
        markets=markets,
        token_ids=token_ids,
        cycle_started_at=now,
        cycle_finished_at=now,
        raw_records_seen=len(markets),
    )


class FakePool:
    """Records add/remove calls; doesn't connect to anything."""

    def __init__(self) -> None:
        self.added: list[set[str]] = []
        self.removed: list[set[str]] = []
        self._running = True

    async def start(self) -> None:
        self._running = True

    async def stop(self) -> None:
        self._running = False

    async def add_subscriptions(self, asset_ids: list[str]) -> None:
        self.added.append(set(asset_ids))

    async def remove_subscriptions(self, asset_ids: list[str]) -> None:
        self.removed.append(set(asset_ids))


# ---------------------------------------------------------------------------
# Test infrastructure
# ---------------------------------------------------------------------------


class CycleScript:
    """
    Lets a test scripted-control what each Gamma cycle returns.

    Usage:
        script = CycleScript([
            [1, 2, 3],       # cycle 1: markets 1, 2, 3
            [1, 2, 4],       # cycle 2: 3 is gone, 4 is new
            ...
        ])
        with patch_fetch(script):
            ...
    """

    def __init__(self, cycles: list[list[int] | Exception]) -> None:
        self.cycles = list(cycles)
        self.index = 0

    async def __call__(self, _gamma, markets_jsonl_path=None):
        if self.index >= len(self.cycles):
            raise StopAsyncIteration("script exhausted")
        cycle = self.cycles[self.index]
        self.index += 1
        if isinstance(cycle, Exception):
            raise cycle
        return make_result(cycle)


def patch_fetch(script: CycleScript):
    return patch(
        "polymarket_wal.discovery_loop.fetch_all_active_binary_markets",
        new=script,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSingleCycle:
    @pytest.mark.asyncio
    async def test_first_cycle_subscribes_everything_seen(self):
        pool = FakePool()
        script = CycleScript([[1, 2, 3]])
        with patch_fetch(script):
            loop = DiscoveryLoop(
                gamma=None,
                pool=pool,
                markets_jsonl_path=None,
            )
            diff = await loop.run_one_cycle()

        # 3 markets, 2 token_ids each = 6 token_ids subscribed
        assert diff is not None
        assert len(diff.added) == 6
        assert len(diff.removed) == 0
        assert len(pool.added) == 1
        assert len(pool.added[0]) == 6
        assert len(pool.removed) == 0
        assert loop.current_subscriptions == diff.added

    @pytest.mark.asyncio
    async def test_unchanged_cycle_no_op(self):
        pool = FakePool()
        script = CycleScript([[1, 2], [1, 2]])
        with patch_fetch(script):
            loop = DiscoveryLoop(gamma=None, pool=pool, markets_jsonl_path=None)
            diff1 = await loop.run_one_cycle()
            diff2 = await loop.run_one_cycle()

        assert len(diff1.added) == 4
        assert len(diff2.added) == 0
        assert len(diff2.removed) == 0
        # Pool only saw the initial add
        assert len(pool.added) == 1

    @pytest.mark.asyncio
    async def test_pure_addition(self):
        pool = FakePool()
        script = CycleScript([[1, 2], [1, 2, 3]])
        with patch_fetch(script):
            loop = DiscoveryLoop(gamma=None, pool=pool, markets_jsonl_path=None)
            await loop.run_one_cycle()
            diff = await loop.run_one_cycle()

        # Market 3 added 2 token_ids
        assert len(diff.added) == 2
        # All token_ids of market 3
        new_market = make_market(3)
        assert diff.added == set(new_market.token_ids)


class TestRemovalStrikes:
    @pytest.mark.asyncio
    async def test_single_miss_does_not_remove(self):
        """An asset missing from one cycle is suspect, not removed."""
        pool = FakePool()
        script = CycleScript([[1, 2], [1]])  # market 2 missing in cycle 2
        with patch_fetch(script):
            loop = DiscoveryLoop(
                gamma=None,
                pool=pool,
                markets_jsonl_path=None,
                removal_strikes=3,
            )
            await loop.run_one_cycle()
            diff = await loop.run_one_cycle()

        # 1 strike accumulated, but no removal yet
        assert len(diff.removed) == 0
        assert len(pool.removed) == 0
        # Tokens for market 2 should still be in our set
        m2 = make_market(2)
        assert m2.token_ids[0] in loop.current_subscriptions

    @pytest.mark.asyncio
    async def test_three_misses_removes(self):
        pool = FakePool()
        script = CycleScript([[1, 2], [1], [1], [1]])
        with patch_fetch(script):
            loop = DiscoveryLoop(
                gamma=None,
                pool=pool,
                markets_jsonl_path=None,
                removal_strikes=3,
            )
            await loop.run_one_cycle()  # cycle 1: see 1,2
            await loop.run_one_cycle()  # cycle 2: 2 gone (strike 1)
            await loop.run_one_cycle()  # cycle 3: 2 gone (strike 2)
            diff = await loop.run_one_cycle()  # cycle 4: 2 gone (strike 3 -> remove)

        m2 = make_market(2)
        assert diff.removed == set(m2.token_ids)
        assert len(pool.removed) == 1
        assert pool.removed[0] == set(m2.token_ids)

    @pytest.mark.asyncio
    async def test_strike_resets_on_reappearance(self):
        """If an asset reappears mid-strike, strikes reset."""
        pool = FakePool()
        # market 2 vanishes for 2 cycles, reappears, vanishes again
        script = CycleScript([[1, 2], [1], [1], [1, 2], [1], [1], [1]])
        with patch_fetch(script):
            loop = DiscoveryLoop(
                gamma=None,
                pool=pool,
                markets_jsonl_path=None,
                removal_strikes=3,
            )
            await loop.run_one_cycle()  # see both
            await loop.run_one_cycle()  # strike 1
            await loop.run_one_cycle()  # strike 2
            await loop.run_one_cycle()  # 2 reappeared — strikes RESET
            await loop.run_one_cycle()  # strike 1 again
            await loop.run_one_cycle()  # strike 2
            diff = await loop.run_one_cycle()  # strike 3 -> remove now

        m2 = make_market(2)
        assert diff.removed == set(m2.token_ids)
        # Only one removal across the whole script
        assert len(pool.removed) == 1


class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_gamma_error_skips_cycle(self):
        """GammaError aborts a cycle without affecting state."""
        pool = FakePool()
        script = CycleScript(
            [[1, 2], GammaError("simulated"), [1]]
        )
        with patch_fetch(script):
            loop = DiscoveryLoop(
                gamma=None,
                pool=pool,
                markets_jsonl_path=None,
                removal_strikes=2,
            )
            await loop.run_one_cycle()  # see 1,2
            result = await loop.run_one_cycle()  # Gamma error
            assert result is None
            # Nothing changed
            assert loop.current_subscriptions == frozenset(
                make_market(1).token_ids + make_market(2).token_ids
            )
            # Final cycle sees only 1, but strikes count from THIS cycle, not
            # the failed one — so this is strike 1, not 2.
            await loop.run_one_cycle()
            # No removal yet (only 1 strike since we have removal_strikes=2)
            assert len(pool.removed) == 0


class TestPersistence:
    @pytest.mark.asyncio
    async def test_jsonl_path_passed_through(self, tmp_path):
        """Loop forwards markets_jsonl_path to fetch_all_active_binary_markets."""
        pool = FakePool()
        path = tmp_path / "markets.jsonl"

        captured = []
        async def capture(gamma, markets_jsonl_path=None):
            captured.append(markets_jsonl_path)
            return make_result([1])

        with patch(
            "polymarket_wal.discovery_loop.fetch_all_active_binary_markets",
            new=capture,
        ):
            loop = DiscoveryLoop(
                gamma=None,
                pool=pool,
                markets_jsonl_path=path,
            )
            await loop.run_one_cycle()

        assert captured == [path]


class TestDiffHandler:
    @pytest.mark.asyncio
    async def test_on_diff_called_with_diff_object(self):
        pool = FakePool()
        diffs = []
        async def on_diff(diff):
            diffs.append(diff)

        script = CycleScript([[1, 2]])
        with patch_fetch(script):
            loop = DiscoveryLoop(
                gamma=None, pool=pool, markets_jsonl_path=None,
                on_diff=on_diff,
            )
            d = await loop.run_one_cycle()

        assert diffs == [d]

    @pytest.mark.asyncio
    async def test_on_diff_exception_swallowed(self):
        pool = FakePool()
        async def buggy_on_diff(_):
            raise RuntimeError("boom")

        script = CycleScript([[1]])
        with patch_fetch(script):
            loop = DiscoveryLoop(
                gamma=None, pool=pool, markets_jsonl_path=None,
                on_diff=buggy_on_diff,
            )
            # Should not raise
            d = await loop.run_one_cycle()
            assert d is not None


class TestBackgroundLoop:
    @pytest.mark.asyncio
    async def test_loop_runs_cycles_until_stopped(self):
        pool = FakePool()
        # Provide enough cycles so the loop always has something to fetch
        script = CycleScript([[1]] * 100)

        with patch_fetch(script):
            loop = DiscoveryLoop(
                gamma=None,
                pool=pool,
                markets_jsonl_path=None,
                interval_sec=0.05,
            )
            await loop.start()
            # Let it run a few cycles
            await asyncio.sleep(0.25)
            await loop.stop()

        # Should have run at least 3 cycles
        assert script.index >= 3

    @pytest.mark.asyncio
    async def test_validation_errors(self):
        pool = FakePool()
        with pytest.raises(ValueError):
            DiscoveryLoop(
                gamma=None, pool=pool, markets_jsonl_path=None,
                interval_sec=0,
            )
        with pytest.raises(ValueError):
            DiscoveryLoop(
                gamma=None, pool=pool, markets_jsonl_path=None,
                removal_strikes=0,
            )
