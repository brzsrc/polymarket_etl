"""
Tests for ``WSPool`` — sharding, refcount, dispatch.

We replace ``WSConnection`` with a controllable mock so we can test the
pool's logic in isolation. The actual connection logic is exercised by
``test_ws_connection.py``.
"""

import asyncio
from datetime import datetime, timezone
from typing import Any
from unittest.mock import patch

import pytest

from polymarket_wal.ws.connection import ConnectionEvent
from polymarket_wal.ws.pool import WSPool


# ---------------------------------------------------------------------------
# Mock WSConnection
# ---------------------------------------------------------------------------


class MockWSConnection:
    """
    Drop-in replacement for ``WSConnection`` that records calls and lets
    tests simulate incoming messages via ``simulate_message()``.
    """

    instances: list["MockWSConnection"] = []

    def __init__(
        self,
        conn_id: int,
        on_message,
        on_event=None,
        **_kwargs,  # ws_url, ping_interval_sec, etc. — ignored in mock
    ) -> None:
        self.conn_id = conn_id
        self.on_message = on_message
        self.on_event = on_event
        self.subs: set[str] = set()
        self.adds: list[list[str]] = []
        self.removes: list[list[str]] = []
        self._stopped = asyncio.Event()
        MockWSConnection.instances.append(self)

    @property
    def subscription_count(self) -> int:
        return len(self.subs)

    def has_subscription(self, asset_id: str) -> bool:
        return asset_id in self.subs

    async def add_subscriptions(self, asset_ids: list[str]) -> None:
        new = [a for a in asset_ids if a not in self.subs]
        if new:
            self.subs.update(new)
            self.adds.append(new)

    async def remove_subscriptions(self, asset_ids: list[str]) -> None:
        rem = [a for a in asset_ids if a in self.subs]
        if rem:
            self.subs.difference_update(rem)
            self.removes.append(rem)

    def stop(self) -> None:
        self._stopped.set()

    async def run(self) -> None:
        await self._stopped.wait()

    async def simulate_message(
        self,
        asset_ids: tuple[str, ...],
        raw_bytes: bytes,
        parsed=None,
    ) -> None:
        """Test helper: pretend the server sent us this message.

        If ``parsed`` is None and ``raw_bytes`` is valid JSON, we parse it
        ourselves so test callers don't have to construct both forms."""
        if parsed is None:
            try:
                import json as _json
                parsed = _json.loads(raw_bytes)
            except Exception:
                parsed = None
        await self.on_message(
            asset_ids, raw_bytes, parsed, datetime.now(timezone.utc), self.conn_id
        )

    async def simulate_event(self, event: ConnectionEvent, extra: dict) -> None:
        if self.on_event:
            await self.on_event(self.conn_id, event, extra)

    @classmethod
    def reset(cls) -> None:
        cls.instances.clear()


@pytest.fixture(autouse=True)
def patch_connection():
    """Replace WSConnection in pool module with our mock for every test."""
    MockWSConnection.reset()
    with patch("polymarket_wal.ws.pool.WSConnection", MockWSConnection):
        yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class CapturingHandler:
    def __init__(self) -> None:
        self.messages: list = []
        self.events: list = []

    async def on_message(self, asset_ids, raw_bytes, parsed, ts_recv, conn_id):
        self.messages.append((asset_ids, raw_bytes, parsed, ts_recv, conn_id))

    async def on_event(self, conn_id, event, extra):
        self.events.append((conn_id, event, extra))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSharding:
    @pytest.mark.asyncio
    async def test_assets_under_limit_share_one_connection(self):
        h = CapturingHandler()
        pool = WSPool(on_message=h.on_message, max_per_connection=10)
        await pool.start()
        try:
            await pool.add_subscriptions([f"asset-{i}" for i in range(5)])
            stats = pool.stats()
            assert stats["connections"] == 1
            assert stats["total_subscriptions"] == 5
        finally:
            await pool.stop()

    @pytest.mark.asyncio
    async def test_overflow_creates_second_connection(self):
        h = CapturingHandler()
        pool = WSPool(on_message=h.on_message, max_per_connection=3)
        await pool.start()
        try:
            await pool.add_subscriptions([f"a{i}" for i in range(7)])
            stats = pool.stats()
            # 7 assets at 3/conn = 3 connections (3+3+1)
            assert stats["connections"] == 3
            assert stats["total_subscriptions"] == 7
            # First-fit: first conn full, second full, third has 1
            counts = sorted(stats["per_connection"].values())
            assert counts == [1, 3, 3]
        finally:
            await pool.stop()

    @pytest.mark.asyncio
    async def test_first_fit_reuses_existing_capacity(self):
        """If we add some, remove some, then add more — the new ones
        should fill empty slots in the first conn before opening a new one."""
        h = CapturingHandler()
        pool = WSPool(on_message=h.on_message, max_per_connection=3)
        await pool.start()
        try:
            # Fill conn 0
            await pool.add_subscriptions(["a", "b", "c"])
            assert pool.stats()["connections"] == 1
            # Free a slot
            await pool.remove_subscriptions(["b"])
            assert pool.stats()["connections"] == 1
            assert pool.stats()["total_subscriptions"] == 2
            # Add another — should fit in conn 0, NOT spawn conn 1
            await pool.add_subscriptions(["d"])
            assert pool.stats()["connections"] == 1
        finally:
            await pool.stop()

    @pytest.mark.asyncio
    async def test_max_per_connection_validation(self):
        h = CapturingHandler()
        with pytest.raises(ValueError):
            WSPool(on_message=h.on_message, max_per_connection=0)
        with pytest.raises(ValueError):
            WSPool(on_message=h.on_message, max_per_connection=501)


class TestRefcounting:
    @pytest.mark.asyncio
    async def test_dup_add_increments_refcount_only(self):
        h = CapturingHandler()
        pool = WSPool(on_message=h.on_message, max_per_connection=10)
        await pool.start()
        try:
            await pool.add_subscriptions(["x"])
            await pool.add_subscriptions(["x"])
            await pool.add_subscriptions(["x"])
            # Underlying connection should only have x subscribed once
            assert MockWSConnection.instances[0].subs == {"x"}
            # And only one .adds call (the first); subsequent are no-ops
            assert MockWSConnection.instances[0].adds == [["x"]]
        finally:
            await pool.stop()

    @pytest.mark.asyncio
    async def test_remove_only_unsubs_when_refcount_hits_zero(self):
        h = CapturingHandler()
        pool = WSPool(on_message=h.on_message, max_per_connection=10)
        await pool.start()
        try:
            await pool.add_subscriptions(["x"])
            await pool.add_subscriptions(["x"])
            await pool.add_subscriptions(["x"])
            # 3 references — first 2 removes are no-ops
            await pool.remove_subscriptions(["x"])
            assert MockWSConnection.instances[0].subs == {"x"}
            assert MockWSConnection.instances[0].removes == []
            await pool.remove_subscriptions(["x"])
            assert MockWSConnection.instances[0].subs == {"x"}
            # Third remove drops to zero — actual unsubscribe fires
            await pool.remove_subscriptions(["x"])
            assert MockWSConnection.instances[0].subs == set()
            assert MockWSConnection.instances[0].removes == [["x"]]
        finally:
            await pool.stop()

    @pytest.mark.asyncio
    async def test_remove_unknown_asset_is_noop(self):
        h = CapturingHandler()
        pool = WSPool(on_message=h.on_message, max_per_connection=10)
        await pool.start()
        try:
            await pool.remove_subscriptions(["never-subbed"])
            # No connection should have been created just for a remove
            assert pool.stats()["connections"] == 0
        finally:
            await pool.stop()


class TestDispatch:
    @pytest.mark.asyncio
    async def test_messages_forwarded_to_handler(self):
        h = CapturingHandler()
        pool = WSPool(on_message=h.on_message, on_event=h.on_event)
        await pool.start()
        try:
            await pool.add_subscriptions(["a1"])
            conn = MockWSConnection.instances[0]
            await conn.simulate_message(("a1",), b'{"event_type":"book","asset_id":"a1"}')
            assert len(h.messages) == 1
            asset_ids, raw_bytes, parsed, _, conn_id = h.messages[0]
            assert asset_ids == ("a1",)
            assert raw_bytes == b'{"event_type":"book","asset_id":"a1"}'
            # MockWSConnection auto-parses raw_bytes when parsed=None
            assert parsed == {"event_type": "book", "asset_id": "a1"}
            assert conn_id == 0
        finally:
            await pool.stop()

    @pytest.mark.asyncio
    async def test_events_forwarded(self):
        h = CapturingHandler()
        pool = WSPool(on_message=h.on_message, on_event=h.on_event)
        await pool.start()
        try:
            await pool.add_subscriptions(["a1"])
            conn = MockWSConnection.instances[0]
            await conn.simulate_event(ConnectionEvent.WATCHDOG_TIMEOUT, {"idle": 65})
            assert len(h.events) == 1
            cid, evt, extra = h.events[0]
            assert cid == 0
            assert evt == ConnectionEvent.WATCHDOG_TIMEOUT
            assert extra == {"idle": 65}
        finally:
            await pool.stop()

    @pytest.mark.asyncio
    async def test_handler_exception_does_not_kill_dispatch(self):
        async def buggy_handler(*_args):
            raise RuntimeError("boom")

        pool = WSPool(on_message=buggy_handler)
        await pool.start()
        try:
            await pool.add_subscriptions(["a1"])
            conn = MockWSConnection.instances[0]
            # Should not raise
            await conn.simulate_message(("a1",), b'{"event_type":"book"}')
            # And the pool is still operating
            assert pool.stats()["total_subscriptions"] == 1
        finally:
            await pool.stop()

    @pytest.mark.asyncio
    async def test_messages_for_unknown_asset_still_forwarded(self):
        """new_market arrives with an asset_id we never subscribed to.
        We must not filter it out — the WAL wants to record it."""
        h = CapturingHandler()
        pool = WSPool(on_message=h.on_message)
        await pool.start()
        try:
            await pool.add_subscriptions(["a1"])
            conn = MockWSConnection.instances[0]
            # Simulate a new_market event for an asset NOT in our set
            await conn.simulate_message(
                ("brand-new-asset",),
                b'{"event_type":"new_market","asset_id":"brand-new-asset"}',
            )
            assert len(h.messages) == 1
            assert h.messages[0][0] == ("brand-new-asset",)
        finally:
            await pool.stop()


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_add_before_start_raises(self):
        h = CapturingHandler()
        pool = WSPool(on_message=h.on_message)
        # Not started yet
        with pytest.raises(RuntimeError):
            await pool.add_subscriptions(["x"])

    @pytest.mark.asyncio
    async def test_start_is_idempotent(self):
        h = CapturingHandler()
        pool = WSPool(on_message=h.on_message)
        await pool.start()
        await pool.start()  # no-op
        await pool.stop()

    @pytest.mark.asyncio
    async def test_stop_terminates_all_connections(self):
        h = CapturingHandler()
        pool = WSPool(on_message=h.on_message, max_per_connection=2)
        await pool.start()
        await pool.add_subscriptions(["a", "b", "c", "d"])  # 2 connections
        assert pool.stats()["connections"] == 2
        await pool.stop()
        # All mock connections were stopped
        for c in MockWSConnection.instances:
            assert c._stopped.is_set()


class TestStats:
    @pytest.mark.asyncio
    async def test_stats_reports_per_connection_counts(self):
        h = CapturingHandler()
        pool = WSPool(on_message=h.on_message, max_per_connection=3)
        await pool.start()
        try:
            await pool.add_subscriptions([f"a{i}" for i in range(7)])
            stats = pool.stats()
            assert stats["running"] is True
            assert stats["connections"] == 3
            assert stats["unique_assets"] == 7
            assert stats["total_subscriptions"] == 7
        finally:
            await pool.stop()
