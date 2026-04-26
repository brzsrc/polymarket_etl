"""
Tests for ``WSConnection`` using a real local WebSocket server.

We start a tiny ``websockets.serve()`` instance that emulates Polymarket's
protocol: accepts the initial subscribe message, replies to "PING" with
"PONG", and supports our test scripts pushing arbitrary messages or closing
the connection.

This is much more reliable than mocking the websockets library — we test
the actual receive/send logic, the actual ping cadence, and the actual
reconnect path.
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Any

import pytest
import websockets
from websockets.asyncio.server import ServerConnection, serve

from polymarket_wal.ws.connection import (
    PONG_TEXT,
    ConnectionEvent,
    WSConnection,
)

logging.basicConfig(level=logging.DEBUG)


# ---------------------------------------------------------------------------
# Fake Polymarket server
# ---------------------------------------------------------------------------


class FakePolymarketServer:
    """
    Minimal Polymarket-like WS server for testing.

    Tracks subscriptions, lets the test push messages, replies to PINGs.
    Optionally simulates failure modes (silent freeze, abrupt close).
    """

    def __init__(self) -> None:
        self.connections: list[ServerConnection] = []
        self.received_messages: list[Any] = []
        # We support multiple connections; track per-connection state.
        self._handlers: list[asyncio.Task] = []
        # Behavior flags toggleable from tests
        self.respond_to_ping = True
        self.silent_freeze = False  # if True, accept messages but stop sending

    async def handler(self, ws: ServerConnection) -> None:
        self.connections.append(ws)
        try:
            async for msg in ws:
                self.received_messages.append((ws, msg))
                if msg == "PING":
                    if self.respond_to_ping and not self.silent_freeze:
                        await ws.send(PONG_TEXT)
                # We don't auto-respond to subscribe messages; tests push
                # data explicitly.
        except websockets.exceptions.ConnectionClosed:
            pass

    async def push_to_all(self, message: str) -> None:
        """Send a message to all currently connected clients."""
        if self.silent_freeze:
            return
        for ws in list(self.connections):
            try:
                await ws.send(message)
            except websockets.exceptions.ConnectionClosed:
                pass

    async def close_all(self, code: int = 1011) -> None:
        for ws in list(self.connections):
            try:
                await ws.close(code=code)
            except Exception:
                pass


@pytest.fixture
async def fake_server():
    """Start a fake Polymarket server on a free port for the test."""
    fake = FakePolymarketServer()
    server = await serve(fake.handler, "127.0.0.1", 0)
    # Get the actual bound port
    port = server.sockets[0].getsockname()[1]
    fake.url = f"ws://127.0.0.1:{port}"
    try:
        yield fake
    finally:
        server.close()
        await server.wait_closed()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class CapturingHandler:
    """Async callable that records every message it receives."""

    def __init__(self) -> None:
        # (asset_ids, raw_bytes, parsed, ts_recv, conn_id)
        self.messages: list = []
        self.events: list[tuple[int, ConnectionEvent, dict]] = []

    async def on_message(
        self,
        asset_ids: tuple[str, ...],
        raw_bytes: bytes,
        parsed,
        ts_recv: datetime,
        conn_id: int,
    ) -> None:
        self.messages.append((asset_ids, raw_bytes, parsed, ts_recv, conn_id))

    async def on_event(self, conn_id: int, event: ConnectionEvent, extra: dict) -> None:
        self.events.append((conn_id, event, extra))


async def wait_for(condition_fn, timeout: float = 5.0) -> None:
    """Poll condition_fn() until True or timeout; small sleep granularity."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if condition_fn():
            return
        await asyncio.sleep(0.05)
    raise TimeoutError("condition not met within timeout")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestConnectAndSubscribe:
    @pytest.mark.asyncio
    async def test_initial_subscribe_sent_with_custom_feature_enabled(
        self, fake_server
    ):
        handler = CapturingHandler()
        conn = WSConnection(
            conn_id=0,
            on_message=handler.on_message,
            on_event=handler.on_event,
            ws_url=fake_server.url,
        )
        await conn.add_subscriptions(["asset-1", "asset-2"])

        run_task = asyncio.create_task(conn.run())
        try:
            # Wait for the SUBSCRIBED event
            await wait_for(
                lambda: any(e[1] == ConnectionEvent.SUBSCRIBED for e in handler.events),
                timeout=3,
            )
            # Server should have received exactly one subscribe message
            await wait_for(lambda: len(fake_server.received_messages) >= 1, timeout=3)

            _ws, sub_msg = fake_server.received_messages[0]
            parsed = json.loads(sub_msg)
            assert parsed["type"] == "market"
            assert set(parsed["assets_ids"]) == {"asset-1", "asset-2"}
            # CRITICAL: this is the bit nautilus didn't do.
            assert parsed["custom_feature_enabled"] is True
        finally:
            conn.stop()
            await run_task

    @pytest.mark.asyncio
    async def test_emits_lifecycle_events_in_order(self, fake_server):
        handler = CapturingHandler()
        conn = WSConnection(
            conn_id=42,
            on_message=handler.on_message,
            on_event=handler.on_event,
            ws_url=fake_server.url,
        )
        run_task = asyncio.create_task(conn.run())
        try:
            await wait_for(
                lambda: any(e[1] == ConnectionEvent.SUBSCRIBED for e in handler.events),
                timeout=3,
            )
            evt_types = [e[1] for e in handler.events]
            # Order: CONNECTED -> SUBSCRIBED
            assert ConnectionEvent.CONNECTED in evt_types
            i_conn = evt_types.index(ConnectionEvent.CONNECTED)
            i_sub = evt_types.index(ConnectionEvent.SUBSCRIBED)
            assert i_conn < i_sub
            # All events tagged with our conn_id
            assert all(e[0] == 42 for e in handler.events)
        finally:
            conn.stop()
            await run_task


class TestMessageReceive:
    @pytest.mark.asyncio
    async def test_book_message_dispatched_with_asset_id(self, fake_server):
        handler = CapturingHandler()
        conn = WSConnection(
            conn_id=0,
            on_message=handler.on_message,
            on_event=handler.on_event,
            ws_url=fake_server.url,
        )
        await conn.add_subscriptions(["asset-1"])
        run_task = asyncio.create_task(conn.run())
        try:
            await wait_for(lambda: len(fake_server.connections) > 0, timeout=3)

            book = {
                "event_type": "book",
                "asset_id": "asset-1",
                "bids": [{"price": "0.5", "size": "10"}],
                "asks": [],
            }
            await fake_server.push_to_all(json.dumps(book))

            await wait_for(lambda: len(handler.messages) >= 1, timeout=3)
            asset_ids, raw_bytes, parsed, ts_recv, conn_id = handler.messages[0]
            assert asset_ids == ("asset-1",)
            # Bytes must be the original — no parse/re-serialize round trip.
            assert json.loads(raw_bytes) == book
            # The parsed dict is also forwarded so downstream consumers
            # (like the WAL writer) don't have to decode again.
            assert parsed == book
            assert isinstance(ts_recv, datetime)
            assert conn_id == 0
        finally:
            conn.stop()
            await run_task

    @pytest.mark.asyncio
    async def test_pong_filtered_not_dispatched(self, fake_server):
        """PONG replies to our PING are filtered before dispatch — they're
        heartbeat, not data."""
        handler = CapturingHandler()
        conn = WSConnection(
            conn_id=0,
            on_message=handler.on_message,
            on_event=handler.on_event,
            ws_url=fake_server.url,
            ping_interval_sec=0.1,  # speed up for the test
        )
        run_task = asyncio.create_task(conn.run())
        try:
            await wait_for(lambda: len(fake_server.connections) > 0, timeout=3)
            # Wait long enough for a few PINGs to roundtrip
            await asyncio.sleep(0.5)
            # We may have received PONGs at the WS level, but none should
            # have gone through to the handler.
            assert all(
                raw != b"PONG"
                for _, raw, _, _, _ in handler.messages
            )
        finally:
            conn.stop()
            await run_task

    @pytest.mark.asyncio
    async def test_initial_array_response_dispatched_as_one_message(
        self, fake_server
    ):
        """When subscribing, Polymarket sends a JSON array of book snapshots.
        We dispatch that as a single message (with multiple asset_ids), not
        N separate messages — it's one wire frame."""
        handler = CapturingHandler()
        conn = WSConnection(
            conn_id=0,
            on_message=handler.on_message,
            ws_url=fake_server.url,
        )
        await conn.add_subscriptions(["a1", "a2"])
        run_task = asyncio.create_task(conn.run())
        try:
            await wait_for(lambda: len(fake_server.connections) > 0, timeout=3)
            payload = [
                {"event_type": "book", "asset_id": "a1", "bids": []},
                {"event_type": "book", "asset_id": "a2", "bids": []},
            ]
            await fake_server.push_to_all(json.dumps(payload))
            await wait_for(lambda: len(handler.messages) >= 1, timeout=3)
            asset_ids, raw_bytes, parsed, _, _ = handler.messages[0]
            assert set(asset_ids) == {"a1", "a2"}
            # Single message in handler, not two
            assert len(handler.messages) == 1
            # Parsed form preserves the list-of-dicts shape
            assert isinstance(parsed, list)
            assert len(parsed) == 2
        finally:
            conn.stop()
            await run_task


class TestPing:
    @pytest.mark.asyncio
    async def test_ping_sent_periodically(self, fake_server):
        handler = CapturingHandler()
        conn = WSConnection(
            conn_id=0,
            on_message=handler.on_message,
            ws_url=fake_server.url,
            ping_interval_sec=0.2,
        )
        run_task = asyncio.create_task(conn.run())
        try:
            await wait_for(lambda: len(fake_server.connections) > 0, timeout=3)
            # Wait for a few PING cycles
            await asyncio.sleep(0.7)
            # Server should have received PING(s) — at least 2 in 0.7s with 0.2s interval.
            ping_count = sum(
                1 for _, m in fake_server.received_messages if m == "PING"
            )
            assert ping_count >= 2
        finally:
            conn.stop()
            await run_task


class TestWatchdogAndReconnect:
    @pytest.mark.asyncio
    async def test_watchdog_triggers_reconnect_on_silent_freeze(self, fake_server):
        """If no data flows for data_idle_timeout, we force reconnect even
        if PING/PONG works."""
        handler = CapturingHandler()
        conn = WSConnection(
            conn_id=0,
            on_message=handler.on_message,
            on_event=handler.on_event,
            ws_url=fake_server.url,
            ping_interval_sec=0.05,  # check watchdog frequently
            data_idle_timeout_sec=0.3,  # very short
        )
        run_task = asyncio.create_task(conn.run())
        try:
            await wait_for(lambda: len(fake_server.connections) > 0, timeout=3)
            # Don't push any data. After 0.3s of silence, watchdog should fire.
            await wait_for(
                lambda: any(
                    e[1] == ConnectionEvent.WATCHDOG_TIMEOUT for e in handler.events
                ),
                timeout=3,
            )
            # And we should subsequently see a reconnect
            await wait_for(
                lambda: any(
                    e[1] == ConnectionEvent.RECONNECTING for e in handler.events
                ),
                timeout=3,
            )
        finally:
            conn.stop()
            await run_task

    @pytest.mark.asyncio
    async def test_reconnect_resends_full_subscription(self, fake_server):
        """After reconnect, the connection must re-send the full asset
        list — Polymarket forgets state on disconnect."""
        handler = CapturingHandler()
        conn = WSConnection(
            conn_id=0,
            on_message=handler.on_message,
            on_event=handler.on_event,
            ws_url=fake_server.url,
        )
        await conn.add_subscriptions(["a1", "a2"])
        run_task = asyncio.create_task(conn.run())
        try:
            # Wait for first subscribe
            await wait_for(
                lambda: sum(
                    1 for _, m in fake_server.received_messages
                    if isinstance(m, str) and "assets_ids" in m
                ) >= 1,
                timeout=3,
            )
            # Force the server to drop the connection
            await fake_server.close_all()
            # Wait for our connection to reconnect and re-subscribe
            await wait_for(
                lambda: sum(
                    1 for _, m in fake_server.received_messages
                    if isinstance(m, str) and "assets_ids" in m
                ) >= 2,
                timeout=5,
            )
            # The 2nd subscribe should still have both asset_ids
            sub_msgs = [
                m for _, m in fake_server.received_messages
                if isinstance(m, str) and '"type"' in m and "market" in m
            ]
            assert len(sub_msgs) >= 2
            second = json.loads(sub_msgs[1])
            assert set(second["assets_ids"]) == {"a1", "a2"}
            assert second["custom_feature_enabled"] is True
        finally:
            conn.stop()
            await run_task

    @pytest.mark.asyncio
    async def test_graceful_server_close_does_not_emit_reconnecting(
        self, fake_server
    ):
        """When the server closes the connection cleanly (e.g. Cloudflare's
        scheduled long-connection cycle), we treat the next connect as a
        continuation, NOT a retry. No RECONNECTING event, no backoff sleep.

        Without this distinction, every Cloudflare cycle (~daily) would
        spam RECONNECTING events and confuse monitoring."""
        handler = CapturingHandler()
        conn = WSConnection(
            conn_id=0,
            on_message=handler.on_message,
            on_event=handler.on_event,
            ws_url=fake_server.url,
        )
        run_task = asyncio.create_task(conn.run())
        try:
            await wait_for(lambda: len(fake_server.connections) > 0, timeout=3)
            # Server closes cleanly (code 1000)
            await fake_server.close_all(code=1000)
            # Wait for the second CONNECTED event (i.e. reconnect happened)
            await wait_for(
                lambda: sum(
                    1 for e in handler.events if e[1] == ConnectionEvent.CONNECTED
                ) >= 2,
                timeout=5,
            )
            # No RECONNECTING events should have been emitted between the
            # two CONNECTED events
            evt_seq = [e[1] for e in handler.events]
            assert ConnectionEvent.RECONNECTING not in evt_seq, (
                f"unexpected RECONNECTING in graceful-close path: {evt_seq}"
            )
        finally:
            conn.stop()
            await run_task

    @pytest.mark.asyncio
    async def test_watchdog_disconnect_emits_reconnecting(self, fake_server):
        """In contrast to graceful close, a watchdog-triggered disconnect IS
        a problem — we want monitoring to see it. Verify RECONNECTING is
        emitted on this path."""
        handler = CapturingHandler()
        conn = WSConnection(
            conn_id=0,
            on_message=handler.on_message,
            on_event=handler.on_event,
            ws_url=fake_server.url,
            ping_interval_sec=0.05,
            data_idle_timeout_sec=0.3,
        )
        run_task = asyncio.create_task(conn.run())
        try:
            await wait_for(lambda: len(fake_server.connections) > 0, timeout=3)
            # No data pushed → watchdog should fire
            await wait_for(
                lambda: any(
                    e[1] == ConnectionEvent.WATCHDOG_TIMEOUT for e in handler.events
                ),
                timeout=3,
            )
            await wait_for(
                lambda: any(
                    e[1] == ConnectionEvent.RECONNECTING for e in handler.events
                ),
                timeout=3,
            )
            # Check the RECONNECTING event has attempt > 0
            reconnect_evt = next(
                e for e in handler.events if e[1] == ConnectionEvent.RECONNECTING
            )
            _, _, extra = reconnect_evt
            assert extra["attempt"] >= 1
        finally:
            conn.stop()
            await run_task


class TestSubscriptionMutation:
    @pytest.mark.asyncio
    async def test_add_after_connected_sends_incremental_subscribe(
        self, fake_server
    ):
        """add_subscriptions() called after the connection is live should
        send an incremental subscribe message with operation=subscribe."""
        handler = CapturingHandler()
        conn = WSConnection(
            conn_id=0,
            on_message=handler.on_message,
            ws_url=fake_server.url,
        )
        await conn.add_subscriptions(["a1"])
        run_task = asyncio.create_task(conn.run())
        try:
            # Wait for initial subscribe
            await wait_for(
                lambda: len(fake_server.received_messages) >= 1, timeout=3
            )
            # Add one more asset
            await conn.add_subscriptions(["a2"])
            await wait_for(
                lambda: any(
                    isinstance(m, str) and '"operation":"subscribe"' in m
                    for _, m in fake_server.received_messages
                ),
                timeout=3,
            )
            # Find and inspect the incremental message
            inc = next(
                json.loads(m)
                for _, m in fake_server.received_messages
                if isinstance(m, str) and '"operation":"subscribe"' in m
            )
            assert inc["assets_ids"] == ["a2"]
            assert inc["operation"] == "subscribe"
        finally:
            conn.stop()
            await run_task

    @pytest.mark.asyncio
    async def test_add_dedupes_existing(self, fake_server):
        handler = CapturingHandler()
        conn = WSConnection(
            conn_id=0,
            on_message=handler.on_message,
            ws_url=fake_server.url,
        )
        await conn.add_subscriptions(["a1"])
        run_task = asyncio.create_task(conn.run())
        try:
            await wait_for(
                lambda: len(fake_server.received_messages) >= 1, timeout=3
            )
            initial_count = len(fake_server.received_messages)
            # Add the same asset again — should NOT send another sub message
            await conn.add_subscriptions(["a1"])
            await asyncio.sleep(0.2)
            # No new subscribe-related message
            new_count = len(fake_server.received_messages)
            # Allow PINGs in between to count, but no new subscribe message
            new_subs = sum(
                1 for _, m in fake_server.received_messages[initial_count:]
                if isinstance(m, str) and "assets_ids" in m
            )
            assert new_subs == 0
        finally:
            conn.stop()
            await run_task

    @pytest.mark.asyncio
    async def test_subscriptions_persist_across_reconnect(self, fake_server):
        """Ensure that asset_ids added before a connection drop are still
        present after the reconnect's re-subscribe."""
        handler = CapturingHandler()
        conn = WSConnection(
            conn_id=0,
            on_message=handler.on_message,
            ws_url=fake_server.url,
        )
        await conn.add_subscriptions(["a1", "a2", "a3"])
        run_task = asyncio.create_task(conn.run())
        try:
            await wait_for(lambda: len(fake_server.connections) > 0, timeout=3)
            await fake_server.close_all()
            # Wait for reconnect and second subscribe
            await wait_for(
                lambda: len(fake_server.connections) >= 2 or
                sum(
                    1 for _, m in fake_server.received_messages
                    if isinstance(m, str) and '"type"' in m
                ) >= 2,
                timeout=5,
            )
            sub_msgs = [
                json.loads(m)
                for _, m in fake_server.received_messages
                if isinstance(m, str) and '"type"' in m and "market" in m
            ]
            assert len(sub_msgs) >= 2
            assert set(sub_msgs[-1]["assets_ids"]) == {"a1", "a2", "a3"}
        finally:
            conn.stop()
            await run_task


class TestBackoff:
    def test_backoff_is_capped(self):
        from polymarket_wal.ws.connection import RECONNECT_BACKOFF_MAX_SEC

        # Even at huge attempt numbers, we never sleep more than the cap.
        for attempt in range(1, 50):
            assert WSConnection._compute_backoff(attempt) <= RECONNECT_BACKOFF_MAX_SEC

    def test_backoff_zero_at_attempt_zero(self):
        assert WSConnection._compute_backoff(0) == 0.0

    def test_backoff_grows(self):
        # With full jitter, the *upper bound* doubles each step. We sample
        # many times and check the maximum approximately doubles.
        from random import seed
        seed(42)
        samples_at_1 = [WSConnection._compute_backoff(1) for _ in range(100)]
        samples_at_3 = [WSConnection._compute_backoff(3) for _ in range(100)]
        assert max(samples_at_3) > max(samples_at_1)
