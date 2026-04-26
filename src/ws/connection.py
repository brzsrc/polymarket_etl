"""
Single WebSocket connection to Polymarket CLOB market channel.

One ``WSConnection`` owns one TCP/WS connection and a fixed-ish set of
``asset_ids`` it's subscribed to. The pool above orchestrates many of these.

Lifecycle:

    new (assets) -> CONNECTING -> SUBSCRIBING -> LIVE -> ... -> CLOSED
                       ^--<-- (reconnect on disconnect or watchdog timeout)

Key responsibilities (Tasks 2.1, 2.2, 2.3):

- Connect to wss://ws-subscriptions-clob.polymarket.com/ws/market
- Send the initial subscription message including ``custom_feature_enabled: true``
  so we receive ``new_market`` and ``best_bid_ask`` events.
- Send "PING" every 10 seconds (Polymarket's documented heartbeat protocol).
- Run a data-flow watchdog: if no DATA message arrives within 60s, force
  reconnect even if PING/PONG is fine. This handles the well-known "silent
  freeze" failure mode where the connection looks alive but the server has
  stopped pushing.
- On disconnect, retry with exponential backoff (1s -> 2s -> 4s -> ... cap 30s)
  and re-send the subscription message (Polymarket forgets state on reconnect).

Things this class does NOT do:

- Parse messages beyond extracting ``asset_id``s for routing. Bytes go up
  to the caller untouched (the WAL layer wants pristine bytes).
- Acknowledge/dedupe messages. We assume at-most-once and log gaps.
- Multiple channels. Market channel only — the user channel is a separate
  WS endpoint with auth and we don't need it for the WAL service.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from enum import Enum
from ..utilities import now_ns
import msgspec
import websockets
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Polymarket protocol expects a 10s heartbeat. We send "PING" (text), server
# replies "PONG". This is *separate* from the WS protocol-level ping/pong
# frames — Polymarket uses application-level strings.
PING_INTERVAL_SEC = 10.0
PING_TEXT = "PING"
PONG_TEXT = "PONG"

# If we go this long without ANY data message (book / price_change / etc.),
# treat the connection as zombied and force a reconnect. Empirically the
# server can stay open + respond to PING but stop streaming, sometimes for
# hours. nautilus uses 60s for the market channel; we match.
DATA_IDLE_TIMEOUT_SEC = 60.0

# Reconnect backoff cap. After enough failures we cap to avoid hammering.
RECONNECT_BACKOFF_MAX_SEC = 30.0


class ConnectionEvent(str, Enum):
    """Lifecycle events the pool / metrics layer wants to observe."""
    CONNECTED = "connected"
    SUBSCRIBED = "subscribed"
    DISCONNECTED = "disconnected"
    RECONNECTING = "reconnecting"
    WATCHDOG_TIMEOUT = "watchdog_timeout"  # idle too long, forcing reconnect
    CLOSED = "closed"  # final, not retrying


# A parsed Polymarket message: usually a dict, but the initial subscription
# response is a list of book snapshots.
ParsedMessage = dict | list

MessageHandler = Callable[
    [tuple[str, ...], bytes, ParsedMessage, int, int],
    Awaitable[None],
]

# Optional callback for connection lifecycle (logging / metrics / persistence
# of "I reconnected" markers in the WAL).
EventHandler = Callable[
    [int, ConnectionEvent, dict],  # conn_id, event, extra context
    Awaitable[None],
]

def asset_ids_from_parsed(parsed: ParsedMessage) -> tuple[str, ...]:
    """
    Extract all asset_ids referenced by an already-parsed message.

    Returns a tuple (possibly empty, possibly multiple). Empty means we
    couldn't identify any asset — either an unrecognized shape or a
    message type we don't route on.

    Returns deduplicated, order-preserved asset_ids.
    """
    seen: list[str] = []

    def _from_obj(obj: dict) -> None:
        # Top-level asset_id covers most types
        top = obj.get("asset_id")
        if isinstance(top, str) and top and top not in seen:
            seen.append(top)
        # price_change has a price_changes[] array
        pcs = obj.get("price_changes")
        if isinstance(pcs, list):
            for pc in pcs:
                if isinstance(pc, dict):
                    aid = pc.get("asset_id")
                    if isinstance(aid, str) and aid and aid not in seen:
                        seen.append(aid)

    if isinstance(parsed, list):
        for obj in parsed:
            if isinstance(obj, dict):
                _from_obj(obj)
    elif isinstance(parsed, dict):
        _from_obj(parsed)

    return tuple(seen)


class WSConnection:
    """
    A single managed WS connection.

    Run with ``await conn.run()`` — this blocks until ``conn.stop()`` is
    called or the connection is asked to close terminally. Inside it loops
    over (connect → subscribe → receive → on disconnect, retry).

    Subscription set is mutable mid-connection. Use ``add_subscriptions(...)``
    and ``remove_subscriptions(...)`` from outside; the changes apply on the
    current connection if it's live (sending an incremental subscribe message)
    and persist across reconnects (by being the source of truth).
    """

    def __init__(
        self,
        conn_id: int,
        on_message: MessageHandler,
        on_event: EventHandler | None = None,
    ) -> None:
        self._conn_id = conn_id
        self._url = WS_URL
        self._ping_interval = PING_INTERVAL_SEC
        self._data_idle_timeout = DATA_IDLE_TIMEOUT_SEC

        self._on_message = on_message
        self._on_event = on_event

        # Subscription state (source of truth; survives reconnects)
        self._subscriptions: set[str] = set()
        # Lock guards mutations to _subscriptions and the subscribe-send path
        self._sub_lock = asyncio.Lock()

        self._stop_requested = False
        self._force_disconnect = asyncio.Event()
        # Tracks last data-message receive time for watchdog. Bumped on any non-PONG inbound message.
        self._last_data_msg_ts: float = 0.0

        # msgspec is dramatically faster than stdlib json for our payload size.
        self._json_decoder = msgspec.json.Decoder()

        # Live connection (None if not currently connected)
        self._ws: ClientConnection | None = None

    @property
    def conn_id(self) -> int:
        return self._conn_id

    @property
    def subscription_count(self) -> int:
        return len(self._subscriptions)

    def has_subscription(self, asset_id: str) -> bool:
        return asset_id in self._subscriptions

    def stop(self) -> None:
        """Signal the connection to terminate at the next opportunity."""
        self._stop_requested = True
        self._force_disconnect.set()

    async def add_subscriptions(self, asset_ids: list[str]) -> None:
        """
        Add asset_ids to this connection's subscription set.

        If we're currently connected, also sends an incremental subscribe
        message so the new assets start streaming immediately. If not
        connected yet, just records them; they'll be in the initial subscribe
        message when we (re)connect.

        Idempotent: re-adding an existing asset_id is a no-op.
        """
        async with self._sub_lock:
            # Filter to only-new ones to avoid sending duplicate sub messages
            new_ones = [a for a in asset_ids if a not in self._subscriptions]
            if not new_ones:
                return
            self._subscriptions.update(new_ones)

            if self._ws is not None and not self._ws.close_code:
                # Send incremental subscribe. Polymarket distinguishes:
                # - Initial subscription (sent right after connect, no
                #   "operation" field).
                # - Incremental subscription (existing connection, includes
                #   `operation: "subscribe"`).
                # We're in the latter case here.
                msg = {
                    "assets_ids": new_ones,
                    "operation": "subscribe",
                    # custom_feature_enabled is set on the initial msg only;
                    # the connection-level flag persists. No need to repeat.
                }
                try:
                    await self._ws.send(msgspec.json.encode(msg).decode())
                except ConnectionClosed:
                    # Will be re-sent in the next reconnect's full subscribe.
                    logger.debug("conn %d: incremental subscribe lost (closed)", self._conn_id)

    async def remove_subscriptions(self, asset_ids: list[str]) -> None:
        """
        Remove asset_ids from this connection's set.

        Note: Polymarket's WS does not reliably honor unsubscribe — see the
        live test in scripts/. We send the unsubscribe message anyway (no
        harm) and remove from our tracking. If the server keeps sending data
        for unsubscribed assets, the pool will drop those messages by the
        asset_id NOT being in any connection's set anymore.
        """
        async with self._sub_lock:
            removed = [a for a in asset_ids if a in self._subscriptions]
            if not removed:
                return
            self._subscriptions.difference_update(removed)

            if self._ws is not None and not self._ws.close_code:
                msg = {"assets_ids": removed, "operation": "unsubscribe"}
                try:
                    await self._ws.send(msgspec.json.encode(msg).decode())
                except ConnectionClosed:
                    logger.debug("conn %d: remove subscribe lost (closed)", self._conn_id)
                    pass  # we're not subscribed anymore anyway


    async def run(self) -> None:
        """
        Main loop: connect → run → on disconnect retry. Returns when
        ``stop()`` has been called (after current iteration completes).

        Each iteration of the outer loop is one connection lifetime. Failures
        are logged and we back off before retrying. The subscription set
        persists across iterations.
        """
        attempt = 0
        while not self._stop_requested:
            try:
                await self._connect_and_run()
                # Normal end (server closed or stop requested)
                attempt = 0
            except asyncio.CancelledError:
                raise
            except Exception:
                # Includes connection refused, DNS failure, mid-stream errors.
                logger.exception("conn %d: connection error", self._conn_id)
                attempt += 1

            if self._stop_requested:
                break

            # Back off before reconnect
            backoff = self._compute_backoff(attempt)
            await self._emit_event(
                ConnectionEvent.RECONNECTING, {"attempt": attempt, "backoff_sec": backoff}
            )
            try:
                await asyncio.sleep(backoff)
            except asyncio.CancelledError:
                raise

        await self._emit_event(ConnectionEvent.CLOSED, {})

    async def _connect_and_run(self) -> None:
        """One connection's lifetime."""
        # ``ping_interval=None`` disables the websockets library's WS-level
        # ping (we use our own application-level "PING"/"PONG" strings).
        async with websockets.connect(
            self._url,
            ping_interval=None,
            close_timeout=5,
        ) as ws:
            self._ws = ws
            self._force_disconnect.clear()
            self._last_data_msg_ts = asyncio.get_event_loop().time()

            await self._emit_event(ConnectionEvent.CONNECTED, {})

            # Send initial subscription. If subscriptions is empty (rare; pool
            # creates a connection slightly before adding), we still need to
            # send something, so we send an empty asset list.
            async with self._sub_lock:
                initial_subs = list(self._subscriptions)

            init_msg = {
                "type": "market",
                "assets_ids": initial_subs,
                # KEY: custom_feature_enabled controls whether we also receive
                # new_market, market_resolved, and best_bid_ask events. We
                # always want these.
                "custom_feature_enabled": True,
            }
            await ws.send(msgspec.json.encode(init_msg).decode())
            await self._emit_event(
                ConnectionEvent.SUBSCRIBED, {"asset_count": len(initial_subs)}
            )

            # Spawn ping + watchdog tasks for the duration of this connection
            self._ping_task = asyncio.create_task(self._ping_loop(ws))
            self._watchdog_task = asyncio.create_task(self._watchdog_loop())

            try:
                await self._receive_loop(ws)
            finally:
                # Tear down side tasks
                for t in (self._ping_task, self._watchdog_task):
                    if t and not t.done():
                        t.cancel()
                # Wait for them to actually finish, ignoring cancel
                for t in (self._ping_task, self._watchdog_task):
                    if t:
                        try:
                            await t
                        except (asyncio.CancelledError, Exception):
                            pass
                self._ping_task = None
                self._watchdog_task = None
                self._ws = None

            await self._emit_event(ConnectionEvent.DISCONNECTED, {})

    async def _receive_loop(self, ws: ClientConnection) -> None:
        """
        Read messages until disconnection.

        Filters out PONG (heartbeat reply) since the watchdog only cares
        about *data* idleness. Bumps last_data_msg_ts on any other message
        and forwards via on_message.
        """
        loop = asyncio.get_event_loop()
        while True:
            # Race the receive against an external disconnect signal: 1.watchdog 2.stop() -> outside stop request
            # Using a regular ws.recv() with a wait_for is cleaner than a long poll loop.
            recv_task = asyncio.create_task(ws.recv())
            disconnect_task = asyncio.create_task(self._force_disconnect.wait())

            done, pending = await asyncio.wait(
                {recv_task, disconnect_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for p in pending:
                p.cancel()
                try:
                    await p
                except (asyncio.CancelledError, Exception):
                    pass

            if disconnect_task in done:
                # External shutdown or watchdog asked us to disconnect.
                # Closing the ws ends the outer ``async with``.
                try:
                    await ws.close(code=1000)
                except Exception:
                    pass
                return

            # recv_task completed: either a message or an error
            try:
                raw = recv_task.result()
            except ConnectionClosed:
                logger.info("conn %d: server closed connection", self._conn_id)
                return

            # ts_recv is set as close to wire arrival as we can get it. We
            # record it BEFORE any further work (parsing, callback) so it
            # accurately represents arrival time for replay/research use.
            ts_recv = now_ns()

            assert isinstance(raw, bytes)

            # PONG: don't bump data idle timer, don't forward
            if raw == PONG_TEXT:
                continue

            # Anything else counts as data activity for the watchdog.
            self._last_data_msg_ts = loop.time()

            try:
                msg = self._json_decoder.decode(raw)
            except msgspec.DecodeError:
                msg = None

            # Extract asset_ids for the dispatcher. May be empty (we don't
            # know how to route) — we still forward, with empty tuple,
            # because the WAL wants every byte regardless.
            asset_ids = (
                asset_ids_from_parsed(msg) if msg is not None else ()
            )

            try:
                await self._on_message(asset_ids, raw, msg, ts_recv, self._conn_id)
            except Exception:
                logger.exception("conn %d: on_message handler raised", self._conn_id)

    async def _ping_loop(self, ws: ClientConnection) -> None:
        """Send PING every ``ping_interval`` seconds."""
        try:
            while True:
                await asyncio.sleep(self._ping_interval)
                try:
                    await ws.send(PING_TEXT)
                except ConnectionClosed:
                    return
        except asyncio.CancelledError:
            raise


    async def _watchdog_loop(self) -> None:
        """
        Force-reconnect if data has been silent for too long.

        Polymarket has a documented failure mode where the connection stays
        OPEN, PING/PONG works, but no data messages flow — sometimes for
        hours. Nothing in the WS protocol detects this. Only a data-flow
        watchdog catches it.

        Checks every ``ping_interval`` seconds (cheap; piggy-backs on the
        cadence we already use).
        """
        loop = asyncio.get_running_loop()
        try:
            while True:
                await asyncio.sleep(self._ping_interval)
                idle_for = loop.time() - self._last_data_msg_ts
                if idle_for > self._data_idle_timeout:
                    logger.warning(
                        "conn %d: watchdog timeout (idle %.0fs > %.0fs), forcing reconnect",
                        self._conn_id,
                        idle_for,
                        self._data_idle_timeout,
                    )
                    await self._emit_event(
                        ConnectionEvent.WATCHDOG_TIMEOUT,
                        {"idle_sec": idle_for},
                    )
                    self._force_disconnect.set()
                    return
        except asyncio.CancelledError:
            raise

    async def _emit_event(self, event: ConnectionEvent, extra: dict) -> None:
        if self._on_event is None:
            return
        try:
            await self._on_event(self._conn_id, event, extra)
        except Exception:
            logger.exception("conn %d: on_event handler raised", self._conn_id)

    @staticmethod
    def _compute_backoff(attempt: int) -> float:
        """1s, 2s, 4s, 8s, 16s, 30s, 30s, ... with full jitter."""
        if attempt <= 0:
            return 0.0
        base = min(2.0 ** (attempt - 1), RECONNECT_BACKOFF_MAX_SEC)
        return random.uniform(0, base)