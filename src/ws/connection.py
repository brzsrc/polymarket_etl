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
    # in connected state
    CONNECTED = "connected"
    # msg just sent to ws
    SUBSCRIBED = "subscribed"
    # disconnected which gonna cause retrying
    DISCONNECTED = "disconnected"
    # reconnecting
    RECONNECTING = "reconnecting"
    # idle too long, forcing reconnect
    WATCHDOG_TIMEOUT = "watchdog_timeout"
    # final, not retrying
    CLOSED = "closed"


# A parsed Polymarket message: usually a dict, but the initial subscription
# response is a list of book snapshots.
ParsedMessage = dict | list

MessageHandler = Callable[
    [tuple[str, ...], str, ParsedMessage, int, int],
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

    Polymarket schema gotcha: most events use ``asset_id`` (singular,
    string) at the top level, but ``new_market`` events use ``assets_ids``
    (plural, array). We support both.
    """
    seen: list[str] = []

    def _add(aid) -> None:
        if isinstance(aid, str) and aid and aid not in seen:
            seen.append(aid)

    def _from_obj(obj: dict) -> None:
        # Most events: top-level "asset_id" (singular, string)
        _add(obj.get("asset_id"))
        # new_market events: top-level "assets_ids" (plural, array of strings)
        ass = obj.get("assets_ids")
        if isinstance(ass, list):
            for aid in ass:
                _add(aid)
        # price_change events: "price_changes" array, each with asset_id
        pcs = obj.get("price_changes")
        if isinstance(pcs, list):
            for pc in pcs:
                if isinstance(pc, dict):
                    _add(pc.get("asset_id"))

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
        ws_url: str = WS_URL,
        ping_interval_sec: float = PING_INTERVAL_SEC,
        data_idle_timeout_sec: float = DATA_IDLE_TIMEOUT_SEC,
    ) -> None:
        self._conn_id = conn_id
        self._url = ws_url
        self._ping_interval = ping_interval_sec
        self._data_idle_timeout = data_idle_timeout_sec

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


    async def run(self, custom_feature_enabled: bool=False) -> None:
        """
        Main loop: connect → run one connection → on disconnect retry.
        Returns when ``stop()`` has been called.

        Each iteration of the outer ``while`` is one connection lifetime.
        Within a single iteration:
        - websockets.connect manages the TCP/TLS/WS handshake
        - we send the subscribe message (custom_feature_enabled=True)
        - three concurrent tasks run for the connection's life:
            * ping_loop      — sends "PING" every 10s
            * watchdog_loop  — sets _force_disconnect if data is idle too long
            * receive_loop   — reads frames, dispatches to handler
        - we await on either receive_loop finishing OR _force_disconnect
          being set, whichever happens first.

        Three exit categories from one connection iteration:

        1. **Graceful** — server cycled the connection cleanly (long-poll
           timeout, scheduled rotation), OR our stop() was called. The
           connection ended without a real failure: ``attempt`` is reset,
           no RECONNECTING event, no backoff. Reconnects immediately on
           next iteration (unless stop() — then we exit).

        2. **Watchdog** — the connection looked alive (PING/PONG OK) but
           no data was flowing. This is a real failure of the underlying
           stream even though no exception was raised. Treat as retry:
           ``attempt`` increments, RECONNECTING fires, backoff applied.

        3. **Exception** — TCP error, DNS failure, server 5xx during
           handshake, recv_task raising mid-stream, etc. Treated the same
           as Watchdog from the retry perspective.

        """
        attempt = 0
        while not self._stop_requested:
            try:
                self._force_disconnect.clear()

                async with websockets.connect(
                    self._url,
                    ping_interval=None,
                    close_timeout=5,
                ) as ws:
                    self._ws = ws
                    self._last_data_msg_ts = asyncio.get_event_loop().time()

                    await self._emit_event(ConnectionEvent.CONNECTED, {})

                    # Send initial subscription. If subscriptions is empty
                    # (rare; pool creates a connection slightly before
                    # adding), we still need to send something — empty list.
                    async with self._sub_lock:
                        initial_subs = list(self._subscriptions)

                    init_msg = {
                        "type": "market",
                        "assets_ids": initial_subs,
                        # custom_feature_enabled controls whether we also
                        # receive new_market, market_resolved, and
                        # best_bid_ask events. We always want these.
                        "custom_feature_enabled": custom_feature_enabled,
                    }
                    await ws.send(msgspec.json.encode(init_msg).decode())
                    await self._emit_event(
                        ConnectionEvent.SUBSCRIBED,
                        {"asset_count": len(initial_subs)},
                    )

                    # Spawn ping + watchdog + receive tasks.
                    self._ping_task = asyncio.create_task(self._ping_loop(ws))
                    self._watchdog_task = asyncio.create_task(self._watchdog_loop())

                    recv_task = asyncio.create_task(self._receive_loop(ws))
                    disconnect_task = asyncio.create_task(
                        self._force_disconnect.wait()
                    )

                    try:
                        done, _pending = await asyncio.wait(
                            {recv_task, disconnect_task},
                            return_when=asyncio.FIRST_COMPLETED,
                        )

                        if disconnect_task in done:
                            # External disconnect (stop() or watchdog)
                            try:
                                await ws.close(code=1000)
                            # any error happends during ws.close shouldn't matter
                            except Exception:
                                pass
                            try:
                                await recv_task
                            except Exception:
                                logger.exception(
                                    "conn %d: receive_loop raised abnormal errors during shutdown",
                                    self._conn_id,
                                )
                        else:
                            # recv_task finished first. If it raised,
                            # propagate to the outer except so we treat
                            # this iteration as a failure.
                            exc = recv_task.exception()
                            if exc is not None:
                                raise exc
                    finally:
                        # Tear down all side tasks. Cancel is idempotent.
                        for t in (recv_task, disconnect_task,
                                  self._ping_task, self._watchdog_task):
                            if t and not t.done():
                                t.cancel()
                        for t in (recv_task, disconnect_task,
                                  self._ping_task, self._watchdog_task):
                            if t:
                                try:
                                    await t
                                except (asyncio.CancelledError, Exception):
                                    pass
                        self._ping_task = None
                        self._watchdog_task = None
                        self._ws = None

                    await self._emit_event(ConnectionEvent.DISCONNECTED, {})

                # ---- end of one connection (with 1 ws:) —> decide retry policy ----
                # - _force_disconnect set + _stop_requested → stop() (graceful)
                # - _force_disconnect set + NOT _stop_requested → watchdog
                # - _force_disconnect not set → server closed cleanly
                if (
                    self._force_disconnect.is_set()
                    and not self._stop_requested
                ):
                    # Watchdog detected a stalled stream — real failure.
                    attempt += 1
                else:
                    # Server cycled us, or stop() was called.
                    attempt = 0
            # somehow receive/ping/watchdog_loop get cancelled
            except asyncio.CancelledError:
                raise
            except Exception:
                # Network error, protocol error, etc.
                logger.exception("conn %d: connection error", self._conn_id)
                attempt += 1

            if self._stop_requested:
                break

            # Only back off and emit RECONNECTING on actual failures.
            if attempt > 0:
                backoff = self._compute_backoff(attempt)
                await self._emit_event(
                    ConnectionEvent.RECONNECTING,
                    {"attempt": attempt, "backoff_sec": backoff},
                )
                await asyncio.sleep(backoff)

        await self._emit_event(ConnectionEvent.CLOSED, {})

    async def _receive_loop(self, ws: ClientConnection) -> None:
        """
        Read messages until the connection ends.

        Filters out PONG (heartbeat reply) since the watchdog only cares
        about *data* idleness. Bumps ``_last_data_msg_ts`` on any other
        message and forwards via on_message.

        Returns normally when the server closes the connection (or our own
        ``ws.close()`` from the outer task triggers ConnectionClosed via
        the in-flight recv). External disconnect signals (stop / watchdog)
        are handled in ``_connect_and_run`` — this function is just a
        straight read loop.
        """
        loop = asyncio.get_running_loop()
        while True:
            try:
                raw = await ws.recv()
            except ConnectionClosed:
                logger.info("conn %d: server closed connection", self._conn_id)
                return

            # ts_recv is set as close to wire arrival as we can get it. We
            # record it BEFORE any further work (parsing, callback) so it
            # accurately represents arrival time for replay/research use.
            ts_recv = now_ns()

            assert isinstance(raw, str)

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

            await self._on_message(asset_ids, raw, msg, ts_recv, self._conn_id)



    async def _ping_loop(self, ws: ClientConnection) -> None:
        """Send PING every ``ping_interval`` seconds."""
        while True:
            await asyncio.sleep(self._ping_interval)
            try:
                await ws.send(PING_TEXT)
            except ConnectionClosed:
                return



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
        while True:
            await asyncio.sleep(self._ping_interval)
            idle_for = loop.time() - self._last_data_msg_ts
            if idle_for > self._data_idle_timeout:
                logger.warning(
                    "conn %d: watchdog timeout (idle %.1fs > %.1fs), force disconnect then reconnect",
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