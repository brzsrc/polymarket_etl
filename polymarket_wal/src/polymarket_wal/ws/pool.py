"""
Connection pool for Polymarket WS subscriptions.

Polymarket caps each WS connection at 500 instruments (undocumented, see
nautilus_trader's polymarket adapter where this constraint is encoded). We
target 200 per connection to leave headroom and to avoid the "around 250 the
server gets weird" anecdotal danger zone.

Responsibilities:

- Sharding: when a new asset_id is added, place it on a connection that
  isn't full. First-fit (not consistent hashing) — this matches nautilus's
  proven approach and keeps the implementation small. The downside (a
  remove + add can re-shard the new asset to a different connection) is
  irrelevant for our use case (we only care about message delivery, not
  connection affinity).

- Reference counting: the discovery layer might call add_subscription
  multiple times for the same asset_id (e.g. WS new_market arrives, then
  Gamma poll arrives 30s later both telling us about it). The pool dedupes
  with a ref counter. Real unsub only fires when the count drops to zero.

- Unified message dispatch: every connection's on_message bubbles up to
  the pool's single ``on_message`` callback. Callers only need to wire one
  handler regardless of how many connections are live.

Out of scope (handled by callers):

- Persisting messages to disk (that's the WAL writer in Phase 3)
- Retrying message handler failures (handler is expected to be best-effort)
- Backpressure (handler is awaited inline, slow handler = slow ingest;
  the WAL writer has to be fast)
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any

from .connection import (
    ConnectionEvent,
    EventHandler,
    MessageHandler,
    ParsedMessage,
    WSConnection,
)

logger = logging.getLogger(__name__)

# Polymarket cap is 500/connection. We use 200 to:
# - leave 60% headroom for protocol-level overhead variance
# - stay below ~250 where some users have reported zombie-connection issues
# - match nautilus_trader's tested default
DEFAULT_MAX_PER_CONNECTION = 200


class WSPool:
    """
    Multi-connection pool for WS market subscriptions.

    Use:

        async def on_msg(asset_ids, raw, ts_recv, conn_id):
            await wal_writer.write(asset_ids, raw, ts_recv)

        pool = WSPool(on_message=on_msg)
        await pool.start()
        await pool.add_subscriptions(["asset1", "asset2", ...])
        # ... runs forever ...
        await pool.stop()
    """

    def __init__(
        self,
        on_message: MessageHandler,
        on_event: EventHandler | None = None,
        max_per_connection: int = DEFAULT_MAX_PER_CONNECTION,
    ) -> None:
        if max_per_connection <= 0 or max_per_connection > 500:
            raise ValueError("max_per_connection must be in (0, 500]")
        self._on_message_external = on_message
        self._on_event_external = on_event
        self._max_per_connection = max_per_connection

        # asset_id -> ref count (how many independent callers want it)
        self._refcount: dict[str, int] = {}
        # asset_id -> conn_id (which connection currently carries it)
        self._asset_to_conn: dict[str, int] = {}
        # conn_id -> WSConnection
        self._connections: dict[int, WSConnection] = {}
        # conn_id -> the asyncio.Task running its run() loop
        self._connection_tasks: dict[int, asyncio.Task] = {}
        self._next_conn_id = 0

        # Guards all of the above. Subscribe operations are not on the
        # message hot path, so a single lock is fine and simpler than
        # finer-grained locking.
        self._lock = asyncio.Lock()

        self._running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Mark the pool as running. Connections are created lazily on add."""
        if self._running:
            return
        self._running = True
        logger.info(
            "ws pool started (max_per_connection=%d)", self._max_per_connection
        )

    async def stop(self) -> None:
        """Stop all connections. Awaits clean shutdown of each."""
        if not self._running:
            return
        self._running = False

        async with self._lock:
            for conn in self._connections.values():
                conn.stop()
            tasks = list(self._connection_tasks.values())

        # Wait for all run-loops to exit. We don't hold the lock here so the
        # connections' own cleanup paths can call back into events freely.
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        async with self._lock:
            self._connections.clear()
            self._connection_tasks.clear()
            # Note: we keep _refcount and _asset_to_conn in case the pool is
            # restarted. But a fresh start() creates new connections, so we
            # clear those mappings too — they're tied to conn_ids that no
            # longer exist.
            self._asset_to_conn.clear()

        logger.info("ws pool stopped")

    # ------------------------------------------------------------------
    # Subscription management (ref-counted, sharded)
    # ------------------------------------------------------------------

    async def add_subscriptions(self, asset_ids: list[str]) -> None:
        """
        Add (or bump refcount on) the given asset_ids.

        For each asset:
        - If already subscribed (refcount > 0): just increment refcount,
          no WS work.
        - Else: pick a connection (first-fit), tell it to subscribe, and
          record the assignment.
        """
        if not self._running:
            raise RuntimeError("pool not started")
        if not asset_ids:
            return

        # Group new (refcount 0 -> 1) assets by chosen connection so we send
        # one batched subscribe per connection rather than N small ones.
        async with self._lock:
            per_conn: dict[int, list[str]] = {}
            for asset_id in asset_ids:
                cur = self._refcount.get(asset_id, 0)
                if cur > 0:
                    self._refcount[asset_id] = cur + 1
                    continue
                # Brand new — assign to a connection
                conn_id = self._pick_or_create_connection_locked()
                self._refcount[asset_id] = 1
                self._asset_to_conn[asset_id] = conn_id
                per_conn.setdefault(conn_id, []).append(asset_id)

            # Snapshot connections to send to (still under lock)
            sends = [
                (self._connections[cid], assets) for cid, assets in per_conn.items()
            ]

        # Issue actual sends OUTSIDE the lock to avoid blocking other
        # callers behind a network send. The WSConnection has its own
        # internal lock for its own sub state.
        for conn, assets in sends:
            await conn.add_subscriptions(assets)

    async def remove_subscriptions(self, asset_ids: list[str]) -> None:
        """
        Decrement refcounts; for any that hit zero, actually unsubscribe.

        Polymarket's unsubscribe is best-effort — see WSConnection docs.
        We always remove the asset from our tracking regardless, which means
        any further messages from the server for that asset will be
        forwarded with an asset_id NOT in our pool (since we don't filter
        in the dispatch path — see dispatch design note below).
        """
        if not self._running:
            return
        if not asset_ids:
            return

        async with self._lock:
            per_conn: dict[int, list[str]] = {}
            for asset_id in asset_ids:
                cur = self._refcount.get(asset_id, 0)
                if cur <= 0:
                    continue
                if cur > 1:
                    self._refcount[asset_id] = cur - 1
                    continue
                # Last reference — actually unsubscribe
                del self._refcount[asset_id]
                conn_id = self._asset_to_conn.pop(asset_id, None)
                if conn_id is not None:
                    per_conn.setdefault(conn_id, []).append(asset_id)

            sends = [
                (self._connections[cid], assets)
                for cid, assets in per_conn.items()
                if cid in self._connections
            ]

        for conn, assets in sends:
            await conn.remove_subscriptions(assets)

    # ------------------------------------------------------------------
    # Internal: connection placement
    # ------------------------------------------------------------------

    def _pick_or_create_connection_locked(self) -> int:
        """
        First-fit: return the first existing conn_id that has spare capacity.
        Otherwise create a new connection.

        Caller must hold ``self._lock``.

        Capacity check uses our own ``_asset_to_conn`` mapping rather than
        ``conn.subscription_count`` because during a batch ``add_subscriptions``
        call, we make all routing decisions inside the lock but don't
        actually call into the connection until we exit the lock. So the
        connection's internal count is stale during the loop.
        """
        # Compute current assignments per connection from our own tracking.
        # This includes assets we've routed in the current call but haven't
        # yet sent to the underlying connection.
        load: dict[int, int] = {cid: 0 for cid in self._connections}
        for cid in self._asset_to_conn.values():
            if cid in load:
                load[cid] += 1

        for conn_id in self._connections:
            if load[conn_id] < self._max_per_connection:
                return conn_id

        # All full or no connections yet — create a new one
        conn_id = self._next_conn_id
        self._next_conn_id += 1

        conn = WSConnection(
            conn_id=conn_id,
            on_message=self._dispatch_message,
            on_event=self._dispatch_event,
        )
        self._connections[conn_id] = conn

        # Spawn its run loop. We hold a reference so we can await it on stop().
        task = asyncio.create_task(conn.run(), name=f"wsconn-{conn_id}")
        self._connection_tasks[conn_id] = task
        # Add a done-callback for diagnostics — the task should only finish
        # when stop() was called, so any other completion is a bug.
        task.add_done_callback(lambda t, cid=conn_id: self._on_conn_task_done(cid, t))

        logger.info(
            "ws pool: created connection %d (total connections: %d)",
            conn_id,
            len(self._connections),
        )
        return conn_id

    def _on_conn_task_done(self, conn_id: int, task: asyncio.Task) -> None:
        """Diagnostic: a connection's run-loop ended."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error("conn %d: task ended with exception: %r", conn_id, exc)
        elif self._running:
            logger.warning("conn %d: task ended unexpectedly while pool running", conn_id)

    # ------------------------------------------------------------------
    # Internal: message + event dispatch
    # ------------------------------------------------------------------

    async def _dispatch_message(
        self,
        asset_ids: tuple[str, ...],
        raw_bytes: bytes,
        parsed: "ParsedMessage | None",
        ts_recv: datetime,
        conn_id: int,
    ) -> None:
        """
        Forward to the user-supplied on_message callback.

        Design note: we DO NOT filter messages by "is this asset still in our
        subscription set". Reasons:
        1. WAL semantics — we want to record everything we receive, even if
           we'd just removed an asset moments before. The data is real.
        2. Message types like ``new_market`` have an asset_id we never
           subscribed to (it just got created). Filtering would drop it.
        3. Polymarket's unreliable unsub means we'd be silently dropping
           legitimate messages anyway.

        The handler can apply its own filter if needed, but the default
        behavior is "forward everything".
        """
        try:
            await self._on_message_external(
                asset_ids, raw_bytes, parsed, ts_recv, conn_id
            )
        except Exception:
            logger.exception("on_message handler raised")

    async def _dispatch_event(
        self,
        conn_id: int,
        event: ConnectionEvent,
        extra: dict[str, Any],
    ) -> None:
        if self._on_event_external is None:
            return
        try:
            await self._on_event_external(conn_id, event, extra)
        except Exception:
            logger.exception("on_event handler raised")

    # ------------------------------------------------------------------
    # Introspection (mainly for tests and metrics)
    # ------------------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        """Snapshot of pool state (cheap, lock-free; eventually consistent)."""
        return {
            "running": self._running,
            "connections": len(self._connections),
            "total_subscriptions": sum(
                c.subscription_count for c in self._connections.values()
            ),
            "unique_assets": len(self._refcount),
            "per_connection": {
                cid: c.subscription_count for cid, c in self._connections.items()
            },
        }
