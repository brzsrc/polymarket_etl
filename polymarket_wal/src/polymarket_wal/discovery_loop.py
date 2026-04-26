"""
Periodic market-discovery loop (Task 1.4).

Runs a Gamma cycle every N minutes, diffs the result against the pool's
current subscription set, and applies the additions/removals.

Design notes:

- The loop is the *backup* path for new markets. The primary path is the
  WS ``new_market`` event (sub-second latency once active subscriptions
  exist on the relevant connection). The Gamma poll catches anything WS
  missed (e.g. during a reconnect).

- ``to_remove`` uses a "strike" mechanism (3 consecutive cycles missing
  before actually removing). This protects against offset-pagination
  edge cases where a single page can shift records between cycles, briefly
  hiding an asset. Without strikes, we'd flap subscriptions for assets that
  are still active.

- Each cycle persists the full set of seen markets to ``markets.jsonl``,
  giving us the metadata history (question text, endDate, volume, etc.)
  over time.

- Failure of one cycle is non-fatal — it just delays discovery by one
  interval. We log and move on.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from .gamma_client import GammaClient, GammaError
from .market_discovery import (
    DiscoveryResult,
    fetch_all_active_binary_markets,
)
from .ws.pool import WSPool

logger = logging.getLogger(__name__)


# Default cycle interval. Spec said "every 5-10 minutes" — we go with 10
# because the WS path covers the urgent cases (new_market arrives in seconds)
# and 10 min keeps Gamma load reasonable. Researchers can change at config.
DEFAULT_INTERVAL_SEC = 600.0

# Number of consecutive cycles an asset must be missing before we remove it.
# Protects against offset-pagination races where a single page may transiently
# omit an asset that's actually still active. With 3 strikes at 10min/cycle,
# false-positive removals delay by ~30 minutes worst case, which is fine —
# Polymarket doesn't reliably honor unsub anyway, so the impact of late
# removal is just "we keep getting messages we don't strictly need".
DEFAULT_REMOVAL_STRIKES = 3


# Optional callback the application can supply to react to each cycle's
# diff (e.g. for metrics or audit logging).
DiffHandler = Callable[
    ["DiscoveryDiff"], Awaitable[None]
]


@dataclass
class DiscoveryDiff:
    """What changed in one discovery cycle."""

    cycle_started_at: datetime
    cycle_finished_at: datetime
    seen_token_ids: set[str]
    added: set[str]
    """Token IDs we just subscribed to (refcount went from 0 to 1)."""
    removed: set[str]
    """Token IDs we just unsubscribed from (3-strike mechanism triggered)."""
    raw_records_seen: int
    markets_kept: int


class DiscoveryLoop:
    """
    Background task that periodically polls Gamma and updates the WSPool.

    Use:

        loop = DiscoveryLoop(gamma=gamma, pool=pool, markets_jsonl_path=...)
        await loop.start()  # spawns task; run cycle once immediately
        ...                 # (the loop runs forever)
        await loop.stop()
    """

    def __init__(
        self,
        gamma: GammaClient,
        pool: WSPool,
        markets_jsonl_path: Path | None,
        interval_sec: float = DEFAULT_INTERVAL_SEC,
        removal_strikes: int = DEFAULT_REMOVAL_STRIKES,
        on_diff: DiffHandler | None = None,
    ) -> None:
        if interval_sec <= 0:
            raise ValueError("interval_sec must be > 0")
        if removal_strikes < 1:
            raise ValueError("removal_strikes must be >= 1")
        self._gamma = gamma
        self._pool = pool
        self._markets_jsonl_path = markets_jsonl_path
        self._interval_sec = interval_sec
        self._removal_strikes = removal_strikes
        self._on_diff = on_diff

        # Source of truth for "what we currently have subscribed". Mirrors
        # the pool's view but kept here so we don't have to query the pool
        # during diff (no lock contention and we own the schedule).
        self._current_subscriptions: set[str] = set()

        # asset_id -> consecutive strikes (cycles missing). Reset to 0 on
        # any cycle where the asset reappears. Removed when the asset is
        # successfully removed.
        self._removal_strikes_count: dict[str, int] = {}

        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    @property
    def current_subscriptions(self) -> frozenset[str]:
        """Read-only snapshot of the asset_ids we believe are subscribed."""
        return frozenset(self._current_subscriptions)

    async def start(self) -> None:
        """Begin the loop. Returns immediately; first cycle runs in the
        background. To wait for the first cycle to finish, see
        ``run_one_cycle()`` which is the building block we expose for tests."""
        if self._task is not None and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_forever(), name="discovery-loop")

    async def stop(self) -> None:
        """Signal stop and wait for the loop task to finish its current cycle."""
        self._stop_event.set()
        if self._task is not None:
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _run_forever(self) -> None:
        """Run cycles back-to-back at ``interval_sec`` cadence.

        We run the first cycle immediately on start, then wait between each.
        If a cycle takes longer than the interval (Gamma slow / many
        markets), we don't try to catch up — next cycle runs immediately
        and the schedule slips. This is the simplest and safest behavior.
        """
        while not self._stop_event.is_set():
            try:
                await self.run_one_cycle()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Any unexpected error (bug / Gamma down / etc.). Sleep and
                # try again next cycle.
                logger.exception("discovery cycle failed; will retry next interval")

            # Sleep until next cycle, but wake early if stop requested.
            try:
                await asyncio.wait_for(self._stop_event.wait(), self._interval_sec)
            except asyncio.TimeoutError:
                continue  # interval elapsed normally
            else:
                break  # stop was set

    async def run_one_cycle(self) -> DiscoveryDiff | None:
        """
        Run a single discovery cycle and apply diff to the pool.

        Returns the ``DiscoveryDiff`` on success, or ``None`` if the Gamma
        fetch failed (in which case nothing was changed). Never raises on
        Gamma errors — those are logged and swallowed; the next cycle will
        retry.
        """
        cycle_started_at = datetime.now(timezone.utc)
        try:
            result: DiscoveryResult = await fetch_all_active_binary_markets(
                self._gamma,
                markets_jsonl_path=self._markets_jsonl_path,
            )
        except GammaError as e:
            # Mid-pagination failure — half-baked data is worse than no
            # data because it would trigger spurious to_remove. Skip cycle.
            logger.warning("discovery cycle aborted (Gamma error): %s", e)
            return None

        seen = result.token_ids

        # Compute diff
        added = seen - self._current_subscriptions
        suspect_remove = self._current_subscriptions - seen

        # Reset strikes for anything we saw this cycle
        for asset in seen:
            self._removal_strikes_count.pop(asset, None)

        # Bump strikes for missing assets, and decide which actually go
        confirmed_remove: set[str] = set()
        for asset in suspect_remove:
            new_count = self._removal_strikes_count.get(asset, 0) + 1
            if new_count >= self._removal_strikes:
                confirmed_remove.add(asset)
                # Don't keep tracking after we remove — the asset will be
                # re-added by name if it reappears, which resets state.
                self._removal_strikes_count.pop(asset, None)
            else:
                self._removal_strikes_count[asset] = new_count

        # Apply changes to the pool
        if added:
            await self._pool.add_subscriptions(list(added))
        if confirmed_remove:
            await self._pool.remove_subscriptions(list(confirmed_remove))

        # Update our local view
        self._current_subscriptions.update(added)
        self._current_subscriptions.difference_update(confirmed_remove)

        cycle_finished_at = datetime.now(timezone.utc)
        diff = DiscoveryDiff(
            cycle_started_at=cycle_started_at,
            cycle_finished_at=cycle_finished_at,
            seen_token_ids=seen,
            added=added,
            removed=confirmed_remove,
            raw_records_seen=result.raw_records_seen,
            markets_kept=len(result.markets),
        )

        logger.info(
            "discovery diff: +%d added, -%d removed (%d still pending strikes), "
            "%d total subs, took %.1fs",
            len(added),
            len(confirmed_remove),
            len(self._removal_strikes_count),
            len(self._current_subscriptions),
            (cycle_finished_at - cycle_started_at).total_seconds(),
        )

        if self._on_diff is not None:
            try:
                await self._on_diff(diff)
            except Exception:
                logger.exception("on_diff handler raised")

        return diff
