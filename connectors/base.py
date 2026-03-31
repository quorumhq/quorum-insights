"""
Base connector interface for Quorum Insights.

All connectors (PostHog, Segment, Langfuse, warehouse) implement this
interface to normalize source events into the canonical InsightEvent format.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import AsyncIterator

from schema.event import InsightEvent


@dataclass
class SyncState:
    """Tracks connector sync progress for incremental imports."""

    last_synced_at: datetime | None = None
    last_event_timestamp: datetime | None = None
    cursor: str | None = None
    events_synced: int = 0
    errors: list[str] = field(default_factory=list)

    def mark_synced(self, event_timestamp: datetime) -> None:
        self.last_event_timestamp = event_timestamp
        self.events_synced += 1

    def mark_complete(self) -> None:
        self.last_synced_at = datetime.now(timezone.utc)

    def add_error(self, error: str) -> None:
        self.errors.append(error)


@dataclass
class ConnectorConfig:
    """Base configuration for all connectors."""

    tenant_id: str
    batch_size: int = 1000
    lookback_days: int = 90


class BaseConnector(ABC):
    """
    Abstract base for all Insights data connectors.

    Subclasses implement:
    - validate(): check credentials and connectivity
    - fetch_events(): yield batches of normalized InsightEvents
    - get_sync_state() / save_sync_state(): track incremental progress
    """

    def __init__(self, config: ConnectorConfig) -> None:
        self.config = config

    @abstractmethod
    async def validate(self) -> bool:
        """
        Check credentials and connectivity.
        Returns True if the connector can reach the source.
        Raises on configuration errors.
        """
        ...

    @abstractmethod
    async def fetch_events(
        self,
        after: datetime | None = None,
        before: datetime | None = None,
    ) -> AsyncIterator[list[InsightEvent]]:
        """
        Yield batches of normalized InsightEvents.

        Args:
            after: Only fetch events after this timestamp (for incremental sync).
                   If None, uses lookback_days from config.
            before: Only fetch events before this timestamp.
                    If None, uses now.

        Yields:
            Lists of InsightEvent (batch_size per batch).
        """
        ...

    @abstractmethod
    async def get_sync_state(self) -> SyncState:
        """Load the last sync state for this connector."""
        ...

    @abstractmethod
    async def save_sync_state(self, state: SyncState) -> None:
        """Persist the sync state for incremental imports."""
        ...

    async def sync(self) -> SyncState:
        """
        Run a full sync cycle:
        1. Load last sync state
        2. Fetch events since last sync (or lookback_days)
        3. Save sync state
        """
        state = await self.get_sync_state()

        after = state.last_event_timestamp
        if after is None:
            from datetime import timedelta
            after = datetime.now(timezone.utc) - timedelta(days=self.config.lookback_days)

        async for batch in self.fetch_events(after=after):
            for event in batch:
                state.mark_synced(event.timestamp)
            yield batch

        state.mark_complete()
        await self.save_sync_state(state)
