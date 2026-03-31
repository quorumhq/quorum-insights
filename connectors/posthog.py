"""
PostHog connector for Quorum Insights.

Uses the PostHog Query API (HogQL) to fetch events — the Events API is deprecated.
Supports PostHog Cloud (us.posthog.com, eu.posthog.com) and self-hosted instances.

Sync approach:
- Timestamp-based pagination (not OFFSET — per PostHog best practices)
- Fetches in batches of batch_size (default 1000, max 50000 per PostHog limit)
- Incremental: tracks last_event_timestamp, only fetches newer events
- First sync: lookback_days (default 90)

Auth: Personal API key with query:read scope.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, AsyncIterator

import httpx

from connectors.base import BaseConnector, ConnectorConfig, SyncState
from connectors.posthog_mapping import map_posthog_event
from schema.event import InsightEvent

logger = logging.getLogger(__name__)


@dataclass
class PostHogConfig(ConnectorConfig):
    """PostHog-specific connector configuration."""

    api_key: str = ""
    host: str = "https://us.i.posthog.com"  # or https://eu.i.posthog.com, or self-hosted
    project_id: str = ""
    batch_size: int = 1000  # max 50000 per PostHog limit
    lookback_days: int = 90
    # Path to persist sync state (simple JSON file for now)
    state_path: str = ".posthog_sync_state.json"


# HogQL query to fetch raw events with all properties.
# Uses timestamp-based pagination (not OFFSET).
_EVENTS_QUERY = """
SELECT
    uuid,
    event,
    distinct_id,
    timestamp,
    properties
FROM events
WHERE timestamp > toDateTime('{after}')
  AND timestamp <= toDateTime('{before}')
ORDER BY timestamp ASC
LIMIT {limit}
"""


class PostHogConnector(BaseConnector):
    """
    Fetches events from PostHog via the Query API (HogQL).

    Usage:
        config = PostHogConfig(
            tenant_id="my-tenant",
            api_key="phx_...",
            project_id="12345",
        )
        connector = PostHogConnector(config)

        if await connector.validate():
            async for batch in connector.sync():
                # batch is a list of InsightEvent
                store_events(batch)
    """

    def __init__(self, config: PostHogConfig) -> None:
        super().__init__(config)
        self.config: PostHogConfig = config
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.config.host,
                headers={
                    "Authorization": f"Bearer {self.config.api_key}",
                    "Content-Type": "application/json",
                },
                timeout=30.0,
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def validate(self) -> bool:
        """
        Check credentials by making a simple query.
        Returns True if connection works.
        """
        client = await self._get_client()
        try:
            resp = await client.post(
                f"/api/projects/{self.config.project_id}/query/",
                json={
                    "query": {
                        "kind": "HogQLQuery",
                        "query": "SELECT count() FROM events WHERE timestamp > now() - INTERVAL 1 HOUR",
                    },
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                count = data.get("results", [[0]])[0][0]
                logger.info(f"PostHog connected. {count} events in last hour.")
                return True
            elif resp.status_code == 401:
                logger.error("PostHog auth failed. Check API key.")
                return False
            elif resp.status_code == 404:
                logger.error(f"PostHog project {self.config.project_id} not found.")
                return False
            else:
                logger.error(f"PostHog returned {resp.status_code}: {resp.text[:200]}")
                return False
        except httpx.ConnectError as e:
            logger.error(f"Cannot reach PostHog at {self.config.host}: {e}")
            return False

    async def _run_query(self, hogql: str) -> list[list[Any]]:
        """Execute a HogQL query and return results."""
        client = await self._get_client()
        resp = await client.post(
            f"/api/projects/{self.config.project_id}/query/",
            json={
                "query": {
                    "kind": "HogQLQuery",
                    "query": hogql,
                },
            },
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])

    def _parse_row(self, row: list[Any]) -> dict[str, Any]:
        """
        Parse a HogQL result row into a PostHog event dict.
        Row columns: [uuid, event, distinct_id, timestamp, properties]
        """
        props = row[4]
        if isinstance(props, str):
            try:
                props = json.loads(props)
            except json.JSONDecodeError:
                props = {}

        return {
            "uuid": row[0],
            "event": row[1],
            "distinct_id": row[2],
            "timestamp": row[3],
            "properties": props or {},
        }

    async def fetch_events(
        self,
        after: datetime | None = None,
        before: datetime | None = None,
    ) -> AsyncIterator[list[InsightEvent]]:
        """
        Fetch events from PostHog using timestamp-based pagination.

        Yields batches of InsightEvent (batch_size per batch).
        """
        if after is None:
            after = datetime.now(timezone.utc) - timedelta(days=self.config.lookback_days)
        if before is None:
            before = datetime.now(timezone.utc)

        current_after = after
        total_fetched = 0

        while current_after < before:
            # Format timestamps for HogQL
            after_str = current_after.strftime("%Y-%m-%d %H:%M:%S")
            before_str = before.strftime("%Y-%m-%d %H:%M:%S")

            query = _EVENTS_QUERY.format(
                after=after_str,
                before=before_str,
                limit=self.config.batch_size,
            )

            try:
                rows = await self._run_query(query)
            except httpx.HTTPStatusError as e:
                logger.error(f"PostHog query failed: {e.response.status_code} {e.response.text[:200]}")
                break
            except httpx.ConnectError as e:
                logger.error(f"PostHog connection error: {e}")
                break

            if not rows:
                logger.info(f"PostHog sync complete. {total_fetched} events fetched.")
                break

            # Parse and map
            batch: list[InsightEvent] = []
            last_timestamp = current_after

            for row in rows:
                raw_event = self._parse_row(row)
                try:
                    event = map_posthog_event(raw_event, self.config.tenant_id)
                    batch.append(event)
                    last_timestamp = event.timestamp
                except Exception as e:
                    logger.warning(f"Failed to map event {raw_event.get('uuid', '?')}: {e}")

            total_fetched += len(batch)
            logger.info(f"Fetched {len(batch)} events (total: {total_fetched}), up to {last_timestamp}")

            if batch:
                yield batch

            # Advance cursor: move past the last timestamp we saw
            # (timestamp-based pagination, not OFFSET)
            current_after = last_timestamp

            # If we got fewer results than batch_size, we've reached the end
            if len(rows) < self.config.batch_size:
                logger.info(f"PostHog sync complete. {total_fetched} events total.")
                break

    async def get_sync_state(self) -> SyncState:
        """Load sync state from a JSON file."""
        state_file = Path(self.config.state_path)
        if state_file.exists():
            try:
                data = json.loads(state_file.read_text())
                return SyncState(
                    last_synced_at=datetime.fromisoformat(data["last_synced_at"]) if data.get("last_synced_at") else None,
                    last_event_timestamp=datetime.fromisoformat(data["last_event_timestamp"]) if data.get("last_event_timestamp") else None,
                    events_synced=data.get("events_synced", 0),
                )
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Corrupt sync state file, starting fresh: {e}")
        return SyncState()

    async def save_sync_state(self, state: SyncState) -> None:
        """Persist sync state to a JSON file."""
        state_file = Path(self.config.state_path)
        data = {
            "last_synced_at": state.last_synced_at.isoformat() if state.last_synced_at else None,
            "last_event_timestamp": state.last_event_timestamp.isoformat() if state.last_event_timestamp else None,
            "events_synced": state.events_synced,
        }
        state_file.write_text(json.dumps(data, indent=2))
