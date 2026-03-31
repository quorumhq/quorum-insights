"""Tests for the PostHog connector."""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from connectors.posthog import PostHogConfig, PostHogConnector
from connectors.base import SyncState
from schema.event import EventType, SourceSystem


@pytest.fixture
def config():
    return PostHogConfig(
        tenant_id="test-tenant",
        api_key="phx_test_key",
        project_id="12345",
        host="https://us.i.posthog.com",
        batch_size=10,
        lookback_days=7,
        state_path="/tmp/test_posthog_sync_state.json",
    )


@pytest.fixture
def connector(config):
    return PostHogConnector(config)


# ─── PostHogConfig Tests ───


class TestPostHogConfig:
    def test_defaults(self):
        config = PostHogConfig(tenant_id="t1")
        assert config.host == "https://us.i.posthog.com"
        assert config.batch_size == 1000
        assert config.lookback_days == 90

    def test_custom_host(self):
        config = PostHogConfig(tenant_id="t1", host="https://posthog.mycompany.com")
        assert config.host == "https://posthog.mycompany.com"


# ─── Row Parsing Tests ───


class TestRowParsing:
    def test_parse_row_with_dict_properties(self, connector):
        row = [
            "uuid-123",
            "$pageview",
            "user-1",
            "2026-03-31T10:00:00Z",
            {"$current_url": "https://example.com", "custom": "value"},
        ]
        result = connector._parse_row(row)
        assert result["uuid"] == "uuid-123"
        assert result["event"] == "$pageview"
        assert result["distinct_id"] == "user-1"
        assert result["properties"]["$current_url"] == "https://example.com"
        assert result["properties"]["custom"] == "value"

    def test_parse_row_with_string_properties(self, connector):
        props_json = json.dumps({"$browser": "Chrome", "plan": "pro"})
        row = ["uuid-456", "signup", "user-2", "2026-03-31T11:00:00Z", props_json]
        result = connector._parse_row(row)
        assert result["properties"]["$browser"] == "Chrome"
        assert result["properties"]["plan"] == "pro"

    def test_parse_row_with_null_properties(self, connector):
        row = ["uuid-789", "click", "user-3", "2026-03-31T12:00:00Z", None]
        result = connector._parse_row(row)
        assert result["properties"] == {}

    def test_parse_row_with_invalid_json_properties(self, connector):
        row = ["uuid-abc", "event", "user-4", "2026-03-31T13:00:00Z", "not-json"]
        result = connector._parse_row(row)
        assert result["properties"] == {}


# ─── Sync State Tests ───


class TestSyncState:
    @pytest.mark.asyncio
    async def test_fresh_sync_state(self, connector):
        """First sync returns empty state."""
        import os
        # Ensure no state file exists
        try:
            os.remove(connector.config.state_path)
        except FileNotFoundError:
            pass

        state = await connector.get_sync_state()
        assert state.last_synced_at is None
        assert state.last_event_timestamp is None
        assert state.events_synced == 0

    @pytest.mark.asyncio
    async def test_save_and_load_state(self, connector):
        """State persists across save/load cycle."""
        import os

        state = SyncState(
            last_synced_at=datetime(2026, 3, 31, 10, 0, 0, tzinfo=timezone.utc),
            last_event_timestamp=datetime(2026, 3, 31, 9, 55, 0, tzinfo=timezone.utc),
            events_synced=42,
        )
        await connector.save_sync_state(state)

        loaded = await connector.get_sync_state()
        assert loaded.events_synced == 42
        assert loaded.last_event_timestamp.year == 2026

        # Cleanup
        os.remove(connector.config.state_path)


# ─── Fetch Events Tests (mocked HTTP) ───


class TestFetchEvents:
    @pytest.mark.asyncio
    async def test_fetches_and_maps_events(self, connector):
        """Mocked query returns events that get mapped to InsightEvent."""
        mock_rows = [
            [
                "uuid-1",
                "$pageview",
                "user-1",
                "2026-03-31T10:00:00Z",
                {"$current_url": "https://example.com/home", "$session_id": "sess-1"},
            ],
            [
                "uuid-2",
                "button_clicked",
                "user-1",
                "2026-03-31T10:01:00Z",
                {"button_id": "cta"},
            ],
            [
                "uuid-3",
                "$identify",
                "user-1",
                "2026-03-31T10:02:00Z",
                {"$set": {"plan": "pro"}},
            ],
        ]

        connector._run_query = AsyncMock(side_effect=[mock_rows, []])  # second call returns empty (end)

        batches = []
        async for batch in connector.fetch_events(
            after=datetime(2026, 3, 31, 9, 0, 0, tzinfo=timezone.utc),
            before=datetime(2026, 3, 31, 11, 0, 0, tzinfo=timezone.utc),
        ):
            batches.append(batch)

        assert len(batches) == 1
        events = batches[0]
        assert len(events) == 3

        # First event is pageview
        assert events[0].event_type == EventType.PAGEVIEW
        assert events[0].event_name == "$pageview"
        assert events[0].page_url == "https://example.com/home"
        assert events[0].source_system == SourceSystem.POSTHOG
        assert events[0].tenant_id == "test-tenant"

        # Second event is custom track
        assert events[1].event_type == EventType.TRACK
        assert events[1].properties["button_id"] == "cta"

        # Third event is identify with user properties
        assert events[2].event_type == EventType.IDENTIFY
        assert events[2].user_properties_set["plan"] == "pro"

    @pytest.mark.asyncio
    async def test_pagination_stops_on_empty(self, connector):
        """Stops fetching when query returns no rows."""
        connector._run_query = AsyncMock(return_value=[])

        batches = []
        async for batch in connector.fetch_events(
            after=datetime(2026, 3, 31, 9, 0, 0, tzinfo=timezone.utc),
        ):
            batches.append(batch)

        assert len(batches) == 0

    @pytest.mark.asyncio
    async def test_pagination_stops_on_partial_batch(self, connector):
        """Stops fetching when batch is smaller than batch_size."""
        # batch_size is 10, return 3 events → should stop after 1 fetch
        mock_rows = [
            ["uuid-1", "$pageview", "user-1", "2026-03-31T10:00:00Z", {}],
            ["uuid-2", "$pageview", "user-1", "2026-03-31T10:01:00Z", {}],
            ["uuid-3", "$pageview", "user-1", "2026-03-31T10:02:00Z", {}],
        ]
        connector._run_query = AsyncMock(return_value=mock_rows)

        batches = []
        async for batch in connector.fetch_events(
            after=datetime(2026, 3, 31, 9, 0, 0, tzinfo=timezone.utc),
        ):
            batches.append(batch)

        assert len(batches) == 1
        assert len(batches[0]) == 3
        # Should have called query only once (partial batch = end)
        assert connector._run_query.call_count == 1

    @pytest.mark.asyncio
    async def test_handles_mapping_errors_gracefully(self, connector):
        """Bad rows are skipped, not crash the sync."""
        mock_rows = [
            ["uuid-1", "$pageview", "user-1", "2026-03-31T10:00:00Z", {}],
            # This row has None distinct_id which will produce an event with no identity
            ["uuid-2", "$pageview", None, "2026-03-31T10:01:00Z", {}],
        ]
        connector._run_query = AsyncMock(side_effect=[mock_rows, []])

        batches = []
        async for batch in connector.fetch_events(
            after=datetime(2026, 3, 31, 9, 0, 0, tzinfo=timezone.utc),
        ):
            batches.append(batch)

        # Should get events (None distinct_id still maps, just with empty user_id)
        assert len(batches) == 1


# ─── Validate Tests (mocked HTTP) ───


class TestValidate:
    @pytest.mark.asyncio
    async def test_validate_success(self, connector):
        """Successful validation returns True."""
        mock_response = AsyncMock()
        mock_response.status_code = 200
        # json() is a regular method on httpx.Response, not async
        mock_response.json = lambda: {"results": [[42]]}

        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client.is_closed = False
        connector._client = mock_client

        result = await connector.validate()
        assert result is True

    @pytest.mark.asyncio
    async def test_validate_auth_failure(self, connector):
        """401 returns False."""
        mock_response = AsyncMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"

        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client.is_closed = False
        connector._client = mock_client

        result = await connector.validate()
        assert result is False

    @pytest.mark.asyncio
    async def test_validate_project_not_found(self, connector):
        """404 returns False."""
        mock_response = AsyncMock()
        mock_response.status_code = 404
        mock_response.text = "Not found"

        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client.is_closed = False
        connector._client = mock_client

        result = await connector.validate()
        assert result is False
