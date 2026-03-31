"""Tests for the canonical event schema and PostHog mapping."""

import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import pytest

from schema.event import (
    AIContext,
    DeviceType,
    EventType,
    InsightEvent,
    SourceSystem,
    UserProfile,
)
from connectors.posthog_mapping import map_posthog_event, stringify_value


# ─── Schema Tests ───


class TestInsightEvent:
    """Test the canonical InsightEvent model."""

    def test_minimal_event(self):
        """Minimum viable event: tenant_id, event_name, timestamp, source, user_id."""
        event = InsightEvent(
            tenant_id="t1",
            user_id="u1",
            event_name="page_viewed",
            event_type=EventType.TRACK,
            timestamp=datetime(2026, 3, 31, tzinfo=timezone.utc),
            source_system=SourceSystem.POSTHOG,
        )
        assert event.tenant_id == "t1"
        assert event.user_id == "u1"
        assert event.event_name == "page_viewed"
        assert event.event_type == EventType.TRACK
        assert event.has_identity()
        assert not event.has_ai_context()

    def test_anonymous_event(self):
        """Event with anonymous_id only (no user_id)."""
        event = InsightEvent(
            tenant_id="t1",
            anonymous_id="anon-abc",
            event_name="$pageview",
            event_type=EventType.PAGEVIEW,
            timestamp=datetime(2026, 3, 31, tzinfo=timezone.utc),
            source_system=SourceSystem.POSTHOG,
        )
        assert event.user_id is None
        assert event.anonymous_id == "anon-abc"
        assert event.has_identity()

    def test_event_with_ai_context(self):
        """Event with AI context populated (the wedge)."""
        event = InsightEvent(
            tenant_id="t1",
            user_id="u1",
            event_name="chat_response",
            event_type=EventType.AI_GENERATION,
            timestamp=datetime(2026, 3, 31, tzinfo=timezone.utc),
            source_system=SourceSystem.LANGFUSE,
            ai=AIContext(
                model="gpt-4o",
                provider="openai",
                feature="chat",
                quality_score=0.92,
                latency_ms=1200,
                tokens_in=150,
                tokens_out=320,
                cost_usd=0.0045,
                trace_id="lf-trace-123",
            ),
        )
        assert event.has_ai_context()
        assert event.ai.model == "gpt-4o"
        assert event.ai.quality_score == 0.92
        assert event.ai.cost_usd == 0.0045

    def test_ai_quality_score_bounds(self):
        """Quality score must be between 0 and 1."""
        with pytest.raises(Exception):
            AIContext(quality_score=1.5)
        with pytest.raises(Exception):
            AIContext(quality_score=-0.1)

    def test_properties_passthrough(self):
        """All source properties pass through in properties dict."""
        event = InsightEvent(
            tenant_id="t1",
            user_id="u1",
            event_name="purchase",
            timestamp=datetime(2026, 3, 31, tzinfo=timezone.utc),
            source_system=SourceSystem.SEGMENT,
            properties={
                "product_id": "SKU-123",
                "price": "29.99",
                "currency": "USD",
                "custom_field": "anything",
            },
        )
        assert event.properties["product_id"] == "SKU-123"
        assert event.properties["price"] == "29.99"
        assert event.properties["custom_field"] == "anything"

    def test_user_properties_set(self):
        """User properties from identify/set operations."""
        event = InsightEvent(
            tenant_id="t1",
            user_id="u1",
            event_name="$identify",
            event_type=EventType.IDENTIFY,
            timestamp=datetime(2026, 3, 31, tzinfo=timezone.utc),
            source_system=SourceSystem.POSTHOG,
            user_properties_set={"plan": "pro", "name": "Alice"},
            user_properties_set_once={"signup_source": "google"},
        )
        assert event.user_properties_set["plan"] == "pro"
        assert event.user_properties_set_once["signup_source"] == "google"

    def test_group_event(self):
        """B2B group/company event."""
        event = InsightEvent(
            tenant_id="t1",
            user_id="u1",
            event_name="$groupidentify",
            event_type=EventType.GROUP_IDENTIFY,
            timestamp=datetime(2026, 3, 31, tzinfo=timezone.utc),
            source_system=SourceSystem.POSTHOG,
            group_type="company",
            group_id="acme-corp",
            group_properties={"industry": "fintech", "employees": "50"},
        )
        assert event.group_type == "company"
        assert event.group_id == "acme-corp"
        assert event.group_properties["industry"] == "fintech"

    def test_to_clickhouse_row(self):
        """ClickHouse row conversion produces flat dict with correct types."""
        event = InsightEvent(
            tenant_id="t1",
            user_id="u1",
            event_name="click",
            timestamp=datetime(2026, 3, 31, tzinfo=timezone.utc),
            source_system=SourceSystem.POSTHOG,
            ai=AIContext(model="gpt-4o", quality_score=0.85),
        )
        row = event.to_clickhouse_row()
        assert row["tenant_id"] == "t1"
        assert row["user_id"] == "u1"
        assert row["ai_model"] == "gpt-4o"
        assert row["ai_quality_score"] == 0.85
        assert row["ai_provider"] == ""  # not set → empty string
        assert isinstance(row["event_id"], str)  # UUID serialized

    def test_to_clickhouse_row_no_ai(self):
        """ClickHouse row without AI context has zero defaults."""
        event = InsightEvent(
            tenant_id="t1",
            user_id="u1",
            event_name="click",
            timestamp=datetime(2026, 3, 31, tzinfo=timezone.utc),
            source_system=SourceSystem.POSTHOG,
        )
        row = event.to_clickhouse_row()
        assert row["ai_model"] == ""
        assert row["ai_quality_score"] == 0.0
        assert row["ai_tokens_in"] == 0

    def test_serialization_roundtrip(self):
        """Serialize to JSON and back — identical."""
        event = InsightEvent(
            tenant_id="t1",
            user_id="u1",
            event_name="test",
            timestamp=datetime(2026, 3, 31, 12, 0, 0, tzinfo=timezone.utc),
            source_system=SourceSystem.POSTHOG,
            properties={"key": "value"},
            ai=AIContext(model="claude-sonnet-4", quality_score=0.95),
        )
        json_str = event.model_dump_json()
        restored = InsightEvent.model_validate_json(json_str)
        assert restored.tenant_id == event.tenant_id
        assert restored.event_name == event.event_name
        assert restored.ai.model == event.ai.model
        assert restored.ai.quality_score == event.ai.quality_score
        assert restored.properties == event.properties

    def test_all_event_types(self):
        """Every EventType enum value is valid."""
        for et in EventType:
            event = InsightEvent(
                tenant_id="t1",
                user_id="u1",
                event_name=f"test_{et.value}",
                event_type=et,
                timestamp=datetime(2026, 3, 31, tzinfo=timezone.utc),
                source_system=SourceSystem.POSTHOG,
            )
            assert event.event_type == et

    def test_all_source_systems(self):
        """Every SourceSystem enum value is valid."""
        for ss in SourceSystem:
            event = InsightEvent(
                tenant_id="t1",
                user_id="u1",
                event_name="test",
                timestamp=datetime(2026, 3, 31, tzinfo=timezone.utc),
                source_system=ss,
            )
            assert event.source_system == ss


# ─── PostHog Mapping Tests ───


class TestPostHogMapping:
    """Test PostHog event → InsightEvent mapping."""

    TENANT = "test-tenant"

    def test_pageview(self):
        """PostHog $pageview maps correctly."""
        raw = {
            "event": "$pageview",
            "distinct_id": "user-123",
            "timestamp": "2026-03-31T10:00:00Z",
            "uuid": "ph-event-456",
            "properties": {
                "$current_url": "https://example.com/dashboard",
                "$pathname": "/dashboard",
                "$referrer": "https://google.com",
                "$session_id": "sess-789",
                "$device_type": "Desktop",
                "$browser": "Chrome",
                "$os": "Mac OS X",
            },
        }
        event = map_posthog_event(raw, self.TENANT)

        assert event.event_type == EventType.PAGEVIEW
        assert event.event_name == "$pageview"
        assert event.user_id == "user-123"
        assert event.page_url == "https://example.com/dashboard"
        assert event.page_path == "/dashboard"
        assert event.referrer == "https://google.com"
        assert event.session_id == "sess-789"
        assert event.device_type == DeviceType.DESKTOP
        assert event.source_system == SourceSystem.POSTHOG
        assert event.source_event_id == "ph-event-456"

    def test_custom_event(self):
        """Custom event with properties maps as TRACK."""
        raw = {
            "event": "purchase_completed",
            "distinct_id": "user-123",
            "timestamp": "2026-03-31T10:00:00Z",
            "properties": {
                "product_id": "SKU-456",
                "price": 29.99,
                "currency": "USD",
            },
        }
        event = map_posthog_event(raw, self.TENANT)

        assert event.event_type == EventType.TRACK
        assert event.event_name == "purchase_completed"
        assert event.properties["product_id"] == "SKU-456"
        assert event.properties["price"] == "29.99"
        assert event.properties["currency"] == "USD"

    def test_identify_event(self):
        """PostHog $identify with $set properties."""
        raw = {
            "event": "$identify",
            "distinct_id": "user-123",
            "timestamp": "2026-03-31T10:00:00Z",
            "properties": {
                "$set": {"plan": "enterprise", "name": "Bob", "company": "Acme"},
                "$set_once": {"signup_date": "2026-01-15"},
                "$anon_distinct_id": "anon-old-id",
            },
        }
        event = map_posthog_event(raw, self.TENANT)

        assert event.event_type == EventType.IDENTIFY
        assert event.user_id == "user-123"
        assert event.anonymous_id == "anon-old-id"
        assert event.user_properties_set["plan"] == "enterprise"
        assert event.user_properties_set["name"] == "Bob"
        assert event.user_properties_set_once["signup_date"] == "2026-01-15"

    def test_group_identify(self):
        """PostHog $groupidentify maps group fields."""
        raw = {
            "event": "$groupidentify",
            "distinct_id": "user-123",
            "timestamp": "2026-03-31T10:00:00Z",
            "properties": {
                "$group_type": "company",
                "$group_key": "acme-corp",
                "$group_set": {"industry": "fintech", "plan": "enterprise"},
            },
        }
        event = map_posthog_event(raw, self.TENANT)

        assert event.event_type == EventType.GROUP_IDENTIFY
        assert event.group_type == "company"
        assert event.group_id == "acme-corp"
        assert event.group_properties["industry"] == "fintech"

    def test_utm_extraction(self):
        """UTM parameters extracted to structured fields."""
        raw = {
            "event": "$pageview",
            "distinct_id": "user-123",
            "timestamp": "2026-03-31T10:00:00Z",
            "properties": {
                "$utm_source": "newsletter",
                "$utm_medium": "email",
                "$utm_campaign": "spring_launch",
                "$utm_term": "ai analytics",
                "$utm_content": "hero_cta",
            },
        }
        event = map_posthog_event(raw, self.TENANT)

        assert event.utm_source == "newsletter"
        assert event.utm_medium == "email"
        assert event.utm_campaign == "spring_launch"
        assert event.utm_term == "ai analytics"
        assert event.utm_content == "hero_cta"

    def test_mobile_device(self):
        """Mobile device type maps correctly."""
        raw = {
            "event": "$pageview",
            "distinct_id": "user-123",
            "timestamp": "2026-03-31T10:00:00Z",
            "properties": {"$device_type": "Mobile"},
        }
        event = map_posthog_event(raw, self.TENANT)
        assert event.device_type == DeviceType.MOBILE

    def test_unknown_device(self):
        """Unknown device type defaults to UNKNOWN."""
        raw = {
            "event": "$pageview",
            "distinct_id": "user-123",
            "timestamp": "2026-03-31T10:00:00Z",
            "properties": {},
        }
        event = map_posthog_event(raw, self.TENANT)
        assert event.device_type == DeviceType.UNKNOWN

    def test_properties_no_data_loss(self):
        """Custom properties pass through without loss. $-prefixed core props are extracted."""
        raw = {
            "event": "button_clicked",
            "distinct_id": "user-123",
            "timestamp": "2026-03-31T10:00:00Z",
            "properties": {
                "$current_url": "https://example.com",  # extracted to page_url
                "button_id": "cta-main",  # pass through
                "color": "blue",  # pass through
                "nested": {"key": "value"},  # pass through as JSON string
                "count": 42,  # pass through as string
            },
        }
        event = map_posthog_event(raw, self.TENANT)

        # Core props extracted
        assert event.page_url == "https://example.com"
        # Custom props pass through
        assert event.properties["button_id"] == "cta-main"
        assert event.properties["color"] == "blue"
        assert event.properties["count"] == "42"
        assert '"key": "value"' in event.properties["nested"]
        # Extracted core props NOT duplicated in properties
        assert "$current_url" not in event.properties

    def test_geoip_country(self):
        """GeoIP country code extracted."""
        raw = {
            "event": "$pageview",
            "distinct_id": "user-123",
            "timestamp": "2026-03-31T10:00:00Z",
            "properties": {"$geoip_country_code": "US"},
        }
        event = map_posthog_event(raw, self.TENANT)
        assert event.country == "US"

    def test_anonymous_event(self):
        """Anonymous PostHog event ($process_person_profile = false)."""
        raw = {
            "event": "$pageview",
            "distinct_id": "anon-uuid-123",
            "timestamp": "2026-03-31T10:00:00Z",
            "properties": {"$process_person_profile": "false"},
        }
        event = map_posthog_event(raw, self.TENANT)
        assert event.user_id is None
        assert event.anonymous_id == "anon-uuid-123"

    def test_no_ai_context(self):
        """Standard PostHog events have no AI context."""
        raw = {
            "event": "$pageview",
            "distinct_id": "user-123",
            "timestamp": "2026-03-31T10:00:00Z",
            "properties": {},
        }
        event = map_posthog_event(raw, self.TENANT)
        assert event.ai is None
        assert not event.has_ai_context()


class TestStringifyValue:
    """Test the property value stringification."""

    def test_none(self):
        assert stringify_value(None) == ""

    def test_string(self):
        assert stringify_value("hello") == "hello"

    def test_int(self):
        assert stringify_value(42) == "42"

    def test_float(self):
        assert stringify_value(3.14) == "3.14"

    def test_bool(self):
        assert stringify_value(True) == "true"
        assert stringify_value(False) == "false"

    def test_dict(self):
        result = stringify_value({"key": "value"})
        assert json.loads(result) == {"key": "value"}

    def test_list(self):
        result = stringify_value([1, 2, 3])
        assert json.loads(result) == [1, 2, 3]


# ─── JSON Schema Validation ───


class TestJSONSchema:
    """Test the JSON schema validates correctly."""

    def test_schema_file_exists(self):
        schema_path = Path(__file__).parent.parent / "schema" / "event.schema.json"
        assert schema_path.exists(), f"Schema file not found at {schema_path}"

    def test_schema_is_valid_json(self):
        schema_path = Path(__file__).parent.parent / "schema" / "event.schema.json"
        with open(schema_path) as f:
            schema = json.load(f)
        assert schema["title"] == "Quorum Insights Canonical Event"
        assert "tenant_id" in schema["required"]
        assert "ai" in schema["properties"]
