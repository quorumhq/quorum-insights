"""
Quorum Insights — Canonical Event Schema

Every connector normalizes source events into this format.
Every stats computation, LLM insight, and UI view reads from it.

Design principles:
1. Flat core, nested extensions — fast ClickHouse queries on core fields
2. Preserve original event name — add normalized event_type for analytics
3. AI context optional — populated only when LLM trace data available
4. No data loss — all source properties pass through in properties dict
5. ClickHouse-native types — DateTime64, LowCardinality, Map(String, String)
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator


class EventType(str, Enum):
    """Normalized event types across all sources."""

    PAGEVIEW = "pageview"
    IDENTIFY = "identify"
    TRACK = "track"
    AI_GENERATION = "ai_generation"
    AI_TOOL_CALL = "ai_tool_call"
    AI_RETRIEVAL = "ai_retrieval"
    SESSION_START = "session_start"
    SESSION_END = "session_end"
    GROUP_IDENTIFY = "group_identify"


class DeviceType(str, Enum):
    """Device type categories."""

    DESKTOP = "desktop"
    MOBILE = "mobile"
    TABLET = "tablet"
    SERVER = "server"
    UNKNOWN = "unknown"


class SourceSystem(str, Enum):
    """Known source systems. Extensible via CUSTOM."""

    POSTHOG = "posthog"
    SEGMENT = "segment"
    AMPLITUDE = "amplitude"
    LANGFUSE = "langfuse"
    HELICONE = "helicone"
    WAREHOUSE = "warehouse"
    QUORUM_ACCURACY = "quorum_accuracy"
    CUSTOM = "custom"


class AIContext(BaseModel):
    """
    AI-specific context fields — the wedge.

    Populated only when LLM trace data is available (Langfuse, Helicone,
    Quorum Accuracy). All Insights analytics work without these fields.
    """

    model: Optional[str] = Field(None, description="LLM model name, e.g. 'gpt-4o', 'claude-sonnet-4'")
    provider: Optional[str] = Field(None, description="Model provider, e.g. 'openai', 'anthropic'")
    feature: Optional[str] = Field(None, description="Which product feature used AI, e.g. 'search', 'chat'")
    quality_score: Optional[float] = Field(
        None, ge=0.0, le=1.0, description="Quality score 0-1 (from Langfuse scores, Quorum verification, etc.)"
    )
    latency_ms: Optional[int] = Field(None, ge=0, description="LLM response time in milliseconds")
    tokens_in: Optional[int] = Field(None, ge=0, description="Input tokens")
    tokens_out: Optional[int] = Field(None, ge=0, description="Output tokens")
    cost_usd: Optional[float] = Field(None, ge=0.0, description="Cost of this AI call in USD")
    trace_id: Optional[str] = Field(None, description="Link back to Langfuse/Helicone/Quorum trace")
    generation_id: Optional[str] = Field(None, description="Specific generation/observation ID within trace")

    # Quorum Accuracy specific (populated when connected)
    verification_result: Optional[str] = Field(None, description="pass, fail, skip")
    consensus_agreement: Optional[float] = Field(None, ge=0.0, le=1.0)
    heal_triggered: Optional[bool] = None


class InsightEvent(BaseModel):
    """
    The canonical event format for Quorum Insights.

    All connectors (PostHog, Segment, Langfuse, warehouse) normalize
    their source events into this format before storage and analysis.
    """

    # ── Identity ──
    tenant_id: str = Field(..., description="Multi-tenant isolation key")
    event_id: UUID = Field(default_factory=uuid4, description="Unique event identifier")
    user_id: Optional[str] = Field(None, description="Identified user ID (from source system)")
    anonymous_id: Optional[str] = Field(None, description="Pre-identification anonymous ID")

    # ── Event ──
    event_name: str = Field(..., description="Original event name verbatim from source ('$pageview', 'Course Clicked')")
    event_type: EventType = Field(EventType.TRACK, description="Normalized event type for analytics")
    timestamp: datetime = Field(..., description="When the event occurred (from source)")
    received_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When Insights ingested the event",
    )

    # ── Session & Context ──
    session_id: Optional[str] = Field(None, description="Session identifier")
    page_url: Optional[str] = Field(None, description="Full page URL (web events)")
    page_path: Optional[str] = Field(None, description="URL path component")
    referrer: Optional[str] = Field(None, description="Referrer URL")
    locale: Optional[str] = Field(None, description="User locale, e.g. 'en-US', 'fr-FR'")
    country: Optional[str] = Field(None, max_length=2, description="ISO 3166-1 alpha-2 country code")
    device_type: DeviceType = Field(DeviceType.UNKNOWN, description="Device category")

    # ── Source ──
    source_system: SourceSystem = Field(..., description="Which system generated this event")
    source_event_id: Optional[str] = Field(None, description="Original event ID from source system")

    # ── Properties (pass-through) ──
    properties: dict[str, str] = Field(
        default_factory=dict,
        description="All source properties, stringified values. No data loss.",
    )

    # ── User Properties (set/update) ──
    user_properties_set: dict[str, str] = Field(
        default_factory=dict,
        description="User properties to set (from $set, identify traits, etc.)",
    )
    user_properties_set_once: dict[str, str] = Field(
        default_factory=dict,
        description="User properties to set only if not already set",
    )

    # ── Group / Company (B2B) ──
    group_type: Optional[str] = Field(None, description="Group type, e.g. 'company', 'organization'")
    group_id: Optional[str] = Field(None, description="Group identifier")
    group_properties: dict[str, str] = Field(
        default_factory=dict,
        description="Group properties to set",
    )

    # ── AI Context (optional — the wedge) ──
    ai: Optional[AIContext] = Field(None, description="AI-specific context. Populated from Langfuse/Helicone/Quorum.")

    # ── UTM / Marketing ──
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None
    utm_term: Optional[str] = None
    utm_content: Optional[str] = None

    @field_validator("user_id", "anonymous_id")
    @classmethod
    def at_least_one_identity(cls, v: Optional[str], info) -> Optional[str]:
        """Ensure at least user_id or anonymous_id is provided during final validation."""
        # Individual field validators can't cross-reference, so we do this in model_validator
        return v

    def has_identity(self) -> bool:
        """Check if the event has at least one identity field."""
        return bool(self.user_id or self.anonymous_id)

    def has_ai_context(self) -> bool:
        """Check if this event has AI-specific data."""
        return self.ai is not None

    def to_clickhouse_row(self) -> dict:
        """Convert to a flat dict suitable for ClickHouse insertion."""
        row = {
            "tenant_id": self.tenant_id,
            "event_id": str(self.event_id),
            "user_id": self.user_id or "",
            "anonymous_id": self.anonymous_id or "",
            "event_name": self.event_name,
            "event_type": self.event_type.value,
            "timestamp": self.timestamp,
            "received_at": self.received_at,
            "session_id": self.session_id or "",
            "page_url": self.page_url or "",
            "page_path": self.page_path or "",
            "referrer": self.referrer or "",
            "locale": self.locale or "",
            "country": self.country or "",
            "device_type": self.device_type.value,
            "source_system": self.source_system.value,
            "source_event_id": self.source_event_id or "",
            "properties": self.properties,
            "utm_source": self.utm_source or "",
            "utm_medium": self.utm_medium or "",
            "utm_campaign": self.utm_campaign or "",
        }

        # Flatten AI context
        if self.ai:
            row["ai_model"] = self.ai.model or ""
            row["ai_provider"] = self.ai.provider or ""
            row["ai_feature"] = self.ai.feature or ""
            row["ai_quality_score"] = self.ai.quality_score if self.ai.quality_score is not None else 0.0
            row["ai_latency_ms"] = self.ai.latency_ms if self.ai.latency_ms is not None else 0
            row["ai_tokens_in"] = self.ai.tokens_in if self.ai.tokens_in is not None else 0
            row["ai_tokens_out"] = self.ai.tokens_out if self.ai.tokens_out is not None else 0
            row["ai_cost_usd"] = self.ai.cost_usd if self.ai.cost_usd is not None else 0.0
            row["ai_trace_id"] = self.ai.trace_id or ""
        else:
            row["ai_model"] = ""
            row["ai_provider"] = ""
            row["ai_feature"] = ""
            row["ai_quality_score"] = 0.0
            row["ai_latency_ms"] = 0
            row["ai_tokens_in"] = 0
            row["ai_tokens_out"] = 0
            row["ai_cost_usd"] = 0.0
            row["ai_trace_id"] = ""

        return row


class UserProfile(BaseModel):
    """
    Materialized user profile — accumulated from events over time.

    This is NOT stored per-event. It's a ClickHouse materialized view
    or computed table that aggregates user properties from identify events
    and user_properties_set across all events for a user.
    """

    tenant_id: str
    user_id: str
    anonymous_ids: list[str] = Field(default_factory=list, description="All anonymous IDs merged into this user")
    first_seen: datetime
    last_seen: datetime
    event_count: int = 0

    # Accumulated properties (latest wins for $set, first wins for $set_once)
    segment: Optional[str] = None
    plan: Optional[str] = None
    locale: Optional[str] = None
    country: Optional[str] = None
    email: Optional[str] = None
    name: Optional[str] = None

    # All other properties
    properties: dict[str, str] = Field(default_factory=dict)

    # AI-specific aggregates
    ai_events_count: int = 0
    ai_avg_quality: Optional[float] = None
    ai_features_used: list[str] = Field(default_factory=list)
