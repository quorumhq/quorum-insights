"""
PostHog → Quorum Insights canonical event mapping.

Maps PostHog's event schema into InsightEvent format.
Handles: $pageview, $pageleave, $identify, $groupidentify, custom events.
Extracts $-prefixed default properties into structured fields.

PostHog event shape (from API / webhook):
{
    "event": "$pageview" | "custom_event_name",
    "distinct_id": "user-123",
    "timestamp": "2026-03-31T10:00:00Z",
    "properties": {
        "$current_url": "https://example.com/page",
        "$pathname": "/page",
        "$referrer": "https://google.com",
        "$os": "Mac OS X",
        "$browser": "Chrome",
        "$device_type": "Desktop",
        "$session_id": "abc123",
        "$set": {"plan": "pro", "name": "Alice"},
        "$set_once": {"first_utm_source": "google"},
        "$group_0": "company-456",
        "$utm_source": "newsletter",
        ...custom properties...
    }
}
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from insights.schema.event import (
    DeviceType,
    EventType,
    InsightEvent,
    SourceSystem,
)

# PostHog $device_type → our DeviceType
_DEVICE_MAP: dict[str, DeviceType] = {
    "Desktop": DeviceType.DESKTOP,
    "Mobile": DeviceType.MOBILE,
    "Tablet": DeviceType.TABLET,
}

# PostHog event names → normalized EventType
_EVENT_TYPE_MAP: dict[str, EventType] = {
    "$pageview": EventType.PAGEVIEW,
    "$pageleave": EventType.PAGEVIEW,  # still a page-level event
    "$identify": EventType.IDENTIFY,
    "$groupidentify": EventType.GROUP_IDENTIFY,
    "$screen": EventType.PAGEVIEW,  # mobile equivalent
}

# PostHog properties that map to core InsightEvent fields (extract, don't duplicate)
_CORE_PROPERTY_KEYS = frozenset({
    "$current_url", "$pathname", "$referrer", "$session_id",
    "$os", "$browser", "$device_type", "$browser_version", "$os_version",
    "$utm_source", "$utm_medium", "$utm_campaign", "$utm_term", "$utm_content",
    "$set", "$set_once", "$group_type", "$group_key",
    "$lib", "$lib_version",
    # group keys are dynamic ($group_0, $group_1, etc.)
})


def stringify_value(v: Any) -> str:
    """Convert any value to a string for the properties map."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (dict, list)):
        import json
        return json.dumps(v, default=str)
    return str(v)


def map_posthog_event(
    raw: dict[str, Any],
    tenant_id: str,
) -> InsightEvent:
    """
    Map a single PostHog event dict to an InsightEvent.

    Args:
        raw: PostHog event as dict (from API response or webhook payload)
        tenant_id: Tenant identifier for multi-tenant isolation

    Returns:
        InsightEvent in canonical format
    """
    props = raw.get("properties", {}) or {}
    event_name = raw.get("event", "unknown")

    # ── Event type ──
    event_type = _EVENT_TYPE_MAP.get(event_name, EventType.TRACK)

    # ── User identity ──
    distinct_id = raw.get("distinct_id", "")
    # PostHog uses distinct_id for both identified and anonymous users.
    # If it looks like a UUID, it's likely anonymous. Otherwise, it's an identified user.
    # Better heuristic: check if $process_person_profile is false (anonymous event)
    is_anonymous = props.get("$process_person_profile") == "false"

    user_id = None if is_anonymous else distinct_id
    anonymous_id = distinct_id if is_anonymous else None

    # For $identify events, the distinct_id is the identified user
    if event_name == "$identify":
        user_id = distinct_id
        anonymous_id = props.get("$anon_distinct_id")

    # ── Timestamp ──
    ts_raw = raw.get("timestamp") or props.get("$timestamp")
    if isinstance(ts_raw, str):
        # Handle ISO 8601 with or without timezone
        timestamp = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
    elif isinstance(ts_raw, (int, float)):
        timestamp = datetime.utcfromtimestamp(ts_raw / 1000)
    else:
        timestamp = datetime.now(timezone.utc)

    # ── Device type ──
    device_type = _DEVICE_MAP.get(
        props.get("$device_type", ""), DeviceType.UNKNOWN
    )

    # ── User properties ($set / $set_once) ──
    user_props_set: dict[str, str] = {}
    user_props_set_once: dict[str, str] = {}
    raw_set = props.get("$set")
    raw_set_once = props.get("$set_once")
    if isinstance(raw_set, dict):
        user_props_set = {k: stringify_value(v) for k, v in raw_set.items()}
    if isinstance(raw_set_once, dict):
        user_props_set_once = {k: stringify_value(v) for k, v in raw_set_once.items()}

    # ── Group (B2B) ──
    # PostHog uses $group_type + $group_key for $groupidentify,
    # and $group_0, $group_1, etc. for regular events
    group_type = props.get("$group_type")
    group_id = props.get("$group_key")
    group_properties: dict[str, str] = {}

    if not group_type:
        # Check for $group_0 pattern
        for i in range(5):
            gkey = f"$group_{i}"
            if gkey in props:
                group_type = f"group_{i}"
                group_id = str(props[gkey])
                break

    if event_name == "$groupidentify":
        raw_group_props = props.get("$group_set", {})
        if isinstance(raw_group_props, dict):
            group_properties = {k: stringify_value(v) for k, v in raw_group_props.items()}

    # ── Pass-through properties (everything not extracted to core fields) ──
    pass_through: dict[str, str] = {}
    for k, v in props.items():
        if k.startswith("$set") or k.startswith("$group"):
            continue  # already extracted
        if k in _CORE_PROPERTY_KEYS:
            continue  # mapped to structured fields
        # Keep $-prefixed PostHog defaults as well as custom properties
        pass_through[k] = stringify_value(v)

    # ── Country / locale ──
    # PostHog doesn't always have these natively; they come from GeoIP plugin
    country = None
    geoip_country = props.get("$geoip_country_code")
    if geoip_country and len(geoip_country) == 2:
        country = geoip_country.upper()

    locale = props.get("$locale")

    return InsightEvent(
        tenant_id=tenant_id,
        user_id=user_id,
        anonymous_id=anonymous_id,
        event_name=event_name,
        event_type=event_type,
        timestamp=timestamp,
        session_id=props.get("$session_id"),
        page_url=props.get("$current_url"),
        page_path=props.get("$pathname"),
        referrer=props.get("$referrer"),
        locale=locale,
        country=country,
        device_type=device_type,
        source_system=SourceSystem.POSTHOG,
        source_event_id=raw.get("uuid"),
        properties=pass_through,
        user_properties_set=user_props_set,
        user_properties_set_once=user_props_set_once,
        group_type=group_type,
        group_id=group_id,
        group_properties=group_properties,
        utm_source=props.get("$utm_source"),
        utm_medium=props.get("$utm_medium"),
        utm_campaign=props.get("$utm_campaign"),
        utm_term=props.get("$utm_term"),
        utm_content=props.get("$utm_content"),
    )


def map_posthog_batch(
    events: list[dict[str, Any]],
    tenant_id: str,
) -> list[InsightEvent]:
    """Map a batch of PostHog events."""
    return [map_posthog_event(e, tenant_id) for e in events]
