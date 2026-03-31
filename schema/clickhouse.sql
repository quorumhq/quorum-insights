-- Quorum Insights: ClickHouse schema
-- Canonical event table + materialized views for user profiles
--
-- Design: MergeTree ordered by (tenant_id, user_id, timestamp)
-- for fast per-tenant, per-user time-range queries.

-- ─── Main Events Table ───

CREATE TABLE IF NOT EXISTS insight_events (
    -- Identity
    tenant_id       String,
    event_id        UUID,
    user_id         String          DEFAULT '',
    anonymous_id    String          DEFAULT '',

    -- Event
    event_name      String,
    event_type      LowCardinality(String),  -- pageview, identify, track, ai_generation, ai_tool_call, etc.
    timestamp       DateTime64(3, 'UTC'),
    received_at     DateTime64(3, 'UTC'),

    -- Session & Context
    session_id      String          DEFAULT '',
    page_url        String          DEFAULT '',
    page_path       String          DEFAULT '',
    referrer        String          DEFAULT '',
    locale          String          DEFAULT '',
    country         LowCardinality(String) DEFAULT '',
    device_type     LowCardinality(String) DEFAULT 'unknown',

    -- Source
    source_system   LowCardinality(String),  -- posthog, segment, langfuse, warehouse, etc.
    source_event_id String          DEFAULT '',

    -- Properties (pass-through, no data loss)
    properties      Map(String, String),

    -- User properties (from $set / identify)
    user_properties_set      Map(String, String),
    user_properties_set_once Map(String, String),

    -- Group / Company (B2B)
    group_type      String          DEFAULT '',
    group_id        String          DEFAULT '',
    group_properties Map(String, String),

    -- AI Context (optional — the wedge)
    ai_model        String          DEFAULT '',
    ai_provider     String          DEFAULT '',
    ai_feature      String          DEFAULT '',
    ai_quality_score Float32        DEFAULT 0,
    ai_latency_ms   UInt32          DEFAULT 0,
    ai_tokens_in    UInt32          DEFAULT 0,
    ai_tokens_out   UInt32          DEFAULT 0,
    ai_cost_usd     Float32         DEFAULT 0,
    ai_trace_id     String          DEFAULT '',

    -- UTM / Marketing
    utm_source      String          DEFAULT '',
    utm_medium      String          DEFAULT '',
    utm_campaign    String          DEFAULT '',
    utm_term        String          DEFAULT '',
    utm_content     String          DEFAULT '',

    -- Derived (populated at insert time)
    event_date      Date            DEFAULT toDate(timestamp)

) ENGINE = MergeTree()
PARTITION BY (tenant_id, toYYYYMM(timestamp))
ORDER BY (tenant_id, user_id, timestamp, event_id)
TTL toDateTime(timestamp) + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;


-- ─── User Profiles (Materialized View) ───
-- Aggregates user properties from all events.
-- Queryable for cohort analysis, segmentation, churn detection.

CREATE MATERIALIZED VIEW IF NOT EXISTS user_profiles_mv
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, user_id)
AS SELECT
    tenant_id,
    user_id,
    min(timestamp)                              AS first_seen,
    max(timestamp)                              AS last_seen,
    count()                                     AS event_count,
    countIf(ai_model != '')                     AS ai_events_count,
    avgIf(ai_quality_score, ai_quality_score > 0) AS ai_avg_quality,
    groupUniqArrayIf(ai_feature, ai_feature != '') AS ai_features_used,
    groupUniqArray(source_system)               AS source_systems
FROM insight_events
WHERE user_id != ''
GROUP BY tenant_id, user_id;


-- ─── Daily Metrics (Materialized View) ───
-- Pre-aggregated daily metrics per tenant for fast trend queries.

CREATE MATERIALIZED VIEW IF NOT EXISTS daily_metrics_mv
ENGINE = SummingMergeTree()
ORDER BY (tenant_id, event_date, event_type)
AS SELECT
    tenant_id,
    event_date,
    event_type,
    count()                         AS event_count,
    uniq(user_id)                   AS unique_users,
    uniq(session_id)                AS unique_sessions,
    countIf(ai_model != '')         AS ai_events,
    avgIf(ai_quality_score, ai_quality_score > 0) AS avg_ai_quality
FROM insight_events
GROUP BY tenant_id, event_date, event_type;


-- ─── Retention Cohorts (Materialized View) ───
-- First-seen date per user, for retention curve computation.

CREATE MATERIALIZED VIEW IF NOT EXISTS user_cohorts_mv
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, user_id)
AS SELECT
    tenant_id,
    user_id,
    min(event_date)                 AS cohort_date,
    max(event_date)                 AS last_active_date,
    count()                         AS lifetime_events
FROM insight_events
WHERE user_id != ''
GROUP BY tenant_id, user_id;


-- ─── Feature Usage (Materialized View) ───
-- Per-feature usage and AI quality aggregates for feature impact analysis.

CREATE MATERIALIZED VIEW IF NOT EXISTS feature_usage_mv
ENGINE = SummingMergeTree()
ORDER BY (tenant_id, event_date, event_name)
AS SELECT
    tenant_id,
    event_date,
    event_name,
    count()                         AS usage_count,
    uniq(user_id)                   AS unique_users,
    countIf(ai_model != '')         AS ai_events,
    avgIf(ai_quality_score, ai_quality_score > 0) AS avg_ai_quality,
    sumIf(ai_cost_usd, ai_cost_usd > 0) AS total_ai_cost
FROM insight_events
GROUP BY tenant_id, event_date, event_name;
