-- Quorum Insights: ClickHouse Schema v2
--
-- Design decisions:
--   1. AggregatingMergeTree MVs use -State/-Merge for correct incremental aggregation
--   2. events_recent (7-day TTL) for fast dashboards; insight_events for full history
--   3. Bloom filters on high-cardinality search columns
--   4. Projections for common access patterns
--   5. Retention/funnel/cohort use ClickHouse native functions at QUERY TIME
--      (retention(), windowFunnel()) — not pre-materialized
--   6. Standalone mode (no AI fields required) + Quorum-enhanced mode (AI fields populated)

-- ═══════════════════════════════════════════════════════════════════════
-- Main Events Table
-- ═══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS insight_events (
    -- Identity
    tenant_id       String,
    event_id        UUID,
    user_id         String          DEFAULT '',
    anonymous_id    String          DEFAULT '',

    -- Event
    event_name      String,
    event_type      LowCardinality(String),  -- pageview, identify, track, ai_generation, etc.
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

    -- AI Context (optional — the wedge into Quorum-enhanced mode)
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
    event_date      Date            DEFAULT toDate(timestamp),

    -- Bloom filter indexes for high-cardinality search columns
    INDEX idx_event_name event_name TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_user_id user_id TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_session_id session_id TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_ai_feature ai_feature TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_ai_model ai_model TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_source_system source_system TYPE set(20) GRANULARITY 4

) ENGINE = MergeTree()
PARTITION BY (tenant_id, toYYYYMM(timestamp))
ORDER BY (tenant_id, user_id, timestamp, event_id)
TTL toDateTime(timestamp) + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;


-- ═══════════════════════════════════════════════════════════════════════
-- Recent Events Table (7-day TTL)
-- Fast table for dashboards, live metrics, recent activity.
-- Populated by MV from insight_events.
-- ═══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS events_recent (
    tenant_id       String,
    event_id        UUID,
    user_id         String          DEFAULT '',
    anonymous_id    String          DEFAULT '',
    event_name      String,
    event_type      LowCardinality(String),
    timestamp       DateTime64(3, 'UTC'),
    session_id      String          DEFAULT '',
    page_url        String          DEFAULT '',
    page_path       String          DEFAULT '',
    source_system   LowCardinality(String),
    properties      Map(String, String),
    ai_model        String          DEFAULT '',
    ai_feature      String          DEFAULT '',
    ai_quality_score Float32        DEFAULT 0,
    event_date      Date            DEFAULT toDate(timestamp)
) ENGINE = MergeTree()
ORDER BY (tenant_id, timestamp, event_id)
TTL toDateTime(timestamp) + INTERVAL 7 DAY DELETE
SETTINGS index_granularity = 8192;


CREATE MATERIALIZED VIEW IF NOT EXISTS events_recent_mv
TO events_recent
AS SELECT
    tenant_id, event_id, user_id, anonymous_id,
    event_name, event_type, timestamp, session_id,
    page_url, page_path, source_system, properties,
    ai_model, ai_feature, ai_quality_score, event_date
FROM insight_events;


-- ═══════════════════════════════════════════════════════════════════════
-- User Profiles (AggregatingMergeTree — proper -State/-Merge)
-- ═══════════════════════════════════════════════════════════════════════
-- Query with: SELECT tenant_id, user_id,
--   minMerge(first_seen), maxMerge(last_seen), countMerge(event_count), ...
-- FROM user_profiles_mv GROUP BY tenant_id, user_id

CREATE MATERIALIZED VIEW IF NOT EXISTS user_profiles_mv
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, user_id)
AS SELECT
    tenant_id,
    user_id,
    minState(timestamp)                                         AS first_seen,
    maxState(timestamp)                                         AS last_seen,
    countState()                                                AS event_count,
    countIfState(ai_model != '')                                AS ai_events_count,
    avgIfState(ai_quality_score, ai_quality_score > 0)          AS ai_avg_quality,
    groupUniqArrayIfState(ai_feature, ai_feature != '')         AS ai_features_used,
    groupUniqArrayState(source_system)                          AS source_systems
FROM insight_events
WHERE user_id != ''
GROUP BY tenant_id, user_id;


-- ═══════════════════════════════════════════════════════════════════════
-- Daily Metrics (AggregatingMergeTree)
-- Pre-aggregated daily metrics per tenant for fast trend queries.
-- ═══════════════════════════════════════════════════════════════════════
-- Query with: SELECT tenant_id, event_date, event_type,
--   countMerge(event_count), uniqMerge(unique_users), ...
-- FROM daily_metrics_mv GROUP BY tenant_id, event_date, event_type

CREATE MATERIALIZED VIEW IF NOT EXISTS daily_metrics_mv
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, event_date, event_type)
AS SELECT
    tenant_id,
    event_date,
    event_type,
    countState()                                                AS event_count,
    uniqState(user_id)                                          AS unique_users,
    uniqState(session_id)                                       AS unique_sessions,
    countIfState(ai_model != '')                                AS ai_events,
    avgIfState(ai_quality_score, ai_quality_score > 0)          AS avg_ai_quality
FROM insight_events
GROUP BY tenant_id, event_date, event_type;


-- ═══════════════════════════════════════════════════════════════════════
-- User Cohorts (AggregatingMergeTree)
-- First-seen date per user, for retention curve computation.
-- Used by query-layer retention functions that JOIN this with events.
-- ═══════════════════════════════════════════════════════════════════════

CREATE MATERIALIZED VIEW IF NOT EXISTS user_cohorts_mv
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, user_id)
AS SELECT
    tenant_id,
    user_id,
    minState(event_date)                                        AS cohort_date,
    maxState(event_date)                                        AS last_active_date,
    countState()                                                AS lifetime_events
FROM insight_events
WHERE user_id != ''
GROUP BY tenant_id, user_id;


-- ═══════════════════════════════════════════════════════════════════════
-- Feature Usage (AggregatingMergeTree)
-- Per-feature usage and AI quality for feature impact analysis.
-- ═══════════════════════════════════════════════════════════════════════

CREATE MATERIALIZED VIEW IF NOT EXISTS feature_usage_mv
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, event_date, event_name)
AS SELECT
    tenant_id,
    event_date,
    event_name,
    countState()                                                AS usage_count,
    uniqState(user_id)                                          AS unique_users,
    countIfState(ai_model != '')                                AS ai_events,
    avgIfState(ai_quality_score, ai_quality_score > 0)          AS avg_ai_quality,
    sumIfState(ai_cost_usd, ai_cost_usd > 0)                   AS total_ai_cost
FROM insight_events
GROUP BY tenant_id, event_date, event_name;


-- ═══════════════════════════════════════════════════════════════════════
-- Daily Active Users (Projection on insight_events)
-- Enables fast DAU/WAU/MAU queries without separate MV.
-- ═══════════════════════════════════════════════════════════════════════

ALTER TABLE insight_events ADD PROJECTION IF NOT EXISTS proj_dau (
    SELECT
        tenant_id,
        event_date,
        uniq(user_id) AS dau
    GROUP BY tenant_id, event_date
);

-- Materialize the projection for existing data
ALTER TABLE insight_events MATERIALIZE PROJECTION proj_dau;


-- ═══════════════════════════════════════════════════════════════════════
-- QUERY-TIME ANALYTICS
-- ═══════════════════════════════════════════════════════════════════════
-- The following analyses are computed AT QUERY TIME using ClickHouse
-- native aggregate functions. This is deliberate — they're parameterized
-- queries that don't benefit from pre-materialization:
--
-- 1. RETENTION: retention(cond1, cond2, ..., condN) function
--    Returns array[N] with 1/0 for each condition met per user.
--    Grouped by cohort_week to build retention matrices.
--
-- 2. FUNNELS: windowFunnel(window)(timestamp, cond1, cond2, ..., condN)
--    Returns the max step reached within the time window.
--    Grouped by date/segment for conversion analysis.
--
-- 3. COHORT ANALYSIS: GROUP BY toStartOfWeek(first_event_date)
--    Uses user_cohorts_mv for first-seen dates joined with events.
--
-- See insights/query/ Python module for parameterized query builders.
