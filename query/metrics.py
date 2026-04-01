"""
Metrics query builder — reads from pre-aggregated materialized views.

These queries read from explicit AggregatingMergeTree target tables
(not the MV definitions) with -Merge combinators for fast dashboards.

Usage:
    q = MetricsQuery(tenant_id="t1", start_date=date(2026, 1, 1), end_date=date(2026, 3, 31))
    sql, params = q.daily_active_users()
    sql, params = q.feature_usage_ranking()
    sql, params = q.user_profile("user-123")
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class MetricsQuery:
    """Build queries against pre-aggregated ClickHouse target tables."""

    tenant_id: str
    start_date: date
    end_date: date

    def _params(self) -> dict:
        return {
            "tenant_id": self.tenant_id,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
        }

    def daily_active_users(self) -> tuple[str, dict]:
        """DAU from daily_metrics (AggregatingMergeTree target)."""
        sql = """
SELECT
    event_date,
    uniqMerge(unique_users) AS dau
FROM daily_metrics
WHERE tenant_id = {tenant_id:String}
    AND event_date >= {start_date:Date}
    AND event_date <= {end_date:Date}
GROUP BY event_date
ORDER BY event_date
"""
        return sql.strip(), self._params()

    def daily_metrics_by_type(self) -> tuple[str, dict]:
        """Daily metrics broken down by event_type from daily_metrics."""
        sql = """
SELECT
    event_date,
    event_type,
    countMerge(event_count) AS events,
    uniqMerge(unique_users) AS users,
    uniqMerge(unique_sessions) AS sessions,
    countMerge(ai_events) AS ai_events,
    avgMerge(avg_ai_quality) AS avg_ai_quality
FROM daily_metrics
WHERE tenant_id = {tenant_id:String}
    AND event_date >= {start_date:Date}
    AND event_date <= {end_date:Date}
GROUP BY event_date, event_type
ORDER BY event_date, event_type
"""
        return sql.strip(), self._params()

    def feature_usage_ranking(self, limit: int = 50) -> tuple[str, dict]:
        """Top features by unique users from feature_usage."""
        sql = f"""
SELECT
    event_name,
    uniqMerge(unique_users) AS unique_users,
    countMerge(usage_count) AS total_usage,
    countMerge(ai_events) AS ai_events,
    avgMerge(avg_ai_quality) AS avg_ai_quality,
    sumMerge(total_ai_cost) AS total_ai_cost
FROM feature_usage
WHERE tenant_id = {{tenant_id:String}}
    AND event_date >= {{start_date:Date}}
    AND event_date <= {{end_date:Date}}
GROUP BY event_name
ORDER BY unique_users DESC
LIMIT {limit}
"""
        return sql.strip(), self._params()

    def feature_trend(self, event_name: str) -> tuple[str, dict]:
        """Daily trend for a specific feature from feature_usage."""
        params = self._params()
        params["event_name"] = event_name

        sql = """
SELECT
    event_date,
    uniqMerge(unique_users) AS unique_users,
    countMerge(usage_count) AS total_usage,
    avgMerge(avg_ai_quality) AS avg_ai_quality
FROM feature_usage
WHERE tenant_id = {tenant_id:String}
    AND event_date >= {start_date:Date}
    AND event_date <= {end_date:Date}
    AND event_name = {event_name:String}
GROUP BY event_date
ORDER BY event_date
"""
        return sql.strip(), params

    def user_profile(self, user_id: str) -> tuple[str, dict]:
        """Single user profile from user_profiles."""
        params = self._params()
        params["user_id"] = user_id

        sql = """
SELECT
    tenant_id,
    user_id,
    minMerge(first_seen) AS first_seen,
    maxMerge(last_seen) AS last_seen,
    countMerge(event_count) AS event_count,
    countMerge(ai_events_count) AS ai_events_count,
    avgMerge(ai_avg_quality) AS ai_avg_quality,
    groupUniqArrayMerge(ai_features_used) AS ai_features_used,
    groupUniqArrayMerge(source_systems) AS source_systems
FROM user_profiles
WHERE tenant_id = {tenant_id:String}
    AND user_id = {user_id:String}
GROUP BY tenant_id, user_id
"""
        return sql.strip(), params

    def user_cohort_info(self, user_id: str) -> tuple[str, dict]:
        """User cohort data from user_cohorts."""
        params = self._params()
        params["user_id"] = user_id

        sql = """
SELECT
    tenant_id,
    user_id,
    minMerge(cohort_date) AS cohort_date,
    maxMerge(last_active_date) AS last_active_date,
    countMerge(lifetime_events) AS lifetime_events
FROM user_cohorts
WHERE tenant_id = {tenant_id:String}
    AND user_id = {user_id:String}
GROUP BY tenant_id, user_id
"""
        return sql.strip(), params

    def overview(self) -> tuple[str, dict]:
        """High-level overview: total events, users, sessions, AI events."""
        sql = """
SELECT
    countMerge(event_count) AS total_events,
    uniqMerge(unique_users) AS total_users,
    uniqMerge(unique_sessions) AS total_sessions,
    countMerge(ai_events) AS total_ai_events,
    avgMerge(avg_ai_quality) AS overall_avg_ai_quality
FROM daily_metrics
WHERE tenant_id = {tenant_id:String}
    AND event_date >= {start_date:Date}
    AND event_date <= {end_date:Date}
"""
        return sql.strip(), self._params()

    def recent_events(self, limit: int = 100) -> tuple[str, dict]:
        """Recent events from events_recent (7-day TTL table)."""
        sql = f"""
SELECT
    timestamp,
    event_name,
    event_type,
    user_id,
    session_id,
    page_path,
    ai_model,
    ai_feature,
    ai_quality_score,
    source_system
FROM events_recent
WHERE tenant_id = {{tenant_id:String}}
ORDER BY timestamp DESC
LIMIT {limit}
"""
        params = {"tenant_id": self.tenant_id}
        return sql.strip(), params
