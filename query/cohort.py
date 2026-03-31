"""
Cohort segmentation query builder.

Segments users into cohorts based on:
- First-seen date (acquisition cohorts)
- First event type (behavioral cohorts)
- Custom property (property cohorts)

Then compares metrics across cohorts.

Usage:
    q = CohortQuery(
        tenant_id="t1",
        start_date=date(2026, 1, 1),
        end_date=date(2026, 3, 31),
        cohort_by=CohortBy.FIRST_SEEN_WEEK,
    )
    sql, params = q.build()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Optional


class CohortBy(str, Enum):
    """How to segment users into cohorts."""

    FIRST_SEEN_DAY = "first_seen_day"
    FIRST_SEEN_WEEK = "first_seen_week"
    FIRST_SEEN_MONTH = "first_seen_month"
    FIRST_EVENT = "first_event"  # cohort by first event_name
    PROPERTY = "property"  # cohort by a user property key


_TRUNC_FN = {
    CohortBy.FIRST_SEEN_DAY: "toDate",
    CohortBy.FIRST_SEEN_WEEK: "toStartOfWeek",
    CohortBy.FIRST_SEEN_MONTH: "toStartOfMonth",
}


@dataclass
class CohortQuery:
    """Build cohort comparison queries."""

    tenant_id: str
    start_date: date
    end_date: date
    cohort_by: CohortBy = CohortBy.FIRST_SEEN_WEEK
    property_key: Optional[str] = None  # required when cohort_by == PROPERTY
    event_filter: Optional[str] = None
    limit_cohorts: int = 20

    def __post_init__(self):
        if self.cohort_by == CohortBy.PROPERTY and not self.property_key:
            raise ValueError("property_key required when cohort_by is PROPERTY")

    def _base_where(self, alias: str = "e") -> str:
        clauses = [
            f"{alias}.tenant_id = {{tenant_id:String}}",
            f"{alias}.event_date >= {{start_date:Date}}",
            f"{alias}.event_date <= {{end_date:Date}}",
            f"{alias}.user_id != ''",
        ]
        if self.event_filter:
            clauses.append(f"({self.event_filter})")
        return " AND ".join(clauses)

    def _params(self) -> dict:
        return {
            "tenant_id": self.tenant_id,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
        }

    def build(self) -> tuple[str, dict]:
        """Cohort summary: size, activity, AI usage per cohort."""
        where = self._base_where("e")

        if self.cohort_by in _TRUNC_FN:
            trunc = _TRUNC_FN[self.cohort_by]
            cohort_expr = f"{trunc}(min(e.event_date))"
            cohort_label = "cohort_date"
        elif self.cohort_by == CohortBy.FIRST_EVENT:
            cohort_expr = "argMin(e.event_name, e.timestamp)"
            cohort_label = "first_event"
        else:
            # PROPERTY — look up in user_properties_set or properties
            cohort_expr = (
                f"argMin(e.user_properties_set['{self.property_key}'], e.timestamp)"
            )
            cohort_label = f"property_{self.property_key}"

        sql = f"""
WITH user_cohorts AS (
    SELECT
        e.user_id,
        {cohort_expr} AS cohort_value,
        min(e.event_date) AS first_active,
        max(e.event_date) AS last_active,
        count() AS event_count,
        countIf(e.ai_model != '') AS ai_event_count,
        uniq(e.event_name) AS distinct_events,
        uniq(e.session_id) AS sessions
    FROM insight_events e
    WHERE {where}
    GROUP BY e.user_id
)
SELECT
    cohort_value AS {cohort_label},
    count() AS cohort_size,
    avg(event_count) AS avg_events_per_user,
    avg(ai_event_count) AS avg_ai_events_per_user,
    avg(distinct_events) AS avg_distinct_events,
    avg(sessions) AS avg_sessions,
    avg(dateDiff('day', first_active, last_active)) AS avg_lifespan_days,
    countIf(last_active >= today() - 7) AS active_last_7d
FROM user_cohorts
GROUP BY cohort_value
ORDER BY cohort_size DESC
LIMIT {self.limit_cohorts}
"""

        return sql.strip(), self._params()

    def build_comparison(self, metric_event: str) -> tuple[str, dict]:
        """Compare a specific metric event across cohorts.

        Shows how different cohorts engage with a particular feature/event.
        """
        where = self._base_where("e")

        if self.cohort_by in _TRUNC_FN:
            trunc = _TRUNC_FN[self.cohort_by]
            cohort_expr = f"{trunc}(min(e.event_date))"
        elif self.cohort_by == CohortBy.FIRST_EVENT:
            cohort_expr = "argMin(e.event_name, e.timestamp)"
        else:
            cohort_expr = (
                f"argMin(e.user_properties_set['{self.property_key}'], e.timestamp)"
            )

        params = self._params()
        params["metric_event"] = metric_event

        sql = f"""
WITH user_cohorts AS (
    SELECT
        e.user_id,
        {cohort_expr} AS cohort_value
    FROM insight_events e
    WHERE {where}
    GROUP BY e.user_id
)
SELECT
    uc.cohort_value,
    count(DISTINCT uc.user_id) AS cohort_size,
    countIf(e.event_name = {{metric_event:String}}) AS metric_events,
    count(DISTINCT if(e.event_name = {{metric_event:String}}, e.user_id, NULL)) AS users_with_metric,
    countIf(e.event_name = {{metric_event:String}}) / greatest(count(DISTINCT uc.user_id), 1) AS metric_rate
FROM user_cohorts uc
INNER JOIN insight_events e
    ON e.user_id = uc.user_id
    AND e.tenant_id = {{tenant_id:String}}
    AND e.event_date >= {{start_date:Date}}
    AND e.event_date <= {{end_date:Date}}
GROUP BY uc.cohort_value
ORDER BY cohort_size DESC
LIMIT {self.limit_cohorts}
"""

        return sql.strip(), params
