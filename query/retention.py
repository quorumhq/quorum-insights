"""
Retention query builder using ClickHouse retention() aggregate function.

ClickHouse's retention(cond1, cond2, ..., condN) returns an array of UInt8
where each element is 1 if the corresponding condition was met BY THAT USER.
We group by cohort period to build a classic retention matrix.

Usage:
    q = RetentionQuery(
        tenant_id="t1",
        start_date=date(2026, 1, 1),
        end_date=date(2026, 3, 31),
        period=RetentionPeriod.WEEK,
        num_periods=8,
    )
    sql, params = q.build()
    # Execute sql with params against ClickHouse
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Optional


class RetentionPeriod(str, Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"


# ClickHouse date-truncation functions per period
_TRUNC_FN = {
    RetentionPeriod.DAY: "toDate",
    RetentionPeriod.WEEK: "toStartOfWeek",
    RetentionPeriod.MONTH: "toStartOfMonth",
}

_INTERVAL_EXPR = {
    RetentionPeriod.DAY: "INTERVAL {n} DAY",
    RetentionPeriod.WEEK: "INTERVAL {n} WEEK",
    RetentionPeriod.MONTH: "INTERVAL {n} MONTH",
}


@dataclass
class RetentionQuery:
    """Build a retention matrix query.

    The query:
    1. Finds each user's cohort date (first event in window)
    2. Uses retention() to check activity in each subsequent period
    3. Groups by cohort period for the retention matrix
    """

    tenant_id: str
    start_date: date
    end_date: date
    period: RetentionPeriod = RetentionPeriod.WEEK
    num_periods: int = 8
    event_filter: Optional[str] = None  # e.g. "event_name = 'login'"
    ai_only: bool = False  # Quorum-enhanced: only users with AI events

    def build(self) -> tuple[str, dict]:
        """Return (sql, params) for ClickHouse execution."""
        trunc = _TRUNC_FN[self.period]
        interval_tmpl = _INTERVAL_EXPR[self.period]

        # Build retention conditions: retention(
        #   event_date >= cohort_start,
        #   event_date >= cohort_start + 1 WEEK,
        #   event_date >= cohort_start + 2 WEEK,
        #   ...
        # )
        # We use a subquery approach: first find cohort_date per user,
        # then compute retention periods relative to that.

        where_clauses = [
            "ie.tenant_id = {tenant_id:String}",
            "ie.event_date >= {start_date:Date}",
            "ie.event_date <= {end_date:Date}",
            "ie.user_id != ''",
        ]
        if self.event_filter:
            where_clauses.append(f"({self.event_filter})")
        if self.ai_only:
            where_clauses.append("ie.ai_model != ''")

        where_sql = " AND ".join(where_clauses)

        # Build retention conditions: each checks a BOUNDED time range
        # so a user active only in week 3 counts for week 3 only, not weeks 0-3.
        # Condition N: event_date >= cohort_date + N periods AND < cohort_date + (N+1) periods
        retention_conds = []
        for i in range(self.num_periods):
            lo = interval_tmpl.format(n=i)
            hi = interval_tmpl.format(n=i + 1)
            retention_conds.append(
                f"ie.event_date >= cohorts.cohort_date + {lo} "
                f"AND ie.event_date < cohorts.cohort_date + {hi}"
            )

        retention_args = ",\n            ".join(retention_conds)

        sql = f"""
WITH cohorts AS (
    SELECT
        user_id,
        {trunc}(min(event_date)) AS cohort_date
    FROM insight_events ie
    WHERE {where_sql}
    GROUP BY user_id
)
SELECT
    cohorts.cohort_date,
    countDistinct(cohorts.user_id) AS cohort_size,
    sumForEach(
        arrayMap(x -> toUInt64(x),
            retention(
                {retention_args}
            )
        )
    ) AS retention_array
FROM insight_events ie
INNER JOIN cohorts ON ie.user_id = cohorts.user_id
WHERE {where_sql}
GROUP BY cohorts.cohort_date
ORDER BY cohorts.cohort_date
"""

        params = {
            "tenant_id": self.tenant_id,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
        }

        return sql.strip(), params

    def build_simple(self) -> tuple[str, dict]:
        """Simpler retention query using manual date arithmetic.

        This variant doesn't use the retention() aggregate but instead
        computes a retention matrix with explicit JOINs. More portable
        and easier to debug.
        """
        trunc = _TRUNC_FN[self.period]
        interval_tmpl = _INTERVAL_EXPR[self.period]

        where_clauses = [
            "e.tenant_id = {tenant_id:String}",
            "e.event_date >= {start_date:Date}",
            "e.event_date <= {end_date:Date}",
            "e.user_id != ''",
        ]
        if self.event_filter:
            where_clauses.append(f"({self.event_filter})")
        if self.ai_only:
            where_clauses.append("e.ai_model != ''")

        where_sql = " AND ".join(where_clauses)

        # For each period offset, count returning users
        period_selects = []
        for i in range(self.num_periods):
            interval = interval_tmpl.format(n=i)
            period_selects.append(
                f"countDistinctIf(e.user_id, e.event_date >= c.cohort_date + {interval} "
                f"AND e.event_date < c.cohort_date + {interval_tmpl.format(n=i+1)}) AS period_{i}"
            )

        periods_sql = ",\n    ".join(period_selects)

        sql = f"""
WITH cohorts AS (
    SELECT
        user_id,
        {trunc}(min(event_date)) AS cohort_date
    FROM insight_events e
    WHERE {where_sql}
    GROUP BY user_id
)
SELECT
    c.cohort_date,
    count(DISTINCT c.user_id) AS cohort_size,
    {periods_sql}
FROM cohorts c
INNER JOIN insight_events e
    ON e.user_id = c.user_id
    AND e.tenant_id = {{tenant_id:String}}
    AND e.event_date >= {{start_date:Date}}
    AND e.event_date <= {{end_date:Date}}
GROUP BY c.cohort_date
ORDER BY c.cohort_date
"""

        params = {
            "tenant_id": self.tenant_id,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
        }

        return sql.strip(), params
