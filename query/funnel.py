"""
Funnel query builder using ClickHouse windowFunnel() aggregate function.

windowFunnel(window_seconds)(timestamp, cond1, cond2, ..., condN)
Returns the maximum step index reached within the time window.

Usage:
    q = FunnelQuery(
        tenant_id="t1",
        steps=[
            FunnelStep("signup", "event_name = 'signup'"),
            FunnelStep("activate", "event_name = 'first_action'"),
            FunnelStep("subscribe", "event_name = 'subscription_started'"),
        ],
        window_seconds=7 * 86400,  # 7 days
        start_date=date(2026, 1, 1),
        end_date=date(2026, 3, 31),
    )
    sql, params = q.build()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Optional


@dataclass
class FunnelStep:
    """A single funnel step with a label and SQL condition."""

    label: str
    condition: str  # SQL boolean expression on insight_events columns

    def __post_init__(self):
        if not self.label.strip():
            raise ValueError("FunnelStep label cannot be empty")
        if not self.condition.strip():
            raise ValueError("FunnelStep condition cannot be empty")


@dataclass
class FunnelQuery:
    """Build a funnel analysis query.

    Produces two result sets:
    1. build() — per-user max step reached, aggregated into step counts
    2. build_by_date() — funnel by date for trend analysis
    """

    tenant_id: str
    steps: list[FunnelStep]
    window_seconds: int = 7 * 86400  # default 7 days
    start_date: date = field(default_factory=lambda: date(2026, 1, 1))
    end_date: date = field(default_factory=lambda: date(2026, 12, 31))
    segment_filter: Optional[str] = None  # additional WHERE clause

    def __post_init__(self):
        if len(self.steps) < 2:
            raise ValueError("Funnel requires at least 2 steps")
        if len(self.steps) > 20:
            raise ValueError("Funnel limited to 20 steps")
        if self.window_seconds <= 0:
            raise ValueError("window_seconds must be positive")

    def _where_clauses(self) -> list[str]:
        clauses = [
            "tenant_id = {tenant_id:String}",
            "event_date >= {start_date:Date}",
            "event_date <= {end_date:Date}",
            "user_id != ''",
        ]
        if self.segment_filter:
            clauses.append(f"({self.segment_filter})")
        return clauses

    def _params(self) -> dict:
        return {
            "tenant_id": self.tenant_id,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
        }

    def build(self) -> tuple[str, dict]:
        """Aggregate funnel: count of users reaching each step."""
        where_sql = " AND ".join(self._where_clauses())
        step_conds = ",\n            ".join(s.condition for s in self.steps)

        # windowFunnel returns max step index (0-based or 1-based depends on version)
        # We aggregate into counts per step level
        step_count_cases = []
        for i, step in enumerate(self.steps):
            step_count_cases.append(
                f"countIf(max_step >= {i + 1}) AS step_{i + 1}_{_sanitize_label(step.label)}"
            )

        step_counts_sql = ",\n    ".join(step_count_cases)

        sql = f"""
WITH funnel AS (
    SELECT
        user_id,
        windowFunnel({self.window_seconds})(
            timestamp,
            {step_conds}
        ) AS max_step
    FROM insight_events
    WHERE {where_sql}
    GROUP BY user_id
)
SELECT
    count() AS total_entered,
    {step_counts_sql}
FROM funnel
WHERE max_step >= 1
"""

        return sql.strip(), self._params()

    def build_by_date(self) -> tuple[str, dict]:
        """Funnel broken down by entry date for trend analysis."""
        where_sql = " AND ".join(self._where_clauses())
        step_conds = ",\n            ".join(s.condition for s in self.steps)

        step_count_cases = []
        for i, step in enumerate(self.steps):
            step_count_cases.append(
                f"countIf(max_step >= {i + 1}) AS step_{i + 1}_{_sanitize_label(step.label)}"
            )

        step_counts_sql = ",\n    ".join(step_count_cases)

        sql = f"""
WITH funnel AS (
    SELECT
        user_id,
        min(event_date) AS entry_date,
        windowFunnel({self.window_seconds})(
            timestamp,
            {step_conds}
        ) AS max_step
    FROM insight_events
    WHERE {where_sql}
    GROUP BY user_id
)
SELECT
    entry_date,
    count() AS total_entered,
    {step_counts_sql}
FROM funnel
WHERE max_step >= 1
GROUP BY entry_date
ORDER BY entry_date
"""

        return sql.strip(), self._params()

    def step_labels(self) -> list[str]:
        """Return ordered list of step labels for UI rendering."""
        return [s.label for s in self.steps]


def _sanitize_label(label: str) -> str:
    """Sanitize a label for use as a SQL column alias."""
    return "".join(c if c.isalnum() or c == "_" else "_" for c in label.lower())
