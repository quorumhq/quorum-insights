"""
Retention curve computation with cohort dimensions.

Takes raw event data (as polars DataFrame or list of dicts) and computes
D1/D7/D30/D90 retention rates per cohort. No live ClickHouse dependency —
this module does post-processing on query results.

The flow:
1. ClickHouse query (via query.retention or query.metrics) returns raw rows
2. This module processes them into RetentionResult with cohort breakdowns
3. Output is structured for the LLM insight engine

Usage:
    computer = RetentionComputer(events_df)
    result = computer.compute(periods=[1, 7, 30, 90])
    result.to_summary()  # -> dict for LLM engine
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Optional

import polars as pl


@dataclass
class CohortRetention:
    """Retention data for a single cohort."""

    cohort_key: str  # e.g. "2026-W01", "pro", "en-US"
    cohort_dimension: str  # e.g. "week", "plan", "locale"
    cohort_size: int
    retention_rates: dict[int, float]  # {1: 0.45, 7: 0.32, 30: 0.18, 90: 0.08}
    retention_counts: dict[int, int]  # {1: 450, 7: 320, 30: 180, 90: 80}


@dataclass
class RetentionResult:
    """Complete retention analysis result."""

    cohorts: list[CohortRetention]
    periods: list[int]  # [1, 7, 30, 90]
    total_users: int
    date_range: tuple[date, date]
    overall_retention: dict[int, float]  # weighted average across cohorts

    def to_summary(self) -> dict:
        """Structured summary for the LLM insight engine."""
        return {
            "metric": "retention",
            "date_range": {
                "start": self.date_range[0].isoformat(),
                "end": self.date_range[1].isoformat(),
            },
            "total_users": self.total_users,
            "periods": self.periods,
            "overall_retention": {
                f"D{p}": round(r, 4) for p, r in self.overall_retention.items()
            },
            "cohorts": [
                {
                    "key": c.cohort_key,
                    "dimension": c.cohort_dimension,
                    "size": c.cohort_size,
                    "retention": {
                        f"D{p}": round(r, 4)
                        for p, r in c.retention_rates.items()
                    },
                }
                for c in self.cohorts
            ],
            "best_cohort": self._best_cohort(),
            "worst_cohort": self._worst_cohort(),
        }

    def _best_cohort(self) -> Optional[dict]:
        if not self.cohorts or not self.periods:
            return None
        # Best by longest-term retention available
        target = self.periods[-1]
        candidates = [c for c in self.cohorts if target in c.retention_rates and c.cohort_size >= 10]
        if not candidates:
            return None
        best = max(candidates, key=lambda c: c.retention_rates[target])
        return {"key": best.cohort_key, "dimension": best.cohort_dimension,
                f"D{target}": round(best.retention_rates[target], 4)}

    def _worst_cohort(self) -> Optional[dict]:
        if not self.cohorts or not self.periods:
            return None
        target = self.periods[-1]
        candidates = [c for c in self.cohorts if target in c.retention_rates and c.cohort_size >= 10]
        if not candidates:
            return None
        worst = min(candidates, key=lambda c: c.retention_rates[target])
        return {"key": worst.cohort_key, "dimension": worst.cohort_dimension,
                f"D{target}": round(worst.retention_rates[target], 4)}


class RetentionComputer:
    """Compute retention curves from event data.

    Expects a polars DataFrame (or list of dicts) with at minimum:
    - user_id: str
    - event_date: date
    And optionally cohort dimension columns like:
    - plan, locale, segment, signup_week, etc.
    """

    def __init__(self, events: pl.DataFrame | list[dict]):
        if isinstance(events, list):
            self._df = pl.DataFrame(events)
        else:
            self._df = events

        # Ensure event_date is Date type
        if "event_date" in self._df.columns:
            if self._df["event_date"].dtype == pl.Utf8:
                self._df = self._df.with_columns(
                    pl.col("event_date").str.to_date().alias("event_date")
                )

    @property
    def df(self) -> pl.DataFrame:
        return self._df

    def compute(
        self,
        periods: list[int] | None = None,
        cohort_column: str | None = None,
    ) -> RetentionResult:
        """Compute retention curves.

        Args:
            periods: Day offsets to compute retention for. Default: [1, 7, 30, 90]
            cohort_column: Column to group cohorts by. None = signup week.
        """
        if periods is None:
            periods = [1, 7, 30, 90]

        df = self._df.filter(pl.col("user_id") != "")

        if df.is_empty():
            date_range = (date.today(), date.today())
            return RetentionResult(
                cohorts=[], periods=periods, total_users=0,
                date_range=date_range, overall_retention={p: 0.0 for p in periods},
            )

        # Compute first-seen date per user
        user_first = (
            df.group_by("user_id")
            .agg(pl.col("event_date").min().alias("first_date"))
        )

        # Join back to get cohort info
        joined = df.join(user_first, on="user_id")

        # Compute days since first seen
        joined = joined.with_columns(
            (pl.col("event_date") - pl.col("first_date")).dt.total_days().alias("days_since_first")
        )

        # Determine cohort grouping
        if cohort_column and cohort_column in joined.columns:
            dimension = cohort_column
            # Use first value of cohort_column per user
            user_cohort = (
                joined.sort("event_date")
                .group_by("user_id")
                .agg(pl.col(cohort_column).first().alias("cohort_key"))
            )
            joined = joined.join(user_cohort, on="user_id")
        else:
            dimension = "week"
            # Default: cohort by signup week
            joined = joined.with_columns(
                pl.col("first_date")
                .dt.truncate("1w")
                .cast(pl.Utf8)
                .alias("cohort_key")
            )

        date_range = (
            df["event_date"].min(),  # type: ignore[arg-type]
            df["event_date"].max(),  # type: ignore[arg-type]
        )
        total_users = df["user_id"].n_unique()

        # Compute retention per cohort
        cohort_keys = joined["cohort_key"].unique().sort().to_list()
        cohorts: list[CohortRetention] = []

        for key in cohort_keys:
            cohort_df = joined.filter(pl.col("cohort_key") == key)
            cohort_users = cohort_df["user_id"].n_unique()

            if cohort_users == 0:
                continue

            retention_rates: dict[int, float] = {}
            retention_counts: dict[int, int] = {}

            for period in periods:
                returning = (
                    cohort_df
                    .filter(pl.col("days_since_first") >= period)
                    ["user_id"]
                    .n_unique()
                )
                retention_counts[period] = returning
                retention_rates[period] = returning / cohort_users

            cohorts.append(CohortRetention(
                cohort_key=str(key),
                cohort_dimension=dimension,
                cohort_size=cohort_users,
                retention_rates=retention_rates,
                retention_counts=retention_counts,
            ))

        # Overall retention (weighted average)
        overall: dict[int, float] = {}
        for period in periods:
            total_returning = sum(c.retention_counts[period] for c in cohorts)
            overall[period] = total_returning / total_users if total_users > 0 else 0.0

        return RetentionResult(
            cohorts=cohorts,
            periods=periods,
            total_users=total_users,
            date_range=date_range,
            overall_retention=overall,
        )
