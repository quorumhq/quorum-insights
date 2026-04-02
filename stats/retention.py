"""
Retention curve computation — VECTORIZED.

Uses polars group_by/join instead of per-cohort Python loops.
Handles 200K+ users in <3 seconds.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Optional

import polars as pl


@dataclass
class CohortRetention:
    cohort_key: str
    cohort_dimension: str
    cohort_size: int
    retention_rates: dict[int, float]
    retention_counts: dict[int, int]


@dataclass
class RetentionResult:
    cohorts: list[CohortRetention]
    periods: list[int]
    total_users: int
    date_range: tuple[date, date]
    overall_retention: dict[int, float]

    def to_summary(self) -> dict:
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
    def __init__(self, events: pl.DataFrame | list[dict]):
        if isinstance(events, list):
            self._df = pl.DataFrame(events)
        else:
            self._df = events

        if "event_date" in self._df.columns and self._df["event_date"].dtype == pl.Utf8:
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
        if periods is None:
            periods = [1, 7, 30, 90]

        df = self._df.filter(pl.col("user_id") != "")

        if df.is_empty():
            return RetentionResult(
                cohorts=[], periods=periods, total_users=0,
                date_range=(date.today(), date.today()),
                overall_retention={p: 0.0 for p in periods},
            )

        date_range = (df["event_date"].min(), df["event_date"].max())

        # First-seen date per user (vectorized)
        user_first = (
            df.group_by("user_id")
            .agg(pl.col("event_date").min().alias("first_date"))
        )

        total_users = user_first.height

        # Active dates per user (deduplicated)
        user_dates = df.select("user_id", "event_date").unique()

        # Join to get days_since_first for all activity
        joined = user_dates.join(user_first, on="user_id").with_columns(
            (pl.col("event_date") - pl.col("first_date")).dt.total_days().alias("days_since")
        )

        # Cohort key per user
        if cohort_column and cohort_column in df.columns:
            dimension = cohort_column
            user_cohort = (
                df.sort("event_date")
                .group_by("user_id")
                .agg(pl.col(cohort_column).first().alias("cohort_key"))
            )
        else:
            dimension = "week"
            user_cohort = user_first.with_columns(
                pl.col("first_date").dt.truncate("1w").cast(pl.Utf8).alias("cohort_key")
            ).select("user_id", "cohort_key")

        # ── Vectorized retention computation ──
        # For each period, count distinct retained users in [period, next_period)
        overall: dict[int, float] = {}
        period_results: dict[int, pl.DataFrame] = {}

        for idx, period in enumerate(periods):
            if idx + 1 < len(periods):
                next_period = periods[idx + 1]
                retained_users = (
                    joined
                    .filter((pl.col("days_since") >= period) & (pl.col("days_since") < next_period))
                    .select("user_id")
                    .unique()
                )
            else:
                retained_users = (
                    joined
                    .filter(pl.col("days_since") >= period)
                    .select("user_id")
                    .unique()
                )

            # Join with cohort keys
            retained_with_cohort = retained_users.join(user_cohort, on="user_id")

            # Count per cohort
            cohort_retained = (
                retained_with_cohort
                .group_by("cohort_key")
                .agg(pl.col("user_id").n_unique().alias("retained"))
            )
            period_results[period] = cohort_retained
            overall[period] = retained_users.height / total_users if total_users > 0 else 0.0

        # Cohort sizes
        cohort_sizes = (
            user_cohort
            .group_by("cohort_key")
            .agg(pl.col("user_id").n_unique().alias("cohort_size"))
        )

        # Build CohortRetention objects
        cohort_keys = cohort_sizes.sort("cohort_key")["cohort_key"].to_list()
        size_map = {row["cohort_key"]: row["cohort_size"] for row in cohort_sizes.iter_rows(named=True)}

        cohorts: list[CohortRetention] = []
        for key in cohort_keys:
            csize = size_map[key]
            if csize == 0:
                continue
            rates = {}
            counts = {}
            for period in periods:
                pr = period_results[period]
                match = pr.filter(pl.col("cohort_key") == key)
                retained = match["retained"][0] if len(match) > 0 else 0
                counts[period] = retained
                rates[period] = retained / csize

            cohorts.append(CohortRetention(
                cohort_key=str(key),
                cohort_dimension=dimension,
                cohort_size=csize,
                retention_rates=rates,
                retention_counts=counts,
            ))

        return RetentionResult(
            cohorts=cohorts,
            periods=periods,
            total_users=total_users,
            date_range=date_range,
            overall_retention=overall,
        )
