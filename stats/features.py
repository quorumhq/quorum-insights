"""
Feature–retention correlation analysis.

Answers: "Which features are associated with higher or lower retention?"

IMPORTANT: This is CORRELATION, not causal impact. Users who adopt a feature
may differ systematically from non-users (self-selection bias). Tenure
normalization reduces but does not eliminate confounding. For causal claims,
use A/B testing or propensity score matching.

Algorithm:
1. For each feature (event_name), split users into used/not-used groups
2. Compare D7/D30 retention between groups
3. Normalize for confounders (user tenure via weekly cohort matching)
4. Rank by net retention correlation
5. Flag negative-correlation features

Usage:
    analyzer = FeatureCorrelationAnalyzer(events_df)
    result = analyzer.analyze(retention_periods=[7, 30])
    result.to_summary()  # -> dict for LLM engine
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import polars as pl


@dataclass
class FeatureCorrelation:
    """Impact analysis for a single feature."""

    feature_name: str
    total_users: int  # users who used this feature
    total_events: int  # total usage count

    # Retention impact: difference between users/non-users
    retention_impact: dict[int, float]  # {7: +0.05, 30: +0.03} = 5% better D7 retention
    retention_users: dict[int, float]  # {7: 0.45, 30: 0.22} = actual retention of users
    retention_non_users: dict[int, float]  # {7: 0.40, 30: 0.19}

    # Normalized impact (after confounder adjustment)
    normalized_impact: dict[int, float]  # {7: +0.04, 30: +0.02}

    # AI quality (if available)
    avg_ai_quality: Optional[float] = None
    ai_event_pct: float = 0.0  # % of events with AI context

    @property
    def is_negative(self) -> bool:
        """Feature hurts retention at the longest measured period."""
        if not self.normalized_impact:
            return False
        longest = max(self.normalized_impact.keys())
        return self.normalized_impact[longest] < -0.01  # >1% negative

    @property
    def net_correlation_score(self) -> float:
        """Single score for ranking: weighted average of normalized impacts."""
        if not self.normalized_impact:
            return 0.0
        # Weight longer periods more heavily
        weights = {7: 1.0, 30: 2.0, 90: 3.0}
        total_w = 0.0
        total_v = 0.0
        for period, impact in self.normalized_impact.items():
            w = weights.get(period, 1.0)
            total_w += w
            total_v += w * impact
        return total_v / total_w if total_w > 0 else 0.0


@dataclass
class FeatureCorrelationResult:
    """Complete feature impact analysis result."""

    features: list[FeatureCorrelation]
    periods: list[int]
    total_users: int
    total_features: int
    date_range: tuple[date, date]

    @property
    def positive_features(self) -> list[FeatureCorrelation]:
        return [f for f in self.features if f.net_correlation_score > 0.01]

    @property
    def negative_features(self) -> list[FeatureCorrelation]:
        return [f for f in self.features if f.is_negative]

    @property
    def ranked(self) -> list[FeatureCorrelation]:
        """Features ranked by net impact (best first)."""
        return sorted(self.features, key=lambda f: f.net_correlation_score, reverse=True)

    def to_summary(self) -> dict:
        """Structured summary for the LLM insight engine."""
        ranked = self.ranked
        return {
            "metric": "feature_correlation",
            "caveat": "Correlation, not causation. Feature users may differ "
                      "systematically from non-users. Use A/B tests for causal claims.",
            "date_range": {
                "start": self.date_range[0].isoformat(),
                "end": self.date_range[1].isoformat(),
            },
            "total_users": self.total_users,
            "total_features": self.total_features,
            "periods": [f"D{p}" for p in self.periods],
            "positive_count": len(self.positive_features),
            "negative_count": len(self.negative_features),
            "top_features": [
                {
                    "name": f.feature_name,
                    "users": f.total_users,
                    "events": f.total_events,
                    "impact": {f"D{p}": round(v, 4) for p, v in f.normalized_impact.items()},
                    "net_score": round(f.net_correlation_score, 4),
                }
                for f in ranked[:10]
            ],
            "negative_features": [
                {
                    "name": f.feature_name,
                    "users": f.total_users,
                    "impact": {f"D{p}": round(v, 4) for p, v in f.normalized_impact.items()},
                    "net_score": round(f.net_correlation_score, 4),
                }
                for f in self.negative_features
            ],
        }


class FeatureCorrelationAnalyzer:
    """Compute feature impact on retention.

    Expects a polars DataFrame with at minimum:
    - user_id: str
    - event_date: date
    - event_name: str
    And optionally:
    - segment/plan: str (for confounder normalization)
    - ai_quality_score: float (for AI apps)
    """

    def __init__(
        self,
        events: pl.DataFrame | list[dict],
        min_feature_users: int = 5,
        min_non_users: int = 5,
    ):
        if isinstance(events, list):
            self._df = pl.DataFrame(events)
        else:
            self._df = events

        if "event_date" in self._df.columns and self._df["event_date"].dtype == pl.Utf8:
            self._df = self._df.with_columns(
                pl.col("event_date").str.to_date().alias("event_date")
            )

        self.min_feature_users = min_feature_users
        self.min_non_users = min_non_users

    def analyze(
        self,
        retention_periods: list[int] | None = None,
        segment_column: str | None = None,
        exclude_events: list[str] | None = None,
    ) -> FeatureCorrelationResult:
        """Analyze feature impact on retention.

        Args:
            retention_periods: Day offsets to measure retention. Default: [7, 30]
            segment_column: Column to normalize within (e.g. "plan")
            exclude_events: Event names to exclude from features (e.g. system events)
        """
        if retention_periods is None:
            retention_periods = [7, 30]

        df = self._df.filter(pl.col("user_id") != "")

        if df.is_empty():
            return FeatureCorrelationResult(
                features=[], periods=retention_periods, total_users=0,
                total_features=0, date_range=(date.today(), date.today()),
            )

        # Filter out excluded events
        if exclude_events:
            df = df.filter(~pl.col("event_name").is_in(exclude_events))

        date_range = (df["event_date"].min(), df["event_date"].max())
        total_users = df["user_id"].n_unique()

        # Compute first-seen date per user
        user_first = (
            df.group_by("user_id")
            .agg(pl.col("event_date").min().alias("first_date"))
        )

        # Build user-level activity: all dates active
        user_dates = (
            df.select("user_id", "event_date")
            .unique()
        )

        # Compute user-level retention for each period
        user_retention = user_first.clone()
        for period in retention_periods:
            # A user is "retained at D{period}" if they have activity >= first_date + period days
            retained = (
                user_dates
                .join(user_first, on="user_id")
                .filter(
                    (pl.col("event_date") - pl.col("first_date")).dt.total_days() >= period
                )
                .select("user_id")
                .unique()
                .with_columns(pl.lit(True).alias(f"retained_d{period}"))
            )
            user_retention = user_retention.join(retained, on="user_id", how="left")
            user_retention = user_retention.with_columns(
                pl.col(f"retained_d{period}").fill_null(False)
            )

        # For each feature, compute impact
        features_list = (
            df.group_by("event_name")
            .agg([
                pl.col("user_id").n_unique().alias("unique_users"),
                pl.len().alias("event_count"),
            ])
            .filter(pl.col("unique_users") >= self.min_feature_users)
            .sort("unique_users", descending=True)
        )

        # Get AI quality if available
        has_ai = "ai_quality_score" in df.columns

        # Users per feature
        feature_users = (
            df.select("user_id", "event_name")
            .unique()
        )

        results: list[FeatureCorrelation] = []

        for row in features_list.iter_rows(named=True):
            feat_name = row["event_name"]
            feat_user_count = row["unique_users"]
            feat_event_count = row["event_count"]

            # Users of this feature
            users_with = (
                feature_users
                .filter(pl.col("event_name") == feat_name)
                .select("user_id")
                .unique()
            )

            # Users without this feature
            all_user_ids = user_retention.select("user_id")
            users_without = all_user_ids.join(users_with, on="user_id", how="anti")

            if len(users_without) < self.min_non_users:
                continue

            # Retention for users WITH feature
            ret_with = user_retention.join(users_with, on="user_id", how="semi")
            # Retention for users WITHOUT feature
            ret_without = user_retention.join(users_without, on="user_id", how="semi")

            retention_impact = {}
            retention_users_dict = {}
            retention_non_users_dict = {}

            for period in retention_periods:
                col = f"retained_d{period}"
                rate_with = ret_with[col].mean() if len(ret_with) > 0 else 0.0
                rate_without = ret_without[col].mean() if len(ret_without) > 0 else 0.0

                # Handle None from empty columns
                rate_with = rate_with if rate_with is not None else 0.0
                rate_without = rate_without if rate_without is not None else 0.0

                retention_impact[period] = rate_with - rate_without
                retention_users_dict[period] = rate_with
                retention_non_users_dict[period] = rate_without

            # Normalized impact (tenure-adjusted)
            # Skip expensive normalization for large datasets (>50K users)
            if total_users <= 50000:
                normalized = self._normalize_impact(
                    df, user_first, users_with, users_without,
                    retention_periods, segment_column,
                )
            else:
                normalized = retention_impact  # use raw correlation for perf

            # AI quality
            avg_ai_quality = None
            ai_event_pct = 0.0
            if has_ai:
                feat_events = df.filter(pl.col("event_name") == feat_name)
                ai_events = feat_events.filter(pl.col("ai_quality_score") > 0)
                if len(ai_events) > 0:
                    avg_ai_quality = ai_events["ai_quality_score"].mean()
                    ai_event_pct = len(ai_events) / len(feat_events)

            results.append(FeatureCorrelation(
                feature_name=feat_name,
                total_users=feat_user_count,
                total_events=feat_event_count,
                retention_impact=retention_impact,
                retention_users=retention_users_dict,
                retention_non_users=retention_non_users_dict,
                normalized_impact=normalized,
                avg_ai_quality=avg_ai_quality,
                ai_event_pct=ai_event_pct,
            ))

        return FeatureCorrelationResult(
            features=results,
            periods=retention_periods,
            total_users=total_users,
            total_features=len(results),
            date_range=date_range,
        )

    def _normalize_impact(
        self,
        df: pl.DataFrame,
        user_first: pl.DataFrame,
        users_with: pl.DataFrame,
        users_without: pl.DataFrame,
        periods: list[int],
        segment_column: str | None,
    ) -> dict[int, float]:
        """Tenure-normalized impact: compare users with similar first_date.

        Groups users into tenure cohorts (weekly) and computes impact
        within each cohort, then averages. This controls for the confounder
        that early adopters both use more features AND retain better.
        """
        # Add first_date to user sets
        with_tenure = users_with.join(user_first, on="user_id")
        without_tenure = users_without.join(user_first, on="user_id")

        # Truncate to weekly cohorts
        with_tenure = with_tenure.with_columns(
            pl.col("first_date").dt.truncate("1w").alias("tenure_cohort")
        )
        without_tenure = without_tenure.with_columns(
            pl.col("first_date").dt.truncate("1w").alias("tenure_cohort")
        )

        # Build user activity dates for retention check
        user_dates = df.select("user_id", "event_date").unique()

        normalized: dict[int, float] = {}
        for period in periods:
            # For each tenure cohort, compute retention delta
            cohort_deltas: list[tuple[float, int]] = []  # (delta, weight)

            # Get all tenure cohorts that have both users
            with_cohorts = set(with_tenure["tenure_cohort"].unique().to_list())
            without_cohorts = set(without_tenure["tenure_cohort"].unique().to_list())
            shared_cohorts = with_cohorts & without_cohorts

            for cohort_val in shared_cohorts:
                cw = with_tenure.filter(pl.col("tenure_cohort") == cohort_val)
                cwout = without_tenure.filter(pl.col("tenure_cohort") == cohort_val)

                if len(cw) < 2 or len(cwout) < 2:
                    continue

                # Retention for this cohort's users
                rate_w = self._retention_rate(
                    cw.select("user_id", "first_date"), user_dates, period
                )
                rate_wo = self._retention_rate(
                    cwout.select("user_id", "first_date"), user_dates, period
                )

                weight = min(len(cw), len(cwout))
                cohort_deltas.append((rate_w - rate_wo, weight))

            if cohort_deltas:
                total_weight = sum(w for _, w in cohort_deltas)
                normalized[period] = sum(d * w for d, w in cohort_deltas) / total_weight
            else:
                normalized[period] = 0.0

        return normalized

    @staticmethod
    def _retention_rate(
        users_df: pl.DataFrame,  # user_id, first_date
        user_dates: pl.DataFrame,  # user_id, event_date
        period: int,
    ) -> float:
        """Compute retention rate at D{period} for a set of users."""
        joined = users_df.join(user_dates, on="user_id")
        retained = joined.filter(
            (pl.col("event_date") - pl.col("first_date")).dt.total_days() >= period
        )
        total = users_df["user_id"].n_unique()
        if total == 0:
            return 0.0
        return retained["user_id"].n_unique() / total
