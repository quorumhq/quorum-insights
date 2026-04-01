"""
Activation moment discovery engine.

Finds: "Users who do X within Y days retain at Zx the rate."

Algorithm:
1. For each event_name, compute:
   - adoption_rate: % of users who did it within the activation window
   - retention_rate_adopters: D30+ retention of users who did it
   - retention_rate_non_adopters: D30+ retention of users who didn't
   - lift: retention_adopters / retention_non_adopters
2. For event combinations (A AND B within window):
   - Same metrics, but for users who did BOTH events
3. Filter:
   - Remove trivially obvious events (configurable blocklist)
   - Remove statistically insignificant patterns (chi-squared p < 0.05)
   - Remove patterns with adoption < min_adoption or > max_adoption
4. Rank by lift × adoption headroom (room to improve adoption)

This is correlation, not causation. The output frames it as "associated with"
and recommends A/B testing to validate causal claims.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import polars as pl


@dataclass
class ActivationMoment:
    """A discovered activation pattern."""

    events: list[str]  # single event or combination
    min_frequency: int  # must do event >= N times (1 = at least once)
    window_days: int  # within N days of signup
    adoption_rate: float  # % of users who met the threshold
    retention_adopters: float  # retention rate of adopters
    retention_non_adopters: float  # retention rate of non-adopters
    lift: float  # retention_adopters / retention_non_adopters
    correlation: float  # Matthews Correlation Coefficient (phi)
    adopter_count: int
    non_adopter_count: int
    p_value: float  # chi-squared significance
    projected_impact: Optional[float] = None  # if adoption went to 100%

    @property
    def is_combination(self) -> bool:
        return len(self.events) > 1

    @property
    def adoption_headroom(self) -> float:
        """Room to improve adoption (1 - adoption_rate)."""
        return 1.0 - self.adoption_rate

    @property
    def impact_score(self) -> float:
        """Lift × headroom — higher means more opportunity."""
        return (self.lift - 1.0) * self.adoption_headroom

    @property
    def label(self) -> str:
        """Human-readable label: 'search ≥3 times' or 'search + invite ≥1 time'."""
        event_str = " + ".join(self.events)
        return f"{event_str} ≥{self.min_frequency} time{'s' if self.min_frequency > 1 else ''}"

    def to_dict(self) -> dict:
        return {
            "events": self.events,
            "min_frequency": self.min_frequency,
            "label": self.label,
            "window_days": self.window_days,
            "adoption_rate": round(self.adoption_rate, 4),
            "retention_adopters": round(self.retention_adopters, 4),
            "retention_non_adopters": round(self.retention_non_adopters, 4),
            "lift": round(self.lift, 2),
            "correlation": round(self.correlation, 4),
            "adopter_count": self.adopter_count,
            "non_adopter_count": self.non_adopter_count,
            "p_value": round(self.p_value, 6),
            "correlation": round(self.correlation, 4),
            "impact_score": round(self.impact_score, 4),
            "is_combination": self.is_combination,
        }


@dataclass
class ActivationResult:
    """Complete activation analysis result."""

    moments: list[ActivationMoment]
    total_users: int
    activation_window_days: int
    retention_period_days: int
    baseline_retention: float  # overall retention rate
    date_range: tuple[date, date]

    def to_summary(self) -> dict:
        """Structured summary for the LLM insight engine."""
        return {
            "metric": "activation_discovery",
            "caveat": "Correlation, not causation. Validate with A/B tests.",
            "date_range": {
                "start": self.date_range[0].isoformat(),
                "end": self.date_range[1].isoformat(),
            },
            "total_users": self.total_users,
            "activation_window_days": self.activation_window_days,
            "retention_period_days": self.retention_period_days,
            "baseline_retention": round(self.baseline_retention, 4),
            "moments_found": len(self.moments),
            "top_moments": [m.to_dict() for m in self.moments[:10]],
        }


# Default blocklist of trivially obvious events
DEFAULT_TRIVIAL_EVENTS = frozenset({
    "pageview", "$pageview", "page_view",
    "click", "$click",
    "session_start", "$session_start", "session_end",
    "identify", "$identify",
    "signup", "sign_up", "$create_alias",
})


def _matthews_correlation(tp: int, fp: int, fn: int, tn: int) -> float:
    """Matthews Correlation Coefficient (MCC) for a 2×2 contingency table.

    MCC = (TP×TN − FP×FN) / √((TP+FP)(TP+FN)(TN+FP)(TN+FN))

    Range: -1 to 1. Higher = better predictive pattern.
    Equivalent to phi coefficient for binary variables.
    Used by Amplitude Compass for ranking activation patterns.
    """
    import math

    denom = math.sqrt(
        (tp + fp) * (tp + fn) * (tn + fp) * (tn + fn)
    ) if (tp + fp) * (tp + fn) * (tn + fp) * (tn + fn) > 0 else 0

    if denom == 0:
        return 0.0

    return (tp * tn - fp * fn) / denom


def _chi_squared_p_value(
    adopters_retained: int,
    adopters_total: int,
    non_adopters_retained: int,
    non_adopters_total: int,
) -> float:
    """Compute chi-squared p-value for 2×2 contingency table.

    Uses the scipy-free formula for a 2×2 chi-squared test.
    Returns p-value (lower = more significant).
    """
    # 2×2 table:
    #                Retained  Not Retained
    # Adopters       a         b
    # Non-adopters   c         d
    a = adopters_retained
    b = adopters_total - adopters_retained
    c = non_adopters_retained
    d = non_adopters_total - non_adopters_retained
    n = a + b + c + d

    if n == 0 or (a + b) == 0 or (c + d) == 0:
        return 1.0

    # Expected values
    e_a = (a + b) * (a + c) / n
    e_b = (a + b) * (b + d) / n
    e_c = (c + d) * (a + c) / n
    e_d = (c + d) * (b + d) / n

    if any(e == 0 for e in [e_a, e_b, e_c, e_d]):
        return 1.0

    # Chi-squared statistic
    chi2 = (
        (a - e_a) ** 2 / e_a
        + (b - e_b) ** 2 / e_b
        + (c - e_c) ** 2 / e_c
        + (d - e_d) ** 2 / e_d
    )

    # Approximate p-value from chi-squared with 1 df
    # Using the survival function approximation
    import math
    if chi2 <= 0:
        return 1.0
    # For 1 df: p ≈ erfc(sqrt(chi2/2))
    p = math.erfc(math.sqrt(chi2 / 2))
    return p


class ActivationDiscovery:
    """Discover activation moments from event data.

    Usage:
        discovery = ActivationDiscovery(events_df)
        result = discovery.discover(
            activation_window=7,
            retention_period=30,
        )
        for m in result.moments:
            print(f"{m.events}: {m.lift:.1f}x lift, {m.adoption_rate:.0%} adoption")
    """

    def __init__(
        self,
        events: pl.DataFrame | list[dict],
        trivial_events: frozenset[str] | None = None,
        min_adopters: int = 10,
        min_non_adopters: int = 10,
        max_adoption_rate: float = 0.95,
        min_adoption_rate: float = 0.02,
        p_value_threshold: float = 0.05,
        frequency_thresholds: list[int] | None = None,
    ):
        if isinstance(events, list):
            self._df = pl.DataFrame(events)
        else:
            self._df = events

        if "event_date" in self._df.columns and self._df["event_date"].dtype == pl.Utf8:
            self._df = self._df.with_columns(
                pl.col("event_date").str.to_date().alias("event_date")
            )

        self._trivial = trivial_events if trivial_events is not None else DEFAULT_TRIVIAL_EVENTS
        self._min_adopters = min_adopters
        self._min_non_adopters = min_non_adopters
        self._max_adoption = max_adoption_rate
        self._min_adoption = min_adoption_rate
        self._p_threshold = p_value_threshold
        self._freq_thresholds = frequency_thresholds or [1, 2, 3, 5, 10]

    def discover(
        self,
        activation_window: int = 7,
        retention_period: int = 30,
        max_combinations: int = 2,
        max_results: int = 20,
    ) -> ActivationResult:
        """Discover activation moments.

        Args:
            activation_window: Days after signup to look for activation events
            retention_period: Days after signup to measure retention
            max_combinations: Max events in a combination (1=singles only, 2=pairs)
            max_results: Max moments to return
        """
        df = self._df.filter(pl.col("user_id") != "")

        if df.is_empty():
            return ActivationResult(
                moments=[], total_users=0,
                activation_window_days=activation_window,
                retention_period_days=retention_period,
                baseline_retention=0.0,
                date_range=(date.today(), date.today()),
            )

        date_range = (df["event_date"].min(), df["event_date"].max())

        # Compute first-seen date per user
        user_first = (
            df.group_by("user_id")
            .agg(pl.col("event_date").min().alias("first_date"))
        )

        # Join to get days_since_first
        joined = df.join(user_first, on="user_id").with_columns(
            (pl.col("event_date") - pl.col("first_date")).dt.total_days().alias("days_since")
        )

        total_users = user_first.height

        # Compute baseline retention (users active >= retention_period days after first)
        user_retained = (
            joined
            .filter(pl.col("days_since") >= retention_period)
            .select("user_id")
            .unique()
        )
        baseline_retained = user_retained.height
        baseline_retention = baseline_retained / total_users if total_users > 0 else 0.0

        # Events within activation window (excluding trivial)
        activation_events = (
            joined
            .filter(
                (pl.col("days_since") >= 0)
                & (pl.col("days_since") <= activation_window)
                & (~pl.col("event_name").is_in(list(self._trivial)))
            )
        )

        # Per-user event COUNTS in window (not just unique events)
        user_event_counts = (
            activation_events
            .group_by("user_id", "event_name")
            .agg(pl.len().alias("event_count"))
        )

        # Get candidate events with enough adopters (at threshold=1)
        event_user_counts = (
            user_event_counts
            .group_by("event_name")
            .agg(pl.col("user_id").n_unique().alias("adopters"))
            .filter(pl.col("adopters") >= self._min_adopters)
            .sort("adopters", descending=True)
        )

        candidate_events = event_user_counts["event_name"].to_list()

        # Discover single-event moments at each frequency threshold
        moments: list[ActivationMoment] = []
        for event_name in candidate_events:
            for freq in self._freq_thresholds:
                moment = self._evaluate_pattern(
                    [event_name],
                    freq,
                    user_event_counts,
                    user_first,
                    user_retained,
                    total_users,
                    activation_window,
                    retention_period,
                )
                if moment is not None:
                    moments.append(moment)

        # Discover 2-event combinations (at freq=1 only to avoid explosion)
        if max_combinations >= 2 and len(candidate_events) >= 2:
            top_events = candidate_events[:30]
            for i, e1 in enumerate(top_events):
                for e2 in top_events[i + 1:]:
                    moment = self._evaluate_pattern(
                        [e1, e2],
                        1,  # combinations at frequency >= 1
                        user_event_counts,
                        user_first,
                        user_retained,
                        total_users,
                        activation_window,
                        retention_period,
                    )
                    if moment is not None:
                        moments.append(moment)

        # Deduplicate: for same event, keep only the best frequency threshold
        moments = self._deduplicate_frequencies(moments)

        # Sort by MCC correlation (matches Amplitude Compass ranking)
        moments.sort(key=lambda m: m.correlation, reverse=True)

        return ActivationResult(
            moments=moments[:max_results],
            total_users=total_users,
            activation_window_days=activation_window,
            retention_period_days=retention_period,
            baseline_retention=baseline_retention,
            date_range=date_range,
        )

    def _deduplicate_frequencies(self, moments: list[ActivationMoment]) -> list[ActivationMoment]:
        """For same event set, keep only the frequency with best MCC."""
        best: dict[str, ActivationMoment] = {}
        for m in moments:
            key = tuple(sorted(m.events))
            key_str = str(key)
            if key_str not in best or m.correlation > best[key_str].correlation:
                best[key_str] = m
        return list(best.values())

    def _evaluate_pattern(
        self,
        events: list[str],
        min_frequency: int,
        user_event_counts: pl.DataFrame,
        user_first: pl.DataFrame,
        user_retained: pl.DataFrame,
        total_users: int,
        activation_window: int,
        retention_period: int,
    ) -> Optional[ActivationMoment]:
        """Evaluate an event pattern at a specific frequency threshold."""

        # Find users who did ALL events >= min_frequency times in the window
        if len(events) == 1:
            adopter_ids = (
                user_event_counts
                .filter(
                    (pl.col("event_name") == events[0])
                    & (pl.col("event_count") >= min_frequency)
                )
                .select("user_id")
                .unique()
            )
        else:
            # Users who did EACH event >= min_frequency times
            adopter_ids = (
                user_event_counts
                .filter(
                    (pl.col("event_name") == events[0])
                    & (pl.col("event_count") >= min_frequency)
                )
                .select("user_id")
            )
            for event in events[1:]:
                other = (
                    user_event_counts
                    .filter(
                        (pl.col("event_name") == event)
                        & (pl.col("event_count") >= min_frequency)
                    )
                    .select("user_id")
                )
                adopter_ids = adopter_ids.join(other, on="user_id", how="inner")
            adopter_ids = adopter_ids.unique()

        adopter_count = adopter_ids.height
        non_adopter_count = total_users - adopter_count

        if adopter_count < self._min_adopters:
            return None
        if non_adopter_count < self._min_non_adopters:
            return None

        adoption_rate = adopter_count / total_users
        if adoption_rate > self._max_adoption or adoption_rate < self._min_adoption:
            return None

        # Retention for adopters vs non-adopters
        adopters_retained = adopter_ids.join(user_retained, on="user_id", how="inner").height
        non_adopter_ids = user_first.select("user_id").join(adopter_ids, on="user_id", how="anti")
        non_adopters_retained = non_adopter_ids.join(user_retained, on="user_id", how="inner").height

        ret_adopters = adopters_retained / adopter_count if adopter_count > 0 else 0.0
        ret_non_adopters = non_adopters_retained / non_adopter_count if non_adopter_count > 0 else 0.0

        if ret_non_adopters == 0:
            lift = float("inf") if ret_adopters > 0 else 1.0
        else:
            lift = ret_adopters / ret_non_adopters

        # Must have meaningful lift (at least 10% better)
        if lift < 1.1:
            return None

        # Chi-squared significance test
        p_value = _chi_squared_p_value(
            adopters_retained, adopter_count,
            non_adopters_retained, non_adopter_count,
        )

        if p_value >= self._p_threshold:
            return None

        # Matthews Correlation Coefficient (MCC / phi)
        # Contingency table: TP=adopters_retained, FP=adopters_not_retained,
        #   FN=non_adopters_retained, TN=non_adopters_not_retained
        tp = adopters_retained
        fp = adopter_count - adopters_retained
        fn = non_adopters_retained
        tn = non_adopter_count - non_adopters_retained
        mcc = _matthews_correlation(tp, fp, fn, tn)

        # Projected impact: if ALL non-adopters adopted, how much would retention improve?
        projected_retention = ret_adopters  # optimistic: assume they'd match adopter retention
        projected_impact = projected_retention - (
            ret_adopters * adoption_rate + ret_non_adopters * (1 - adoption_rate)
        )

        return ActivationMoment(
            events=events,
            min_frequency=min_frequency,
            window_days=activation_window,
            adoption_rate=adoption_rate,
            retention_adopters=ret_adopters,
            retention_non_adopters=ret_non_adopters,
            lift=lift,
            correlation=mcc,
            adopter_count=adopter_count,
            non_adopter_count=non_adopter_count,
            p_value=p_value,
            projected_impact=projected_impact,
        )
