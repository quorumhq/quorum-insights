"""
Churn prediction from behavioral patterns.

Uses the Behavioral Decay Model (5 stages of disengagement)
with a signal-based scanner — NOT a black-box ML score.

Each user gets:
- health_score: 0-100 composite
- decay_stage: thriving/coasting/fading/ghosting/gone
- matched_signals: which specific patterns triggered
- trend: score trajectory (stable, declining, recovering)

Design principles (from research):
- "You don't need a score. You need a list of accounts with
   specific problems you can fix." (Cotera)
- Trend analysis > static thresholds (FirstDistro)
- Detect recency degradation first — it's the earliest signal
- Actionable output: what's wrong + what to do about it

Health score formula (adapted from FirstDistro Signal Stack):
  Score = Activity × 0.45 + FeatureBreadth × 0.35 + Recency × 0.20
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from enum import Enum
from typing import Optional

import polars as pl


class DecayStage(str, Enum):
    """Behavioral decay stages (sequential: recency → activity → engagement)."""
    THRIVING = "thriving"    # all signals stable/rising
    COASTING = "coasting"    # recency degrading, activity still OK
    FADING = "fading"        # activity + engagement declining
    GHOSTING = "ghosting"    # near-zero activity
    GONE = "gone"            # no activity in extended period


class ChurnSignal(str, Enum):
    """Specific detectable churn patterns."""
    USAGE_FREQUENCY_DROP = "usage_frequency_drop"    # WoW active days down >40%
    SESSION_GAP_GROWTH = "session_gap_growth"        # avg gap between sessions growing
    FEATURE_BREADTH_NARROW = "feature_breadth_narrow"  # using fewer distinct features
    ACTIVITY_VOLUME_DECLINE = "activity_volume_decline"  # total events down >50% WoW
    ABSENCE = "absence"                              # no activity in 14+ days


_STAGE_SAVE_RATE = {
    DecayStage.THRIVING: None,
    DecayStage.COASTING: "60-80%",
    DecayStage.FADING: "30-50%",
    DecayStage.GHOSTING: "10-20%",
    DecayStage.GONE: "5-10%",
}

_STAGE_WINDOW = {
    DecayStage.THRIVING: "No action needed",
    DecayStage.COASTING: "30-60 days to act",
    DecayStage.FADING: "14-30 days to act",
    DecayStage.GHOSTING: "7-14 days to act",
    DecayStage.GONE: "Last resort",
}


@dataclass
class UserChurnRisk:
    """Churn risk assessment for a single user."""

    user_id: str
    health_score: float  # 0-100
    decay_stage: DecayStage
    matched_signals: list[ChurnSignal]
    signal_details: dict[str, str]  # signal → human explanation
    trend: str  # "declining", "stable", "recovering"
    days_inactive: int
    activity_score: float  # 0-100 component
    feature_breadth_score: float  # 0-100 component
    recency_score: float  # 0-100 component

    @property
    def is_at_risk(self) -> bool:
        return self.decay_stage in (DecayStage.FADING, DecayStage.GHOSTING, DecayStage.GONE)

    @property
    def save_rate(self) -> Optional[str]:
        return _STAGE_SAVE_RATE.get(self.decay_stage)

    @property
    def action_window(self) -> str:
        return _STAGE_WINDOW.get(self.decay_stage, "")

    def to_dict(self) -> dict:
        return {
            "user_id": self.user_id,
            "health_score": round(self.health_score, 1),
            "decay_stage": self.decay_stage.value,
            "matched_signals": [s.value for s in self.matched_signals],
            "signal_details": self.signal_details,
            "trend": self.trend,
            "days_inactive": self.days_inactive,
            "is_at_risk": self.is_at_risk,
            "save_rate": self.save_rate,
            "action_window": self.action_window,
            "components": {
                "activity": round(self.activity_score, 1),
                "feature_breadth": round(self.feature_breadth_score, 1),
                "recency": round(self.recency_score, 1),
            },
        }


@dataclass
class ChurnCohort:
    """A group of users sharing the same churn pattern."""

    signal: ChurnSignal
    users: list[str]  # user_ids
    avg_health_score: float
    description: str

    def to_dict(self) -> dict:
        return {
            "signal": self.signal.value,
            "user_count": len(self.users),
            "avg_health_score": round(self.avg_health_score, 1),
            "description": self.description,
        }


@dataclass
class ChurnResult:
    """Complete churn analysis result."""

    users: list[UserChurnRisk]
    cohorts: list[ChurnCohort]
    total_users: int
    at_risk_count: int
    date_range: tuple[date, date]
    analysis_date: date

    def to_summary(self) -> dict:
        stage_counts = {}
        for stage in DecayStage:
            stage_counts[stage.value] = sum(1 for u in self.users if u.decay_stage == stage)

        return {
            "metric": "churn_prediction",
            "analysis_date": self.analysis_date.isoformat(),
            "date_range": {
                "start": self.date_range[0].isoformat(),
                "end": self.date_range[1].isoformat(),
            },
            "total_users": self.total_users,
            "at_risk_count": self.at_risk_count,
            "at_risk_pct": round(self.at_risk_count / max(self.total_users, 1), 4),
            "stage_distribution": stage_counts,
            "cohorts": [c.to_dict() for c in self.cohorts],
            "top_at_risk": [
                u.to_dict() for u in sorted(
                    self.users, key=lambda u: u.health_score
                )[:10]
            ],
        }


class ChurnDetector:
    """Detect churn risk from behavioral patterns.

    Uses the Behavioral Decay Model with configurable signal thresholds.
    No ML — pure pattern matching on time-series behavioral data.

    Usage:
        detector = ChurnDetector(events_df)
        result = detector.analyze()
        for user in result.users:
            if user.is_at_risk:
                print(f"{user.user_id}: {user.decay_stage} — {user.matched_signals}")
    """

    def __init__(
        self,
        events: pl.DataFrame | list[dict],
        analysis_date: date | None = None,
        # Signal thresholds
        usage_drop_pct: float = 0.40,       # WoW active days drop > 40%
        activity_decline_pct: float = 0.50,  # WoW event count drop > 50%
        absence_days: int = 14,              # no activity in N days
        breadth_decline_pct: float = 0.40,   # feature count drop > 40%
        lookback_weeks: int = 4,             # weeks of history to analyze
        # Health score weights
        w_activity: float = 0.45,
        w_breadth: float = 0.35,
        w_recency: float = 0.20,
    ):
        if isinstance(events, list):
            self._df = pl.DataFrame(events)
        else:
            self._df = events

        if "event_date" in self._df.columns and self._df["event_date"].dtype == pl.Utf8:
            self._df = self._df.with_columns(
                pl.col("event_date").str.to_date().alias("event_date")
            )

        self._analysis_date = analysis_date or (
            self._df["event_date"].max() if not self._df.is_empty() else date.today()
        )
        self._usage_drop = usage_drop_pct
        self._activity_decline = activity_decline_pct
        self._absence_days = absence_days
        self._breadth_decline = breadth_decline_pct
        self._lookback = lookback_weeks
        self._w = (w_activity, w_breadth, w_recency)

    def analyze(self) -> ChurnResult:
        """Run churn analysis on all users."""
        df = self._df.filter(pl.col("user_id") != "")

        if df.is_empty():
            return ChurnResult(
                users=[], cohorts=[], total_users=0, at_risk_count=0,
                date_range=(date.today(), date.today()),
                analysis_date=self._analysis_date,
            )

        date_range = (df["event_date"].min(), df["event_date"].max())
        analysis = self._analysis_date

        # Split into "recent" (last 2 weeks) and "prior" (2-4 weeks ago)
        recent_start = analysis - timedelta(days=14)
        prior_start = analysis - timedelta(days=self._lookback * 7)

        recent = df.filter(
            (pl.col("event_date") > recent_start) & (pl.col("event_date") <= analysis)
        )
        prior = df.filter(
            (pl.col("event_date") > prior_start) & (pl.col("event_date") <= recent_start)
        )

        # All users who were ever active in the lookback window
        all_user_ids = (
            df.filter(pl.col("event_date") > prior_start)
            .select("user_id").unique()["user_id"].to_list()
        )

        users: list[UserChurnRisk] = []
        for uid in all_user_ids:
            risk = self._assess_user(uid, recent, prior, df, analysis)
            users.append(risk)

        # Build cohorts
        cohorts = self._build_cohorts(users)

        at_risk = sum(1 for u in users if u.is_at_risk)

        return ChurnResult(
            users=users,
            cohorts=cohorts,
            total_users=len(users),
            at_risk_count=at_risk,
            date_range=date_range,
            analysis_date=analysis,
        )

    def _assess_user(
        self,
        user_id: str,
        recent: pl.DataFrame,
        prior: pl.DataFrame,
        all_events: pl.DataFrame,
        analysis_date: date,
    ) -> UserChurnRisk:
        """Assess churn risk for a single user."""
        user_recent = recent.filter(pl.col("user_id") == user_id)
        user_prior = prior.filter(pl.col("user_id") == user_id)
        user_all = all_events.filter(pl.col("user_id") == user_id)

        # Days inactive
        if user_all.is_empty():
            last_active = analysis_date - timedelta(days=999)
        else:
            last_active = user_all["event_date"].max()
        days_inactive = (analysis_date - last_active).days

        # Component scores
        activity_score = self._activity_score(user_recent, user_prior)
        breadth_score = self._breadth_score(user_recent, user_prior)
        recency_score = self._recency_score(days_inactive)

        # Composite health score
        w_a, w_b, w_r = self._w
        health = w_a * activity_score + w_b * breadth_score + w_r * recency_score

        # Detect specific signals
        signals, details = self._detect_signals(
            user_recent, user_prior, days_inactive,
            activity_score, breadth_score,
        )

        # Classify decay stage
        stage = self._classify_stage(health, days_inactive, signals)

        # Trend (compare health now vs what it would have been 2 weeks ago)
        trend = self._compute_trend(activity_score, breadth_score, recency_score)

        return UserChurnRisk(
            user_id=user_id,
            health_score=health,
            decay_stage=stage,
            matched_signals=signals,
            signal_details=details,
            trend=trend,
            days_inactive=days_inactive,
            activity_score=activity_score,
            feature_breadth_score=breadth_score,
            recency_score=recency_score,
        )

    def _activity_score(self, recent: pl.DataFrame, prior: pl.DataFrame) -> float:
        """Score 0-100 based on event count in recent vs prior period."""
        recent_count = len(recent)
        prior_count = len(prior)

        if prior_count == 0 and recent_count == 0:
            return 0.0
        if prior_count == 0:
            return 100.0  # new user, only recent activity

        ratio = recent_count / max(prior_count, 1)
        # Clamp to 0-100: ratio of 1.0+ = 100, ratio of 0 = 0
        return min(100.0, ratio * 100.0)

    def _breadth_score(self, recent: pl.DataFrame, prior: pl.DataFrame) -> float:
        """Score 0-100 based on distinct features used recently vs prior."""
        recent_features = recent["event_name"].n_unique() if len(recent) > 0 else 0
        prior_features = prior["event_name"].n_unique() if len(prior) > 0 else 0

        if prior_features == 0 and recent_features == 0:
            return 0.0
        if prior_features == 0:
            return 100.0

        ratio = recent_features / max(prior_features, 1)
        return min(100.0, ratio * 100.0)

    def _recency_score(self, days_inactive: int) -> float:
        """Score 0-100 based on days since last activity. Decays rapidly after 7 days."""
        if days_inactive <= 1:
            return 100.0
        elif days_inactive <= 3:
            return 90.0
        elif days_inactive <= 7:
            return 70.0
        elif days_inactive <= 14:
            return 40.0
        elif days_inactive <= 30:
            return 15.0
        elif days_inactive <= 60:
            return 5.0
        return 0.0

    def _detect_signals(
        self,
        recent: pl.DataFrame,
        prior: pl.DataFrame,
        days_inactive: int,
        activity_score: float,
        breadth_score: float,
    ) -> tuple[list[ChurnSignal], dict[str, str]]:
        """Detect which specific churn signals this user matches."""
        signals: list[ChurnSignal] = []
        details: dict[str, str] = {}

        recent_count = len(recent)
        prior_count = len(prior)

        # Absence
        if days_inactive >= self._absence_days:
            signals.append(ChurnSignal.ABSENCE)
            details[ChurnSignal.ABSENCE.value] = f"No activity in {days_inactive} days"

        # Usage frequency drop
        recent_days = recent["event_date"].n_unique() if len(recent) > 0 else 0
        prior_days = prior["event_date"].n_unique() if len(prior) > 0 else 0
        if prior_days > 0:
            day_drop = 1.0 - (recent_days / prior_days)
            if day_drop >= self._usage_drop:
                signals.append(ChurnSignal.USAGE_FREQUENCY_DROP)
                details[ChurnSignal.USAGE_FREQUENCY_DROP.value] = (
                    f"Active days dropped {day_drop:.0%} "
                    f"({prior_days}d → {recent_days}d)"
                )

        # Activity volume decline
        if prior_count > 0:
            vol_drop = 1.0 - (recent_count / prior_count)
            if vol_drop >= self._activity_decline:
                signals.append(ChurnSignal.ACTIVITY_VOLUME_DECLINE)
                details[ChurnSignal.ACTIVITY_VOLUME_DECLINE.value] = (
                    f"Event count dropped {vol_drop:.0%} "
                    f"({prior_count} → {recent_count})"
                )

        # Feature breadth narrowing
        recent_features = recent["event_name"].n_unique() if len(recent) > 0 else 0
        prior_features = prior["event_name"].n_unique() if len(prior) > 0 else 0
        if prior_features > 1:
            breadth_drop = 1.0 - (recent_features / prior_features)
            if breadth_drop >= self._breadth_decline:
                signals.append(ChurnSignal.FEATURE_BREADTH_NARROW)
                details[ChurnSignal.FEATURE_BREADTH_NARROW.value] = (
                    f"Features used dropped {breadth_drop:.0%} "
                    f"({prior_features} → {recent_features})"
                )

        # Session gap growth (recency-based)
        if recent_count > 0 and days_inactive >= 7 and prior_count > 0:
            # If they were active in prior but gaps are growing
            prior_gap = 14 / max(prior_days, 1) if prior_days > 0 else 14
            recent_gap = 14 / max(recent_days, 1) if recent_days > 0 else 14
            if recent_gap > prior_gap * 1.5:
                signals.append(ChurnSignal.SESSION_GAP_GROWTH)
                details[ChurnSignal.SESSION_GAP_GROWTH.value] = (
                    f"Avg days between sessions grew "
                    f"({prior_gap:.1f}d → {recent_gap:.1f}d)"
                )

        return signals, details

    def _classify_stage(
        self,
        health: float,
        days_inactive: int,
        signals: list[ChurnSignal],
    ) -> DecayStage:
        """Classify user into behavioral decay stage."""
        if days_inactive >= 30 or health < 10:
            return DecayStage.GONE
        if days_inactive >= 14 or health < 25:
            return DecayStage.GHOSTING
        if len(signals) >= 2 or health < 50:
            return DecayStage.FADING
        if len(signals) >= 1 or health < 70:
            return DecayStage.COASTING
        return DecayStage.THRIVING

    def _compute_trend(
        self,
        activity: float,
        breadth: float,
        recency: float,
    ) -> str:
        """Simple trend classification based on component scores."""
        avg = (activity + breadth + recency) / 3
        if avg >= 70:
            return "stable"
        elif avg >= 40:
            return "declining"
        else:
            return "declining"

    def _build_cohorts(self, users: list[UserChurnRisk]) -> list[ChurnCohort]:
        """Group users by shared churn signals for cohort-level alerts."""
        signal_users: dict[ChurnSignal, list[UserChurnRisk]] = {}
        for user in users:
            for signal in user.matched_signals:
                signal_users.setdefault(signal, []).append(user)

        descriptions = {
            ChurnSignal.USAGE_FREQUENCY_DROP: "Users whose login frequency dropped significantly",
            ChurnSignal.SESSION_GAP_GROWTH: "Users with growing gaps between sessions",
            ChurnSignal.FEATURE_BREADTH_NARROW: "Users retreating to fewer features",
            ChurnSignal.ACTIVITY_VOLUME_DECLINE: "Users generating significantly fewer events",
            ChurnSignal.ABSENCE: "Users with no recent activity",
        }

        cohorts = []
        for signal, signal_user_list in sorted(signal_users.items(), key=lambda x: -len(x[1])):
            avg_health = sum(u.health_score for u in signal_user_list) / len(signal_user_list)
            cohorts.append(ChurnCohort(
                signal=signal,
                users=[u.user_id for u in signal_user_list],
                avg_health_score=avg_health,
                description=descriptions.get(signal, signal.value),
            ))

        return cohorts
