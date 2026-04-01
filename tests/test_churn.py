"""Tests for churn prediction from behavioral patterns."""

from datetime import date, timedelta

import polars as pl
import pytest

from stats.churn import (
    ChurnDetector,
    ChurnResult,
    UserChurnRisk,
    ChurnCohort,
    DecayStage,
    ChurnSignal,
)


# ─── Test Data ───


def _make_churn_events(analysis_date: date = date(2026, 3, 1)) -> pl.DataFrame:
    """Synthetic events with users at different decay stages.

    - u_thriving: active every day, many features
    - u_coasting: was active, gaps growing
    - u_fading: activity declining, fewer features
    - u_ghosting: barely active
    - u_gone: no activity in 30+ days
    """
    rows = []

    # u_thriving: active every day for 4 weeks, 5 features
    for d in range(28):
        dt = analysis_date - timedelta(days=d)
        for feat in ["search", "dashboard", "export", "settings", "chat"]:
            if d % 3 == 0 or feat == "search":  # search daily, others every 3 days
                rows.append({"user_id": "u_thriving", "event_date": dt, "event_name": feat})

    # u_coasting: active in prior period, gaps in recent
    for d in range(14, 28):
        dt = analysis_date - timedelta(days=d)
        rows.append({"user_id": "u_coasting", "event_date": dt, "event_name": "search"})
        if d % 4 == 0:
            rows.append({"user_id": "u_coasting", "event_date": dt, "event_name": "dashboard"})
    # Only 2 days active in recent period
    rows.append({"user_id": "u_coasting", "event_date": analysis_date - timedelta(days=3), "event_name": "search"})
    rows.append({"user_id": "u_coasting", "event_date": analysis_date - timedelta(days=10), "event_name": "search"})

    # u_fading: was active with many features, now barely active with 1 feature
    for d in range(14, 28):
        dt = analysis_date - timedelta(days=d)
        for feat in ["search", "dashboard", "export"]:
            rows.append({"user_id": "u_fading", "event_date": dt, "event_name": feat})
    # Recent: only 1 event
    rows.append({"user_id": "u_fading", "event_date": analysis_date - timedelta(days=5), "event_name": "search"})

    # u_ghosting: last activity 16 days ago
    rows.append({"user_id": "u_ghosting", "event_date": analysis_date - timedelta(days=16), "event_name": "search"})
    rows.append({"user_id": "u_ghosting", "event_date": analysis_date - timedelta(days=20), "event_name": "search"})

    # u_gone: last activity 25 days ago (within 4-week lookback)
    rows.append({"user_id": "u_gone", "event_date": analysis_date - timedelta(days=25), "event_name": "search"})
    rows.append({"user_id": "u_gone", "event_date": analysis_date - timedelta(days=26), "event_name": "dashboard"})

    return pl.DataFrame(rows)


# ─── Tests ───


class TestChurnDetector:

    def test_basic_analysis(self):
        events = _make_churn_events()
        detector = ChurnDetector(events, analysis_date=date(2026, 3, 1))
        result = detector.analyze()

        assert isinstance(result, ChurnResult)
        assert result.total_users == 5
        assert result.at_risk_count > 0

    def test_thriving_user(self):
        events = _make_churn_events()
        detector = ChurnDetector(events, analysis_date=date(2026, 3, 1))
        result = detector.analyze()

        thriving = next(u for u in result.users if u.user_id == "u_thriving")
        assert thriving.decay_stage == DecayStage.THRIVING
        assert thriving.health_score >= 70
        assert len(thriving.matched_signals) == 0
        assert not thriving.is_at_risk

    def test_coasting_user(self):
        events = _make_churn_events()
        detector = ChurnDetector(events, analysis_date=date(2026, 3, 1))
        result = detector.analyze()

        coasting = next(u for u in result.users if u.user_id == "u_coasting")
        assert coasting.decay_stage in (DecayStage.COASTING, DecayStage.FADING)
        assert len(coasting.matched_signals) >= 1

    def test_fading_user(self):
        events = _make_churn_events()
        detector = ChurnDetector(events, analysis_date=date(2026, 3, 1))
        result = detector.analyze()

        fading = next(u for u in result.users if u.user_id == "u_fading")
        assert fading.decay_stage in (DecayStage.FADING, DecayStage.COASTING)
        assert fading.is_at_risk or len(fading.matched_signals) >= 1

    def test_ghosting_user(self):
        events = _make_churn_events()
        detector = ChurnDetector(events, analysis_date=date(2026, 3, 1))
        result = detector.analyze()

        ghosting = next(u for u in result.users if u.user_id == "u_ghosting")
        assert ghosting.decay_stage in (DecayStage.GHOSTING, DecayStage.GONE)
        assert ghosting.is_at_risk
        assert ChurnSignal.ABSENCE in ghosting.matched_signals

    def test_gone_user(self):
        events = _make_churn_events()
        detector = ChurnDetector(events, analysis_date=date(2026, 3, 1))
        result = detector.analyze()

        gone = next(u for u in result.users if u.user_id == "u_gone")
        assert gone.decay_stage in (DecayStage.GONE, DecayStage.GHOSTING)
        assert gone.is_at_risk
        assert gone.health_score < 20
        assert gone.days_inactive >= 25

    def test_health_scores_ordered(self):
        """Thriving should have highest score, gone should have lowest."""
        events = _make_churn_events()
        detector = ChurnDetector(events, analysis_date=date(2026, 3, 1))
        result = detector.analyze()

        scores = {u.user_id: u.health_score for u in result.users}
        assert scores["u_thriving"] > scores["u_gone"]
        assert scores["u_thriving"] > scores["u_ghosting"]

    def test_at_least_3_signals(self):
        """Must detect at least 3 distinct signal types (acceptance criteria)."""
        events = _make_churn_events()
        detector = ChurnDetector(events, analysis_date=date(2026, 3, 1))
        result = detector.analyze()

        all_signals = set()
        for u in result.users:
            all_signals.update(u.matched_signals)
        assert len(all_signals) >= 3, f"Only detected signals: {all_signals}"

    def test_cohort_aggregation(self):
        """Users grouped by shared churn signal."""
        events = _make_churn_events()
        detector = ChurnDetector(events, analysis_date=date(2026, 3, 1))
        result = detector.analyze()

        assert len(result.cohorts) >= 1
        for cohort in result.cohorts:
            assert len(cohort.users) >= 1
            assert cohort.description != ""

    def test_signal_details_human_readable(self):
        """Signal details should contain human-readable explanations."""
        events = _make_churn_events()
        detector = ChurnDetector(events, analysis_date=date(2026, 3, 1))
        result = detector.analyze()

        for user in result.users:
            for signal, detail in user.signal_details.items():
                assert len(detail) > 10, f"Signal {signal} detail too short: {detail}"

    def test_to_summary(self):
        events = _make_churn_events()
        detector = ChurnDetector(events, analysis_date=date(2026, 3, 1))
        result = detector.analyze()
        summary = result.to_summary()

        assert summary["metric"] == "churn_prediction"
        assert "stage_distribution" in summary
        assert "cohorts" in summary
        assert "top_at_risk" in summary
        assert summary["total_users"] == 5
        assert summary["at_risk_count"] > 0

    def test_to_dict(self):
        events = _make_churn_events()
        detector = ChurnDetector(events, analysis_date=date(2026, 3, 1))
        result = detector.analyze()

        for user in result.users:
            d = user.to_dict()
            assert "health_score" in d
            assert "decay_stage" in d
            assert "matched_signals" in d
            assert "signal_details" in d
            assert "components" in d
            assert "is_at_risk" in d
            assert "action_window" in d

    def test_empty_events(self):
        events = pl.DataFrame({
            "user_id": [], "event_date": [], "event_name": [],
        }).cast({"user_id": pl.Utf8, "event_date": pl.Date, "event_name": pl.Utf8})
        detector = ChurnDetector(events)
        result = detector.analyze()
        assert result.total_users == 0

    def test_from_list_of_dicts(self):
        events = [
            {"user_id": "u1", "event_date": date(2026, 2, 15), "event_name": "login"},
            {"user_id": "u1", "event_date": date(2026, 2, 20), "event_name": "search"},
        ]
        detector = ChurnDetector(events, analysis_date=date(2026, 3, 1))
        result = detector.analyze()
        assert result.total_users >= 1

    def test_configurable_thresholds(self):
        events = _make_churn_events()
        # Very strict thresholds
        detector = ChurnDetector(
            events, analysis_date=date(2026, 3, 1),
            usage_drop_pct=0.90,  # only flag 90%+ drops
            absence_days=30,      # only flag 30+ day absence
        )
        result = detector.analyze()
        # Should detect fewer signals with stricter thresholds
        strict_signals = sum(len(u.matched_signals) for u in result.users)

        detector2 = ChurnDetector(
            events, analysis_date=date(2026, 3, 1),
            usage_drop_pct=0.20,  # flag 20%+ drops
            absence_days=7,       # flag 7+ day absence
        )
        result2 = detector2.analyze()
        loose_signals = sum(len(u.matched_signals) for u in result2.users)

        assert loose_signals >= strict_signals


class TestDecayStage:
    def test_all_stages_present(self):
        """All 5 decay stages should be used."""
        assert len(DecayStage) == 5
        assert DecayStage.THRIVING in DecayStage
        assert DecayStage.GONE in DecayStage

    def test_save_rates(self):
        from stats.churn import _STAGE_SAVE_RATE
        assert _STAGE_SAVE_RATE[DecayStage.COASTING] == "60-80%"
        assert _STAGE_SAVE_RATE[DecayStage.GONE] == "5-10%"


class TestChurnCohort:
    def test_to_dict(self):
        cohort = ChurnCohort(
            signal=ChurnSignal.ABSENCE,
            users=["u1", "u2", "u3"],
            avg_health_score=25.5,
            description="Users with no recent activity",
        )
        d = cohort.to_dict()
        assert d["signal"] == "absence"
        assert d["user_count"] == 3
        assert d["avg_health_score"] == 25.5
