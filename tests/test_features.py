"""Tests for feature impact correlation analysis."""

from datetime import date, timedelta

import polars as pl
import pytest

from stats.features import FeatureCorrelationAnalyzer, FeatureCorrelationResult, FeatureCorrelation


# ─── Test Data Helpers ───


def _make_feature_events(
    num_users: int = 200,
    start: date = date(2026, 1, 1),
    days: int = 60,
) -> pl.DataFrame:
    """Synthetic events where 'good_feature' improves retention and 'bad_feature' hurts it.

    - Users who use 'good_feature' have 60% D7 retention
    - Users who use 'bad_feature' have 15% D7 retention
    - Users who use neither have 30% D7 retention
    - 'neutral_feature' has no impact
    """
    import random
    random.seed(42)

    rows = []
    for uid in range(num_users):
        signup_day = random.randint(0, 14)
        signup_date = start + timedelta(days=signup_day)

        # Assign feature group
        group = random.choices(
            ["good", "bad", "neutral", "none"],
            weights=[30, 20, 25, 25],
        )[0]

        # Signup event
        rows.append({
            "user_id": f"u{uid}",
            "event_date": signup_date,
            "event_name": "signup",
        })

        # Feature usage event
        if group == "good":
            rows.append({
                "user_id": f"u{uid}",
                "event_date": signup_date + timedelta(days=random.randint(0, 2)),
                "event_name": "good_feature",
            })
            retention_prob = 0.6
        elif group == "bad":
            rows.append({
                "user_id": f"u{uid}",
                "event_date": signup_date + timedelta(days=random.randint(0, 2)),
                "event_name": "bad_feature",
            })
            retention_prob = 0.15
        elif group == "neutral":
            rows.append({
                "user_id": f"u{uid}",
                "event_date": signup_date + timedelta(days=random.randint(0, 2)),
                "event_name": "neutral_feature",
            })
            retention_prob = 0.30
        else:
            retention_prob = 0.30

        # Return activity based on retention probability
        for d in [7, 8, 9, 14, 21, 30]:
            if random.random() < retention_prob:
                rows.append({
                    "user_id": f"u{uid}",
                    "event_date": signup_date + timedelta(days=d),
                    "event_name": random.choice(["pageview", "click"]),
                })

    return pl.DataFrame(rows)


def _make_feature_events_with_ai(num_users: int = 100) -> pl.DataFrame:
    """Events with AI quality scores for AI-feature analysis."""
    import random
    random.seed(42)

    rows = []
    for uid in range(num_users):
        signup_date = date(2026, 1, 1) + timedelta(days=random.randint(0, 14))

        rows.append({
            "user_id": f"u{uid}",
            "event_date": signup_date,
            "event_name": "signup",
            "ai_quality_score": 0.0,
        })

        # AI feature with quality score
        if random.random() < 0.5:
            rows.append({
                "user_id": f"u{uid}",
                "event_date": signup_date + timedelta(days=1),
                "event_name": "ai_search",
                "ai_quality_score": random.uniform(0.7, 0.95),
            })

            # Higher quality → better retention
            if random.random() < 0.5:
                rows.append({
                    "user_id": f"u{uid}",
                    "event_date": signup_date + timedelta(days=8),
                    "event_name": "pageview",
                    "ai_quality_score": 0.0,
                })

    return pl.DataFrame(rows)


def _make_segmented_events(num_users: int = 200) -> pl.DataFrame:
    """Events with segment column for confounder testing."""
    import random
    random.seed(42)

    rows = []
    for uid in range(num_users):
        plan = random.choice(["free", "pro"])
        signup_date = date(2026, 1, 1) + timedelta(days=random.randint(0, 14))

        rows.append({
            "user_id": f"u{uid}",
            "event_date": signup_date,
            "event_name": "signup",
            "plan": plan,
        })

        if random.random() < 0.4:
            rows.append({
                "user_id": f"u{uid}",
                "event_date": signup_date + timedelta(days=1),
                "event_name": "feature_x",
                "plan": plan,
            })

        # Pro users retain better regardless of feature
        retention_prob = 0.5 if plan == "pro" else 0.2
        for d in [7, 14, 30]:
            if random.random() < retention_prob:
                rows.append({
                    "user_id": f"u{uid}",
                    "event_date": signup_date + timedelta(days=d),
                    "event_name": "pageview",
                    "plan": plan,
                })

    return pl.DataFrame(rows)


# ─── Tests ───


class TestFeatureCorrelationAnalyzer:
    """Test feature impact correlation."""

    def test_basic_analysis(self):
        events = _make_feature_events(num_users=200)
        analyzer = FeatureCorrelationAnalyzer(events)
        result = analyzer.analyze(retention_periods=[7, 30])

        assert isinstance(result, FeatureCorrelationResult)
        assert result.total_users > 0
        assert result.total_features > 0
        assert result.periods == [7, 30]

    def test_good_feature_positive_impact(self):
        """good_feature should have positive retention impact."""
        events = _make_feature_events(num_users=300)
        analyzer = FeatureCorrelationAnalyzer(events, min_feature_users=3)
        result = analyzer.analyze(retention_periods=[7])

        good = next((f for f in result.features if f.feature_name == "good_feature"), None)
        assert good is not None
        assert good.retention_impact[7] > 0, f"Expected positive D7 impact, got {good.retention_impact[7]}"

    def test_bad_feature_negative_impact(self):
        """bad_feature should have negative retention impact."""
        events = _make_feature_events(num_users=300)
        analyzer = FeatureCorrelationAnalyzer(events, min_feature_users=3)
        result = analyzer.analyze(retention_periods=[7])

        bad = next((f for f in result.features if f.feature_name == "bad_feature"), None)
        assert bad is not None
        assert bad.retention_impact[7] < 0, f"Expected negative D7 impact, got {bad.retention_impact[7]}"

    def test_negative_features_flagged(self):
        events = _make_feature_events(num_users=300)
        analyzer = FeatureCorrelationAnalyzer(events, min_feature_users=3)
        result = analyzer.analyze(retention_periods=[7, 30])

        assert len(result.negative_features) >= 1
        for f in result.negative_features:
            assert f.is_negative

    def test_ranking_order(self):
        """Ranked features should be sorted by net impact (best first)."""
        events = _make_feature_events(num_users=200)
        analyzer = FeatureCorrelationAnalyzer(events, min_feature_users=3)
        result = analyzer.analyze(retention_periods=[7, 30])

        ranked = result.ranked
        for i in range(len(ranked) - 1):
            assert ranked[i].net_correlation_score >= ranked[i + 1].net_correlation_score

    def test_retention_rates_bounded(self):
        events = _make_feature_events(num_users=200)
        analyzer = FeatureCorrelationAnalyzer(events, min_feature_users=3)
        result = analyzer.analyze(retention_periods=[7])

        for f in result.features:
            for period in result.periods:
                assert 0.0 <= f.retention_users[period] <= 1.0
                assert 0.0 <= f.retention_non_users[period] <= 1.0

    def test_normalized_impact_exists(self):
        events = _make_feature_events(num_users=200)
        analyzer = FeatureCorrelationAnalyzer(events, min_feature_users=3)
        result = analyzer.analyze(retention_periods=[7])

        for f in result.features:
            assert 7 in f.normalized_impact

    def test_exclude_events(self):
        events = _make_feature_events(num_users=100)
        analyzer = FeatureCorrelationAnalyzer(events, min_feature_users=3)
        result = analyzer.analyze(exclude_events=["signup", "pageview", "click"])

        feature_names = {f.feature_name for f in result.features}
        assert "signup" not in feature_names
        assert "pageview" not in feature_names

    def test_min_feature_users_filter(self):
        events = _make_feature_events(num_users=50)
        analyzer = FeatureCorrelationAnalyzer(events, min_feature_users=100)
        result = analyzer.analyze()

        # With only 50 users, no feature should have 100+ users
        assert result.total_features == 0

    def test_empty_events(self):
        events = pl.DataFrame({
            "user_id": [],
            "event_date": [],
            "event_name": [],
        }).cast({"user_id": pl.Utf8, "event_date": pl.Date, "event_name": pl.Utf8})
        analyzer = FeatureCorrelationAnalyzer(events)
        result = analyzer.analyze()

        assert result.total_users == 0
        assert result.total_features == 0

    def test_from_list_of_dicts(self):
        events = [
            {"user_id": "u1", "event_date": date(2026, 1, 1), "event_name": "signup"},
            {"user_id": "u1", "event_date": date(2026, 1, 2), "event_name": "feature_a"},
            {"user_id": "u1", "event_date": date(2026, 1, 8), "event_name": "pageview"},
            {"user_id": "u2", "event_date": date(2026, 1, 1), "event_name": "signup"},
            {"user_id": "u3", "event_date": date(2026, 1, 1), "event_name": "signup"},
            {"user_id": "u4", "event_date": date(2026, 1, 1), "event_name": "signup"},
            {"user_id": "u5", "event_date": date(2026, 1, 1), "event_name": "signup"},
            {"user_id": "u6", "event_date": date(2026, 1, 1), "event_name": "signup"},
        ]
        analyzer = FeatureCorrelationAnalyzer(events, min_feature_users=1, min_non_users=1)
        result = analyzer.analyze(retention_periods=[7])
        assert result.total_users >= 1

    def test_ai_quality_scores(self):
        events = _make_feature_events_with_ai(num_users=100)
        analyzer = FeatureCorrelationAnalyzer(events, min_feature_users=3)
        result = analyzer.analyze(retention_periods=[7])

        ai_feature = next((f for f in result.features if f.feature_name == "ai_search"), None)
        if ai_feature:
            assert ai_feature.avg_ai_quality is not None
            assert 0.0 < ai_feature.avg_ai_quality <= 1.0
            assert ai_feature.ai_event_pct > 0


class TestFeatureCorrelation:
    """Test FeatureCorrelation dataclass."""

    def test_is_negative(self):
        fi = FeatureCorrelation(
            feature_name="bad",
            total_users=100,
            total_events=200,
            retention_impact={7: -0.1},
            retention_users={7: 0.2},
            retention_non_users={7: 0.3},
            normalized_impact={7: -0.05},
        )
        assert fi.is_negative

    def test_is_not_negative(self):
        fi = FeatureCorrelation(
            feature_name="good",
            total_users=100,
            total_events=200,
            retention_impact={7: 0.1},
            retention_users={7: 0.4},
            retention_non_users={7: 0.3},
            normalized_impact={7: 0.08},
        )
        assert not fi.is_negative

    def test_net_correlation_score_weighted(self):
        fi = FeatureCorrelation(
            feature_name="test",
            total_users=100,
            total_events=200,
            retention_impact={7: 0.1, 30: 0.05},
            retention_users={7: 0.4, 30: 0.25},
            retention_non_users={7: 0.3, 30: 0.20},
            normalized_impact={7: 0.1, 30: 0.05},
        )
        score = fi.net_correlation_score
        # D30 weighted 2x, D7 weighted 1x: (0.1*1 + 0.05*2) / 3 = 0.0667
        assert abs(score - 0.0667) < 0.01

    def test_empty_normalized_impact(self):
        fi = FeatureCorrelation(
            feature_name="empty",
            total_users=0,
            total_events=0,
            retention_impact={},
            retention_users={},
            retention_non_users={},
            normalized_impact={},
        )
        assert fi.net_correlation_score == 0.0
        assert not fi.is_negative


class TestFeatureCorrelationResult:
    """Test FeatureCorrelationResult."""

    def test_to_summary_format(self):
        events = _make_feature_events(num_users=200)
        analyzer = FeatureCorrelationAnalyzer(events, min_feature_users=3)
        result = analyzer.analyze(retention_periods=[7, 30])
        summary = result.to_summary()

        assert summary["metric"] == "feature_correlation"
        assert "date_range" in summary
        assert "total_users" in summary
        assert "positive_count" in summary
        assert "negative_count" in summary
        assert isinstance(summary["top_features"], list)
        assert isinstance(summary["negative_features"], list)

    def test_top_features_limited_to_10(self):
        events = _make_feature_events(num_users=200)
        analyzer = FeatureCorrelationAnalyzer(events, min_feature_users=3)
        result = analyzer.analyze()
        summary = result.to_summary()

        assert len(summary["top_features"]) <= 10

    def test_positive_features_property(self):
        events = _make_feature_events(num_users=200)
        analyzer = FeatureCorrelationAnalyzer(events, min_feature_users=3)
        result = analyzer.analyze(retention_periods=[7])

        for f in result.positive_features:
            assert f.net_correlation_score > 0.01
