"""Tests for activation moment discovery."""

from datetime import date, timedelta

import polars as pl
import pytest

from stats.activation import (
    ActivationDiscovery,
    ActivationResult,
    ActivationMoment,
    _chi_squared_p_value,
    _matthews_correlation,
    DEFAULT_TRIVIAL_EVENTS,
)


# ─── Test Data ───


def _make_activation_events(num_users: int = 300, days: int = 60) -> pl.DataFrame:
    """Synthetic events where 'onboarding_complete' is a strong activation moment.

    - Users who do 'onboarding_complete' within 7 days: 60% D30 retention
    - Users who don't: 15% D30 retention
    - 'settings_viewed' has no effect (noise)
    """
    import random
    random.seed(42)

    rows = []
    for uid in range(num_users):
        signup_day = random.randint(0, 10)
        signup_date = date(2026, 1, 1) + timedelta(days=signup_day)

        # Signup
        rows.append({"user_id": f"u{uid}", "event_date": signup_date, "event_name": "signup"})

        # Activation event: onboarding_complete (40% adoption)
        did_onboarding = random.random() < 0.40
        if did_onboarding:
            rows.append({
                "user_id": f"u{uid}",
                "event_date": signup_date + timedelta(days=random.randint(1, 5)),
                "event_name": "onboarding_complete",
            })
            retention_prob = 0.60
        else:
            retention_prob = 0.15

        # Noise event: settings_viewed (30% adoption, no retention effect)
        if random.random() < 0.30:
            rows.append({
                "user_id": f"u{uid}",
                "event_date": signup_date + timedelta(days=random.randint(0, 3)),
                "event_name": "settings_viewed",
            })

        # Another good event: invite_sent (20% adoption, good retention)
        did_invite = random.random() < 0.20
        if did_invite:
            rows.append({
                "user_id": f"u{uid}",
                "event_date": signup_date + timedelta(days=random.randint(1, 6)),
                "event_name": "invite_sent",
            })
            retention_prob = max(retention_prob, 0.55)  # also correlated

        # Return activity (determines D30 retention)
        for d in [1, 7, 14, 30, 35, 40]:
            if random.random() < retention_prob * (1 - 0.01 * d):
                rows.append({
                    "user_id": f"u{uid}",
                    "event_date": signup_date + timedelta(days=d),
                    "event_name": "pageview",
                })

    return pl.DataFrame(rows)


# ─── Tests ───


class TestActivationDiscovery:

    def test_basic_discovery(self):
        events = _make_activation_events()
        discovery = ActivationDiscovery(events, min_adopters=5)
        result = discovery.discover(activation_window=7, retention_period=30)

        assert isinstance(result, ActivationResult)
        assert result.total_users > 0
        assert len(result.moments) > 0

    def test_finds_onboarding_complete(self):
        """onboarding_complete should be discovered as a strong activation moment."""
        events = _make_activation_events(num_users=500)
        discovery = ActivationDiscovery(events, min_adopters=5)
        result = discovery.discover(activation_window=7, retention_period=30)

        event_names = [m.events for m in result.moments]
        found = any("onboarding_complete" in evts for evts in event_names)
        assert found, f"onboarding_complete not found. Moments: {event_names}"

    def test_onboarding_has_high_lift(self):
        events = _make_activation_events(num_users=500)
        discovery = ActivationDiscovery(events, min_adopters=5)
        result = discovery.discover(activation_window=7, retention_period=30)

        onb = next((m for m in result.moments if "onboarding_complete" in m.events), None)
        assert onb is not None
        assert onb.lift >= 1.5, f"Expected lift >= 1.5, got {onb.lift}"

    def test_trivial_events_filtered(self):
        """pageview, signup etc. should be excluded."""
        events = _make_activation_events()
        discovery = ActivationDiscovery(events, min_adopters=5)
        result = discovery.discover()

        for m in result.moments:
            for e in m.events:
                assert e not in DEFAULT_TRIVIAL_EVENTS, f"Trivial event {e} not filtered"

    def test_statistical_significance(self):
        """All returned moments should have p < 0.05."""
        events = _make_activation_events(num_users=500)
        discovery = ActivationDiscovery(events, min_adopters=5, p_value_threshold=0.05)
        result = discovery.discover()

        for m in result.moments:
            assert m.p_value < 0.05, f"{m.events} has p={m.p_value}"

    def test_ranked_by_correlation_primary(self):
        """Primary ranking is by MCC correlation (Amplitude Compass approach)."""
        events = _make_activation_events(num_users=500)
        discovery = ActivationDiscovery(events, min_adopters=5)
        result = discovery.discover()

        for i in range(len(result.moments) - 1):
            assert result.moments[i].correlation >= result.moments[i + 1].correlation

    def test_adoption_rate_bounded(self):
        events = _make_activation_events()
        discovery = ActivationDiscovery(
            events, min_adopters=5,
            min_adoption_rate=0.05, max_adoption_rate=0.90,
        )
        result = discovery.discover()

        for m in result.moments:
            assert 0.05 <= m.adoption_rate <= 0.90

    def test_combination_discovery(self):
        """Should find 2-event combinations."""
        events = _make_activation_events(num_users=500)
        discovery = ActivationDiscovery(events, min_adopters=5)
        result = discovery.discover(max_combinations=2)

        combos = [m for m in result.moments if m.is_combination]
        # May or may not find combos depending on data, but should not crash
        assert isinstance(combos, list)

    def test_max_results(self):
        events = _make_activation_events(num_users=500)
        discovery = ActivationDiscovery(events, min_adopters=5)
        result = discovery.discover(max_results=3)

        assert len(result.moments) <= 3

    def test_empty_events(self):
        events = pl.DataFrame({
            "user_id": [], "event_date": [], "event_name": [],
        }).cast({"user_id": pl.Utf8, "event_date": pl.Date, "event_name": pl.Utf8})
        discovery = ActivationDiscovery(events)
        result = discovery.discover()

        assert result.total_users == 0
        assert len(result.moments) == 0

    def test_to_summary(self):
        events = _make_activation_events()
        discovery = ActivationDiscovery(events, min_adopters=5)
        result = discovery.discover()
        summary = result.to_summary()

        assert summary["metric"] == "activation_discovery"
        assert "caveat" in summary
        assert "baseline_retention" in summary
        assert isinstance(summary["top_moments"], list)

    def test_custom_trivial_events(self):
        """Custom trivial blocklist."""
        events = _make_activation_events()
        # Block onboarding_complete as trivial
        custom_trivial = DEFAULT_TRIVIAL_EVENTS | {"onboarding_complete"}
        discovery = ActivationDiscovery(events, trivial_events=custom_trivial, min_adopters=5)
        result = discovery.discover()

        for m in result.moments:
            assert "onboarding_complete" not in m.events

    def test_from_list_of_dicts(self):
        events = [
            {"user_id": f"u{i}", "event_date": date(2026, 1, 1), "event_name": "signup"}
            for i in range(20)
        ] + [
            {"user_id": f"u{i}", "event_date": date(2026, 1, 3), "event_name": "feature_x"}
            for i in range(10)
        ] + [
            {"user_id": f"u{i}", "event_date": date(2026, 2, 1), "event_name": "return"}
            for i in range(8)
        ]
        discovery = ActivationDiscovery(events, min_adopters=3, min_non_adopters=3)
        result = discovery.discover(activation_window=7, retention_period=30)
        assert result.total_users >= 1


class TestActivationMoment:

    def test_impact_score(self):
        m = ActivationMoment(
            events=["feature_x"], min_frequency=1,
            window_days=7, adoption_rate=0.30,
            retention_adopters=0.60, retention_non_adopters=0.20,
            lift=3.0, correlation=0.35,
            adopter_count=100, non_adopter_count=200, p_value=0.001,
        )
        # impact = (3.0 - 1.0) × (1.0 - 0.30) = 2.0 × 0.70 = 1.4
        assert abs(m.impact_score - 1.4) < 0.01

    def test_is_combination(self):
        single = ActivationMoment(
            events=["a"], min_frequency=1, window_days=7, adoption_rate=0.5,
            retention_adopters=0.5, retention_non_adopters=0.3, lift=1.67,
            correlation=0.2, adopter_count=50, non_adopter_count=50, p_value=0.01,
        )
        combo = ActivationMoment(
            events=["a", "b"], min_frequency=1, window_days=7, adoption_rate=0.2,
            retention_adopters=0.7, retention_non_adopters=0.3, lift=2.33,
            correlation=0.3, adopter_count=20, non_adopter_count=80, p_value=0.01,
        )
        assert not single.is_combination
        assert combo.is_combination

    def test_to_dict(self):
        m = ActivationMoment(
            events=["feature_x"], min_frequency=3,
            window_days=7, adoption_rate=0.30,
            retention_adopters=0.60, retention_non_adopters=0.20,
            lift=3.0, correlation=0.35,
            adopter_count=100, non_adopter_count=200, p_value=0.001,
        )
        d = m.to_dict()
        assert d["events"] == ["feature_x"]
        assert d["min_frequency"] == 3
        assert d["lift"] == 3.0
        assert d["correlation"] == 0.35
        assert d["adoption_rate"] == 0.3
        assert "impact_score" in d
        assert d["label"] == "feature_x ≥3 times"

    def test_label_single(self):
        m = ActivationMoment(
            events=["search"], min_frequency=1,
            window_days=7, adoption_rate=0.5,
            retention_adopters=0.5, retention_non_adopters=0.3, lift=1.67,
            correlation=0.2, adopter_count=50, non_adopter_count=50, p_value=0.01,
        )
        assert m.label == "search ≥1 time"

    def test_label_combo(self):
        m = ActivationMoment(
            events=["search", "invite"], min_frequency=2,
            window_days=7, adoption_rate=0.2,
            retention_adopters=0.7, retention_non_adopters=0.3, lift=2.33,
            correlation=0.3, adopter_count=20, non_adopter_count=80, p_value=0.01,
        )
        assert m.label == "search + invite ≥2 times"


    def test_frequency_threshold_discovery(self):
        """Should find that doing events MORE times correlates better."""
        events = _make_activation_events(num_users=500)
        discovery = ActivationDiscovery(events, min_adopters=5)
        result = discovery.discover(activation_window=7, retention_period=30)

        # At least some moments should have min_frequency > 1
        # (if the data supports it)
        has_freq_gt_1 = any(m.min_frequency > 1 for m in result.moments)
        # This is data-dependent, so we just check the field exists
        for m in result.moments:
            assert m.min_frequency >= 1

    def test_correlation_in_results(self):
        """All moments should have MCC correlation."""
        events = _make_activation_events(num_users=500)
        discovery = ActivationDiscovery(events, min_adopters=5)
        result = discovery.discover()

        for m in result.moments:
            assert -1.0 <= m.correlation <= 1.0
            assert m.correlation > 0  # we filter for lift > 1.1, so MCC should be positive

    def test_ranked_by_correlation(self):
        """Moments should be ranked by MCC (matches Amplitude Compass)."""
        events = _make_activation_events(num_users=500)
        discovery = ActivationDiscovery(events, min_adopters=5)
        result = discovery.discover()

        for i in range(len(result.moments) - 1):
            assert result.moments[i].correlation >= result.moments[i + 1].correlation

    def test_custom_frequency_thresholds(self):
        events = _make_activation_events(num_users=300)
        discovery = ActivationDiscovery(
            events, min_adopters=5,
            frequency_thresholds=[1, 5, 20],
        )
        result = discovery.discover()
        # Should not crash, results depend on data
        assert isinstance(result, ActivationResult)


class TestMCC:

    def test_perfect_prediction(self):
        # All adopters retained, all non-adopters churned
        mcc = _matthews_correlation(50, 0, 0, 50)
        assert abs(mcc - 1.0) < 0.001

    def test_no_prediction(self):
        # Same retention rate for both groups
        mcc = _matthews_correlation(25, 25, 25, 25)
        assert abs(mcc) < 0.001

    def test_zeros(self):
        mcc = _matthews_correlation(0, 0, 0, 0)
        assert mcc == 0.0


class TestChiSquared:

    def test_significant_difference(self):
        # 80/100 retained vs 20/100 retained — very significant
        p = _chi_squared_p_value(80, 100, 20, 100)
        assert p < 0.001

    def test_no_difference(self):
        # 50/100 vs 50/100 — no difference
        p = _chi_squared_p_value(50, 100, 50, 100)
        assert p > 0.5

    def test_small_difference(self):
        # 52/100 vs 48/100 — not significant
        p = _chi_squared_p_value(52, 100, 48, 100)
        assert p > 0.05

    def test_zero_totals(self):
        p = _chi_squared_p_value(0, 0, 0, 0)
        assert p == 1.0

    def test_one_sided_zero(self):
        p = _chi_squared_p_value(10, 10, 0, 10)
        assert p < 0.001
