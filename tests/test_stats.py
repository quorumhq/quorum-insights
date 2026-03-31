"""Tests for the stats layer: retention curves and anomaly detection."""

from datetime import date, timedelta

import polars as pl
import pytest

from stats.retention import RetentionComputer, RetentionResult, CohortRetention
from stats.anomaly import (
    AnomalyDetector,
    AnomalyResult,
    Anomaly,
    MetricSeries,
    Severity,
    Direction,
)


# ─── Test Data Helpers ───


def _make_events(
    num_users: int = 100,
    start: date = date(2026, 1, 1),
    days: int = 90,
    daily_active_pct: float = 0.3,
    decay: float = 0.02,
) -> pl.DataFrame:
    """Generate synthetic event data with realistic retention decay.

    Each user has a signup date (uniformly distributed in first 30 days).
    After signup, they return with probability (daily_active_pct - decay * days_since).
    """
    import random
    random.seed(42)

    rows = []
    for uid in range(num_users):
        signup_offset = random.randint(0, min(30, days - 1))
        signup_date = start + timedelta(days=signup_offset)

        # Always active on signup day
        rows.append({
            "user_id": f"u{uid}",
            "event_date": signup_date,
            "event_name": "signup",
            "plan": random.choice(["free", "pro", "enterprise"]),
        })

        # Subsequent days with decaying probability
        for d in range(1, days - signup_offset):
            prob = max(0, daily_active_pct - decay * d)
            if random.random() < prob:
                rows.append({
                    "user_id": f"u{uid}",
                    "event_date": signup_date + timedelta(days=d),
                    "event_name": random.choice(["pageview", "click", "search"]),
                    "plan": random.choice(["free", "pro", "enterprise"]),
                })

    return pl.DataFrame(rows)


def _make_stable_series(
    name: str = "dau",
    start: date = date(2026, 1, 1),
    weeks: int = 12,
    base_value: float = 1000.0,
    noise: float = 50.0,
) -> MetricSeries:
    """Weekly metric series with small gaussian noise."""
    import random
    random.seed(42)
    dates = [start + timedelta(weeks=w) for w in range(weeks)]
    values = [base_value + random.gauss(0, noise) for _ in range(weeks)]
    return MetricSeries(name=name, dates=dates, values=values)


def _make_anomalous_series(
    name: str = "dau",
    start: date = date(2026, 1, 1),
    weeks: int = 12,
    base_value: float = 1000.0,
    noise: float = 50.0,
    spike_week: int = 8,
    spike_magnitude: float = 500.0,
) -> MetricSeries:
    """Weekly metric series with a clear anomaly at spike_week."""
    import random
    random.seed(42)
    dates = [start + timedelta(weeks=w) for w in range(weeks)]
    values = [base_value + random.gauss(0, noise) for _ in range(weeks)]
    values[spike_week] += spike_magnitude  # inject anomaly
    return MetricSeries(name=name, dates=dates, values=values)


# ─── Retention Tests ───


class TestRetentionComputer:
    """Test retention curve computation."""

    def test_basic_retention(self):
        events = _make_events(num_users=50, days=60)
        computer = RetentionComputer(events)
        result = computer.compute(periods=[1, 7, 30])

        assert isinstance(result, RetentionResult)
        assert result.total_users == 50
        assert result.periods == [1, 7, 30]
        assert len(result.cohorts) > 0

    def test_retention_rates_decrease_over_time(self):
        """D1 > D7 > D30 in realistic data."""
        events = _make_events(num_users=100, days=90)
        computer = RetentionComputer(events)
        result = computer.compute(periods=[1, 7, 30])

        # Overall retention should decrease
        assert result.overall_retention[1] >= result.overall_retention[7]
        assert result.overall_retention[7] >= result.overall_retention[30]

    def test_d1_retention_bounded(self):
        """D1 retention should be between 0 and 1."""
        events = _make_events(num_users=50, days=30)
        computer = RetentionComputer(events)
        result = computer.compute(periods=[1])

        for cohort in result.cohorts:
            assert 0.0 <= cohort.retention_rates[1] <= 1.0

    def test_retention_by_cohort_dimension(self):
        """Compute retention grouped by 'plan' column."""
        events = _make_events(num_users=100, days=60)
        computer = RetentionComputer(events)
        result = computer.compute(periods=[1, 7], cohort_column="plan")

        # Should have cohorts for free, pro, enterprise
        cohort_keys = {c.cohort_key for c in result.cohorts}
        assert len(cohort_keys) > 1
        for c in result.cohorts:
            assert c.cohort_dimension == "plan"

    def test_default_cohort_is_week(self):
        events = _make_events(num_users=30, days=30)
        computer = RetentionComputer(events)
        result = computer.compute(periods=[1])

        for c in result.cohorts:
            assert c.cohort_dimension == "week"

    def test_retention_counts_match_rates(self):
        events = _make_events(num_users=50, days=30)
        computer = RetentionComputer(events)
        result = computer.compute(periods=[1, 7])

        for c in result.cohorts:
            for p in result.periods:
                expected_rate = c.retention_counts[p] / c.cohort_size
                assert abs(c.retention_rates[p] - expected_rate) < 0.001

    def test_empty_events(self):
        events = pl.DataFrame({
            "user_id": [],
            "event_date": [],
        }).cast({"user_id": pl.Utf8, "event_date": pl.Date})
        computer = RetentionComputer(events)
        result = computer.compute()

        assert result.total_users == 0
        assert len(result.cohorts) == 0

    def test_single_user(self):
        events = pl.DataFrame({
            "user_id": ["u1", "u1", "u1"],
            "event_date": [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 8)],
        })
        computer = RetentionComputer(events)
        result = computer.compute(periods=[1, 7, 30])

        assert result.total_users == 1
        assert result.overall_retention[1] == 1.0  # returned on D1
        assert result.overall_retention[7] == 1.0  # returned on D7
        assert result.overall_retention[30] == 0.0  # did not return on D30

    def test_to_summary_format(self):
        events = _make_events(num_users=50, days=60)
        computer = RetentionComputer(events)
        result = computer.compute(periods=[1, 7, 30])
        summary = result.to_summary()

        assert summary["metric"] == "retention"
        assert "date_range" in summary
        assert "overall_retention" in summary
        assert "D1" in summary["overall_retention"]
        assert "D7" in summary["overall_retention"]
        assert "D30" in summary["overall_retention"]
        assert isinstance(summary["cohorts"], list)
        assert len(summary["cohorts"]) > 0

    def test_best_worst_cohort(self):
        events = _make_events(num_users=100, days=60)
        computer = RetentionComputer(events)
        result = computer.compute(periods=[1, 7])
        summary = result.to_summary()

        # May be None if all cohorts < 10 users, but should exist for 100 users
        if summary["best_cohort"]:
            assert "key" in summary["best_cohort"]
        if summary["worst_cohort"]:
            assert "key" in summary["worst_cohort"]

    def test_string_dates_converted(self):
        """String dates in input are auto-converted."""
        events = pl.DataFrame({
            "user_id": ["u1", "u1"],
            "event_date": ["2026-01-01", "2026-01-08"],
        })
        computer = RetentionComputer(events)
        result = computer.compute(periods=[7])
        assert result.total_users == 1

    def test_from_list_of_dicts(self):
        events = [
            {"user_id": "u1", "event_date": date(2026, 1, 1)},
            {"user_id": "u1", "event_date": date(2026, 1, 2)},
            {"user_id": "u2", "event_date": date(2026, 1, 1)},
        ]
        computer = RetentionComputer(events)
        result = computer.compute(periods=[1])
        assert result.total_users == 2


# ─── Anomaly Detection Tests ───


class TestMetricSeries:
    """Test MetricSeries data class."""

    def test_create(self):
        s = MetricSeries("dau", [date(2026, 1, 1)], [100.0])
        assert s.name == "dau"
        assert len(s.dates) == 1

    def test_mismatched_lengths(self):
        with pytest.raises(ValueError, match="same length"):
            MetricSeries("dau", [date(2026, 1, 1)], [100.0, 200.0])

    def test_to_polars(self):
        s = MetricSeries("dau", [date(2026, 1, 1), date(2026, 1, 8)], [100.0, 110.0])
        df = s.to_polars()
        assert df.shape == (2, 2)
        assert "date" in df.columns
        assert "value" in df.columns

    def test_from_polars(self):
        df = pl.DataFrame({
            "dt": [date(2026, 1, 1), date(2026, 1, 8)],
            "val": [100.0, 110.0],
        })
        s = MetricSeries.from_polars("dau", df, date_col="dt", value_col="val")
        assert s.name == "dau"
        assert len(s.dates) == 2


class TestAnomalyDetector:
    """Test anomaly detection."""

    def test_no_anomalies_in_stable_data(self):
        # noise=20 on base=1000 is 2% CV — genuinely stable
        series = _make_stable_series(noise=20.0, weeks=16, base_value=1000.0)
        detector = AnomalyDetector(sigma_threshold=2.5, window=6)
        result = detector.detect(series)

        # Stable data should have very few anomalies
        assert result.anomaly_count <= 2  # statistical noise may produce 1-2
        assert result.metrics_checked == ["dau"]

    def test_detects_spike(self):
        series = _make_anomalous_series(
            noise=30.0, weeks=12, spike_week=8, spike_magnitude=500.0,
        )
        detector = AnomalyDetector(sigma_threshold=2.0, window=4)
        result = detector.detect(series)

        assert len(result.anomalies) >= 1
        # The spike should be detected
        spike_detected = any(
            a.date == series.dates[8] and a.direction == Direction.UP
            for a in result.anomalies
        )
        assert spike_detected, f"Spike not detected. Anomalies: {[a.to_dict() for a in result.anomalies]}"

    def test_detects_drop(self):
        series = _make_anomalous_series(
            noise=30.0, weeks=12, spike_week=8, spike_magnitude=-500.0,
        )
        detector = AnomalyDetector(sigma_threshold=2.0, window=4)
        result = detector.detect(series)

        drop_detected = any(
            a.date == series.dates[8] and a.direction == Direction.DOWN
            for a in result.anomalies
        )
        assert drop_detected

    def test_severity_classification(self):
        """Large spike (>3σ) should be CRITICAL."""
        series = _make_anomalous_series(
            noise=20.0, weeks=12, spike_week=8, spike_magnitude=1000.0,
        )
        detector = AnomalyDetector(sigma_threshold=2.0, window=4)
        result = detector.detect(series)

        critical = [a for a in result.anomalies if a.severity == Severity.CRITICAL]
        assert len(critical) >= 1

    def test_higher_threshold_fewer_anomalies(self):
        series = _make_anomalous_series(noise=50.0, spike_magnitude=200.0)
        result_low = AnomalyDetector(sigma_threshold=1.5).detect(series)
        result_high = AnomalyDetector(sigma_threshold=3.0).detect(series)

        assert len(result_high.anomalies) <= len(result_low.anomalies)

    def test_too_few_data_points(self):
        series = MetricSeries("dau", [date(2026, 1, 1)], [100.0])
        detector = AnomalyDetector(min_data_points=4)
        result = detector.detect(series)

        assert len(result.anomalies) == 0
        assert result.total_data_points == 1

    def test_detect_multiple_series(self):
        series1 = _make_stable_series(name="dau", noise=30.0)
        series2 = _make_anomalous_series(name="sessions", spike_magnitude=500.0)

        detector = AnomalyDetector(sigma_threshold=2.0, window=4)
        result = detector.detect_multiple([series1, series2])

        assert "dau" in result.metrics_checked
        assert "sessions" in result.metrics_checked
        # The anomalous series should contribute anomalies
        session_anomalies = [a for a in result.anomalies if a.metric_name == "sessions"]
        assert len(session_anomalies) >= 1

    def test_anomalies_sorted_by_severity(self):
        """Critical anomalies should come first in results."""
        series = _make_anomalous_series(noise=20.0, spike_magnitude=1000.0)
        detector = AnomalyDetector(sigma_threshold=2.0)
        result = detector.detect(series)

        if len(result.anomalies) > 1:
            severities = [a.severity for a in result.anomalies]
            critical_indices = [i for i, s in enumerate(severities) if s == Severity.CRITICAL]
            warning_indices = [i for i, s in enumerate(severities) if s == Severity.WARNING]
            if critical_indices and warning_indices:
                assert max(critical_indices) < min(warning_indices)

    def test_to_summary_format(self):
        series = _make_anomalous_series(spike_magnitude=500.0)
        detector = AnomalyDetector(sigma_threshold=2.0, window=4)
        result = detector.detect(series)
        summary = result.to_summary()

        assert summary["metric"] == "anomaly_detection"
        assert "date_range" in summary
        assert "anomaly_count" in summary
        assert "critical_count" in summary
        assert "warning_count" in summary
        assert isinstance(summary["anomalies"], list)

    def test_pct_change_in_anomaly(self):
        series = _make_anomalous_series(
            base_value=1000.0, noise=20.0, spike_magnitude=500.0,
        )
        detector = AnomalyDetector(sigma_threshold=2.0, window=4)
        result = detector.detect(series)

        for a in result.anomalies:
            assert isinstance(a.pct_change, float)

    def test_has_critical_property(self):
        series = _make_anomalous_series(noise=20.0, spike_magnitude=1000.0)
        detector = AnomalyDetector(sigma_threshold=2.0)
        result = detector.detect(series)
        assert isinstance(result.has_critical, bool)

    def test_validation_sigma(self):
        with pytest.raises(ValueError, match="positive"):
            AnomalyDetector(sigma_threshold=0)

    def test_validation_window(self):
        with pytest.raises(ValueError, match="window"):
            AnomalyDetector(window=1)

    def test_validation_min_points(self):
        with pytest.raises(ValueError, match="min_data_points"):
            AnomalyDetector(min_data_points=1)

    def test_constant_series_no_anomalies(self):
        """A perfectly constant series should have no anomalies."""
        dates = [date(2026, 1, 1) + timedelta(weeks=w) for w in range(12)]
        series = MetricSeries("flat", dates, [100.0] * 12)
        detector = AnomalyDetector(sigma_threshold=2.0, window=4)
        result = detector.detect(series)
        assert len(result.anomalies) == 0

    def test_detect_multiple_empty(self):
        """Empty list of series."""
        detector = AnomalyDetector()
        result = detector.detect_multiple([])
        assert len(result.anomalies) == 0
        assert result.total_data_points == 0
