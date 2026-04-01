"""Tests for the stats summary aggregator."""

from datetime import date, timedelta

import polars as pl
import pytest

from stats.aggregator import (
    StatsAggregator,
    StatsSummary,
    Finding,
    FindingSeverity,
    FindingCategory,
    DataFreshness,
    SCHEMA_VERSION,
)
from stats.retention import RetentionComputer
from stats.anomaly import AnomalyDetector, MetricSeries
from stats.features import FeatureCorrelationAnalyzer


# ─── Test Data Helpers ───


def _retention_summary() -> dict:
    """Realistic retention summary from RetentionComputer."""
    return {
        "metric": "retention",
        "date_range": {"start": "2026-01-01", "end": "2026-03-31"},
        "total_users": 500,
        "periods": [1, 7, 30],
        "overall_retention": {"D1": 0.45, "D7": 0.28, "D30": 0.12},
        "cohorts": [
            {"key": "2026-W01", "dimension": "week", "size": 80,
             "retention": {"D1": 0.50, "D7": 0.32, "D30": 0.15}},
            {"key": "2026-W02", "dimension": "week", "size": 75,
             "retention": {"D1": 0.42, "D7": 0.25, "D30": 0.10}},
        ],
        "best_cohort": {"key": "2026-W01", "dimension": "week", "D30": 0.15},
        "worst_cohort": {"key": "2026-W02", "dimension": "week", "D30": 0.10},
    }


def _anomaly_summary() -> dict:
    """Realistic anomaly summary with a critical spike."""
    return {
        "metric": "anomaly_detection",
        "date_range": {"start": "2026-01-01", "end": "2026-03-31"},
        "metrics_checked": ["dau", "sessions"],
        "total_data_points": 24,
        "anomaly_count": 2,
        "critical_count": 1,
        "warning_count": 1,
        "anomalies": [
            {
                "metric": "dau",
                "date": "2026-02-15",
                "value": 1500.0,
                "expected": 1000.0,
                "sigma_distance": 4.2,
                "direction": "up",
                "severity": "critical",
                "pct_change": 0.50,
            },
            {
                "metric": "sessions",
                "date": "2026-03-01",
                "value": 800.0,
                "expected": 950.0,
                "sigma_distance": 2.3,
                "direction": "down",
                "severity": "warning",
                "pct_change": -0.158,
            },
        ],
    }


def _feature_summary() -> dict:
    """Realistic feature impact summary."""
    return {
        "metric": "feature_correlation",
        "date_range": {"start": "2026-01-01", "end": "2026-03-31"},
        "total_users": 500,
        "total_features": 5,
        "periods": ["D7", "D30"],
        "positive_count": 2,
        "negative_count": 1,
        "top_features": [
            {"name": "search", "users": 200, "events": 1500,
             "impact": {"D7": 0.08, "D30": 0.05}, "net_score": 0.06},
            {"name": "dashboard", "users": 150, "events": 800,
             "impact": {"D7": 0.05, "D30": 0.03}, "net_score": 0.037},
        ],
        "negative_features": [
            {"name": "legacy_export", "users": 50,
             "impact": {"D7": -0.12, "D30": -0.08}, "net_score": -0.093},
        ],
    }


# ─── Tests ───


class TestStatsAggregator:
    """Test the aggregator."""

    def test_full_aggregation(self):
        agg = StatsAggregator()
        agg.add_retention(_retention_summary())
        agg.add_anomalies(_anomaly_summary())
        agg.add_feature_correlation(_feature_summary())
        summary = agg.build()

        assert isinstance(summary, StatsSummary)
        assert summary.finding_count > 0
        assert summary.schema_version == SCHEMA_VERSION

    def test_findings_ranked_by_score(self):
        agg = StatsAggregator()
        agg.add_retention(_retention_summary())
        agg.add_anomalies(_anomaly_summary())
        agg.add_feature_correlation(_feature_summary())
        summary = agg.build()

        scores = [f.rank_score for f in summary.findings]
        for i in range(len(scores) - 1):
            assert scores[i] >= scores[i + 1], \
                f"Finding {i} (score {scores[i]}) should rank >= finding {i+1} (score {scores[i+1]})"

    def test_critical_anomalies_included(self):
        agg = StatsAggregator()
        agg.add_anomalies(_anomaly_summary())
        summary = agg.build()

        anomaly_findings = [f for f in summary.findings if f.category == FindingCategory.ANOMALY]
        critical = [f for f in anomaly_findings if f.severity == FindingSeverity.CRITICAL]
        assert len(critical) >= 1

    def test_negative_features_flagged(self):
        agg = StatsAggregator()
        agg.add_feature_correlation(_feature_summary())
        summary = agg.build()

        negative = [f for f in summary.findings
                    if f.category == FindingCategory.FEATURE_CORRELATION
                    and f.severity == FindingSeverity.HIGH
                    and "Negative" in f.title]
        assert len(negative) >= 1

    def test_retention_only(self):
        """Works with just retention data."""
        agg = StatsAggregator()
        agg.add_retention(_retention_summary())
        summary = agg.build()

        assert summary.finding_count >= 1
        assert "retention" in summary.freshness.modules_available
        assert "anomaly" in summary.freshness.modules_missing
        assert "feature_correlation" in summary.freshness.modules_missing

    def test_anomaly_only(self):
        agg = StatsAggregator()
        agg.add_anomalies(_anomaly_summary())
        summary = agg.build()

        assert summary.finding_count >= 1
        assert "anomaly" in summary.freshness.modules_available

    def test_empty_aggregator(self):
        agg = StatsAggregator()
        summary = agg.build()

        assert summary.finding_count == 0
        assert len(summary.freshness.modules_missing) == 3

    def test_noise_filtering_anomaly_threshold(self):
        """Low-sigma anomalies filtered out."""
        summary_data = _anomaly_summary()
        # Make the warning anomaly below threshold
        summary_data["anomalies"][1]["sigma_distance"] = 1.5

        agg = StatsAggregator(min_anomaly_sigma=2.0)
        agg.add_anomalies(summary_data)
        summary = agg.build()

        anomaly_findings = [f for f in summary.findings if f.category == FindingCategory.ANOMALY]
        assert len(anomaly_findings) == 1  # only the critical one

    def test_noise_filtering_feature_threshold(self):
        """Low-impact features filtered out."""
        summary_data = _feature_summary()
        # Make features have tiny impact
        for f in summary_data["top_features"]:
            f["net_score"] = 0.005  # below threshold
            f["impact"] = {"D7": 0.005, "D30": 0.003}
        summary_data["negative_features"][0]["net_score"] = -0.005
        summary_data["negative_features"][0]["impact"] = {"D7": -0.005}

        agg = StatsAggregator(min_impact_threshold=0.02)
        agg.add_feature_correlation(summary_data)
        summary = agg.build()

        feature_findings = [f for f in summary.findings
                           if f.category == FindingCategory.FEATURE_CORRELATION]
        assert len(feature_findings) == 0

    def test_date_range_merged(self):
        agg = StatsAggregator()
        agg.add_retention({"date_range": {"start": "2026-02-01", "end": "2026-03-15"},
                          "total_users": 100, "overall_retention": {}})
        agg.add_anomalies({"date_range": {"start": "2026-01-01", "end": "2026-03-31"},
                          "total_data_points": 50, "anomalies": []})
        summary = agg.build()

        assert summary.freshness.date_start == date(2026, 1, 1)
        assert summary.freshness.date_end == date(2026, 3, 31)

    def test_user_count_max(self):
        agg = StatsAggregator()
        agg.add_retention({"date_range": {}, "total_users": 500, "overall_retention": {}})
        agg.add_feature_correlation({"date_range": {}, "total_users": 300,
                               "top_features": [], "negative_features": []})
        summary = agg.build()

        assert summary.freshness.user_count == 500  # max of both

    def test_fluent_api(self):
        """Builder pattern returns self for chaining."""
        summary = (
            StatsAggregator()
            .add_retention(_retention_summary())
            .add_anomalies(_anomaly_summary())
            .add_feature_correlation(_feature_summary())
            .build()
        )
        assert summary.finding_count > 0


class TestStatsSummary:
    """Test StatsSummary output formats."""

    def _build_summary(self) -> StatsSummary:
        return (
            StatsAggregator()
            .add_retention(_retention_summary())
            .add_anomalies(_anomaly_summary())
            .add_feature_correlation(_feature_summary())
            .build()
        )

    def test_to_dict_structure(self):
        summary = self._build_summary()
        d = summary.to_dict()

        assert d["schema_version"] == SCHEMA_VERSION
        assert "generated_at" in d
        assert "freshness" in d
        assert "summary" in d
        assert "findings" in d
        assert isinstance(d["findings"], list)
        assert d["summary"]["finding_count"] == summary.finding_count

    def test_to_dict_freshness(self):
        summary = self._build_summary()
        f = summary.to_dict()["freshness"]

        assert f["user_count"] > 0
        assert "date_range" in f
        assert isinstance(f["modules_available"], list)
        assert isinstance(f["modules_missing"], list)

    def test_to_llm_prompt_context(self):
        summary = self._build_summary()
        text = summary.to_llm_prompt_context(max_findings=5)

        assert "Product Analytics Summary" in text
        assert "users" in text
        assert "CRITICAL" in text or "HIGH" in text
        assert len(text) > 100

    def test_to_llm_prompt_truncation(self):
        summary = self._build_summary()
        short = summary.to_llm_prompt_context(max_findings=1)
        full = summary.to_llm_prompt_context(max_findings=100)

        assert len(short) < len(full) or summary.finding_count <= 1

    def test_critical_count(self):
        summary = self._build_summary()
        assert summary.critical_count >= 0
        assert summary.critical_count <= summary.finding_count


class TestFinding:
    """Test Finding dataclass."""

    def test_rank_score(self):
        f = Finding(
            category=FindingCategory.ANOMALY,
            severity=FindingSeverity.CRITICAL,
            title="test",
            description="test",
            data={},
            confidence=1.0,
            impact_score=1.0,
        )
        # CRITICAL weight = 1.0 × 1.0 × 1.0 = 1.0
        assert f.rank_score == 1.0

    def test_rank_score_low_confidence(self):
        f = Finding(
            category=FindingCategory.RETENTION,
            severity=FindingSeverity.HIGH,
            title="test",
            description="test",
            data={},
            confidence=0.5,
            impact_score=0.8,
        )
        # HIGH weight = 0.8 × 0.5 × 0.8 = 0.32
        assert abs(f.rank_score - 0.32) < 0.001

    def test_rank_score_minimum_impact(self):
        """Even with 0 impact_score, use 0.1 floor."""
        f = Finding(
            category=FindingCategory.RETENTION,
            severity=FindingSeverity.CRITICAL,
            title="test",
            description="test",
            data={},
            confidence=1.0,
            impact_score=0.0,
        )
        assert f.rank_score == 0.1  # 1.0 × 1.0 × 0.1

    def test_to_dict(self):
        f = Finding(
            category=FindingCategory.ANOMALY,
            severity=FindingSeverity.CRITICAL,
            title="DAU spike",
            description="DAU increased 50%",
            data={"value": 1500},
            confidence=0.9,
            impact_score=0.8,
        )
        d = f.to_dict()

        assert d["category"] == "anomaly"
        assert d["severity"] == "critical"
        assert d["title"] == "DAU spike"
        assert "rank_score" in d
        assert d["data"]["value"] == 1500


class TestIntegration:
    """End-to-end integration: real stats modules → aggregator → summary."""

    def _make_events(self) -> pl.DataFrame:
        import random
        random.seed(42)
        rows = []
        for uid in range(200):
            signup = date(2026, 1, 1) + timedelta(days=random.randint(0, 14))
            rows.append({"user_id": f"u{uid}", "event_date": signup, "event_name": "signup"})
            if random.random() < 0.4:
                rows.append({"user_id": f"u{uid}",
                            "event_date": signup + timedelta(days=1),
                            "event_name": "search"})
            for d in [1, 7, 14, 30]:
                if random.random() < (0.3 - 0.005 * d):
                    rows.append({"user_id": f"u{uid}",
                                "event_date": signup + timedelta(days=d),
                                "event_name": "pageview"})
        return pl.DataFrame(rows)

    def test_full_pipeline(self):
        """Run all stats modules then aggregate."""
        events = self._make_events()

        # Retention
        ret_result = RetentionComputer(events).compute(periods=[7, 30])

        # Anomaly (on synthetic weekly DAU)
        import random
        random.seed(42)
        dates = [date(2026, 1, 1) + timedelta(weeks=w) for w in range(12)]
        values = [200.0 + random.gauss(0, 10) for _ in range(12)]
        values[8] += 100  # spike
        series = MetricSeries("dau", dates, values)
        anom_result = AnomalyDetector(sigma_threshold=2.0, window=4).detect(series)

        # Features
        feat_result = FeatureCorrelationAnalyzer(events, min_feature_users=3).analyze(
            retention_periods=[7, 30]
        )

        # Aggregate
        summary = (
            StatsAggregator()
            .add_retention(ret_result.to_summary())
            .add_anomalies(anom_result.to_summary())
            .add_feature_correlation(feat_result.to_summary())
            .build()
        )

        assert summary.finding_count > 0
        assert len(summary.freshness.modules_available) == 3
        assert len(summary.freshness.modules_missing) == 0

        # Validate output formats
        d = summary.to_dict()
        assert d["schema_version"] == SCHEMA_VERSION
        assert len(d["findings"]) > 0

        text = summary.to_llm_prompt_context()
        assert "Product Analytics Summary" in text
        assert len(text) > 50

    def test_noise_reduction(self):
        """Aggregator with filtering produces fewer findings than unfiltered."""
        events = self._make_events()

        ret = RetentionComputer(events).compute(periods=[7, 30])
        feat = FeatureCorrelationAnalyzer(events, min_feature_users=3).analyze(
            retention_periods=[7, 30]
        )

        strict = (
            StatsAggregator(min_impact_threshold=0.10)
            .add_retention(ret.to_summary())
            .add_feature_correlation(feat.to_summary())
            .build()
        )
        loose = (
            StatsAggregator(min_impact_threshold=0.001)
            .add_retention(ret.to_summary())
            .add_feature_correlation(feat.to_summary())
            .build()
        )

        assert strict.finding_count <= loose.finding_count
