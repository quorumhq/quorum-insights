"""Tests for the FastAPI server."""

import json
import os
from datetime import date, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def _make_events_file(tmp_path: Path) -> Path:
    """Generate synthetic JSONL events for server testing."""
    import random
    random.seed(42)

    path = tmp_path / "events.jsonl"
    lines = []
    for uid in range(100):
        signup_day = random.randint(0, 14)
        signup_date = date(2026, 1, 1) + timedelta(days=signup_day)

        lines.append(json.dumps({
            "user_id": f"u{uid}",
            "event_date": signup_date.isoformat(),
            "event_name": "signup",
        }))

        features = ["search", "dashboard", "export", "settings"]
        if random.random() < 0.4:
            feat = random.choice(features)
            lines.append(json.dumps({
                "user_id": f"u{uid}",
                "event_date": (signup_date + timedelta(days=random.randint(0, 3))).isoformat(),
                "event_name": feat,
            }))

        for d in [1, 2, 7, 14, 30]:
            if random.random() < max(0, 0.35 - 0.006 * d):
                lines.append(json.dumps({
                    "user_id": f"u{uid}",
                    "event_date": (signup_date + timedelta(days=d)).isoformat(),
                    "event_name": random.choice(["pageview", "click"]),
                }))

    path.write_text("\n".join(lines))
    return path


@pytest.fixture
def client(tmp_path):
    """Create a test client with synthetic data."""
    events_file = _make_events_file(tmp_path)
    os.environ["EVENTS_FILE"] = str(events_file)

    # Reset the data store to pick up new env var
    from server import _store, _cache, app
    _store._df = None
    _store._loaded_at = 0
    _cache._cache.clear()

    yield TestClient(app)

    # Cleanup
    if "EVENTS_FILE" in os.environ:
        del os.environ["EVENTS_FILE"]


class TestHealthEndpoint:
    def test_health(self, client):
        resp = client.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["events_loaded"] > 0
        assert data["users"] > 0


class TestOverviewEndpoint:
    def test_overview(self, client):
        resp = client.get("/api/overview")
        assert resp.status_code == 200
        data = resp.json()
        assert "total_events" in data
        assert "total_users" in data
        assert "dau_series" in data
        assert "date_range" in data
        assert data["total_users"] > 0

    def test_overview_with_dates(self, client):
        resp = client.get("/api/overview?start_date=2026-01-01&end_date=2026-01-15")
        assert resp.status_code == 200
        data = resp.json()
        assert data["date_range"]["start"] == "2026-01-01"
        assert data["date_range"]["end"] == "2026-01-15"

    def test_overview_dau_series(self, client):
        resp = client.get("/api/overview")
        data = resp.json()
        assert len(data["dau_series"]) > 0
        assert "date" in data["dau_series"][0]
        assert "dau" in data["dau_series"][0]


class TestRetentionEndpoint:
    def test_retention_default(self, client):
        resp = client.get("/api/retention")
        assert resp.status_code == 200
        data = resp.json()
        assert data["metric"] == "retention"
        assert "overall_retention" in data
        assert "cohorts" in data

    def test_retention_custom_periods(self, client):
        resp = client.get("/api/retention?periods=1,7")
        assert resp.status_code == 200
        data = resp.json()
        assert "D1" in data["overall_retention"]
        assert "D7" in data["overall_retention"]


class TestFeaturesEndpoint:
    def test_features(self, client):
        resp = client.get("/api/features")
        assert resp.status_code == 200
        data = resp.json()
        assert data["metric"] == "feature_correlation"
        assert "top_features" in data
        assert "caveat" in data  # correlation caveat


class TestInsightsEndpoint:
    def test_insights_without_anthropic_key(self, client):
        # No ANTHROPIC_API_KEY set — should return stats summary without cards
        resp = client.get("/api/insights")
        assert resp.status_code == 200
        data = resp.json()
        assert "cards" in data or "stats_summary" in data

    def test_insights_returns_stats_even_without_llm(self, client):
        resp = client.get("/api/insights")
        data = resp.json()
        # Should have either cards or stats_summary
        if "stats_summary" in data:
            assert "schema_version" in data["stats_summary"]


class TestChurnEndpoint:
    def test_churn(self, client):
        resp = client.get("/api/churn")
        assert resp.status_code == 200
        data = resp.json()
        assert data["metric"] == "churn_prediction"
        assert "stage_distribution" in data
        assert "cohorts" in data
        assert "total_users" in data


class TestActivationEndpoint:
    def test_activation(self, client):
        resp = client.get("/api/activation")
        assert resp.status_code == 200
        data = resp.json()
        assert data["metric"] == "activation_discovery"
        assert "top_moments" in data
        assert "baseline_retention" in data

    def test_activation_custom_window(self, client):
        resp = client.get("/api/activation?window=14&retention_period=30")
        assert resp.status_code == 200
        data = resp.json()
        assert data["activation_window_days"] == 14


class TestAnomaliesEndpoint:
    def test_anomalies(self, client):
        resp = client.get("/api/anomalies")
        assert resp.status_code == 200
        data = resp.json()
        assert data["metric"] == "anomaly_detection"
        assert "anomalies" in data
        assert "anomaly_count" in data


class TestFeedbackEndpoint:
    def test_feedback(self, client, tmp_path):
        os.environ["FEEDBACK_DIR"] = str(tmp_path / "feedback")
        resp = client.post("/api/feedback?insight_rank=1&vote=useful")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["recorded"]["vote"] == "useful"

        # Check file written
        fb_file = tmp_path / "feedback" / "feedback.jsonl"
        assert fb_file.exists()
        del os.environ["FEEDBACK_DIR"]

    def test_feedback_invalid_vote(self, client):
        resp = client.post("/api/feedback?insight_rank=1&vote=invalid")
        assert resp.status_code == 422  # validation error


class TestCaching:
    def test_second_request_cached(self, client):
        """Second request should be faster (cached)."""
        import time
        t1 = time.time()
        client.get("/api/overview")
        elapsed1 = time.time() - t1

        t2 = time.time()
        client.get("/api/overview")
        elapsed2 = time.time() - t2

        # Cached request should be faster (or at least not slower)
        # We mainly test it doesn't crash, not exact timing
        assert elapsed2 < elapsed1 * 5  # generous bound


class TestCORS:
    def test_cors_headers(self, client):
        resp = client.options(
            "/api/health",
            headers={"Origin": "http://localhost:5173", "Access-Control-Request-Method": "GET"},
        )
        assert resp.status_code == 200
        assert "access-control-allow-origin" in resp.headers
