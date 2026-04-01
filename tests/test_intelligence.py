"""Tests for the LLM insight generation engine."""

import json
import tempfile
from datetime import date
from pathlib import Path

import pytest

from intelligence.models import InsightCard, InsightSeverity, InsightResponse
from intelligence.prompts import PromptRegistry, PROMPT_VERSIONS
from intelligence.cache import InsightCache
from intelligence.engine import InsightEngine, InsightEngineConfig


# ─── Test Data ───


def _sample_summary() -> dict:
    """Minimal stats summary for testing."""
    return {
        "schema_version": "1.0.0",
        "freshness": {
            "event_count": 50000,
            "user_count": 2000,
            "date_range": {"start": "2026-01-01", "end": "2026-03-31"},
            "modules_available": ["retention", "anomaly", "feature_correlation"],
            "modules_missing": [],
        },
        "summary": {
            "finding_count": 5,
            "critical_count": 1,
            "categories": ["retention", "anomaly", "feature_correlation"],
        },
        "findings": [
            {
                "category": "anomaly",
                "severity": "critical",
                "title": "DAU spiked 50% on 2026-02-15",
                "description": "DAU was 1500 (expected 1000, 4.2σ)",
                "confidence": 0.9,
                "impact_score": 0.8,
                "data": {"value": 1500, "expected": 1000},
            },
            {
                "category": "retention",
                "severity": "medium",
                "title": "D30 retention at 12%",
                "description": "Overall D30 retention is 12%",
                "confidence": 0.95,
                "impact_score": 0.7,
                "data": {"D30": 0.12},
            },
        ],
    }


def _sample_llm_response() -> str:
    """Simulated LLM JSON response."""
    return json.dumps([
        {
            "title": "Critical DAU spike needs investigation",
            "severity": "critical",
            "finding": "DAU increased 50% on Feb 15 (1500 vs expected 1000, 4.2σ).",
            "evidence": "anomaly detection: 4.2σ deviation, DAU metric",
            "action": "Check for bot traffic or viral event on Feb 15. If organic, identify the source.",
            "confidence": 0.9,
            "category": "anomaly",
            "estimated_impact": "Understanding this spike could reveal a growth channel",
            "related_metrics": ["dau", "sessions"],
        },
        {
            "title": "D30 retention below SaaS benchmark",
            "severity": "high",
            "finding": "D30 retention is 12%, below the 20% SaaS benchmark.",
            "evidence": "retention analysis: D1=45%, D7=28%, D30=12%",
            "action": "Run user interviews with D7-retained users who churned before D30.",
            "confidence": 0.95,
            "category": "retention",
            "estimated_impact": "Improving D30 from 12% to 20% would increase LTV by ~67%",
            "related_metrics": ["retention_d30", "retention_d7"],
        },
    ])


class MockLLMClient:
    """Mock LLM client that returns canned responses."""

    def __init__(self, response: str = ""):
        self.response = response
        self.calls: list[dict] = []

    def generate(self, system: str, user: str, model: str) -> tuple[str, dict]:
        self.calls.append({"system": system, "user": user, "model": model})
        return self.response, {"input": 100, "output": 50}


# ─── InsightCard Tests ───


class TestInsightCard:
    def test_to_dict(self):
        card = InsightCard(
            title="Test",
            severity=InsightSeverity.HIGH,
            finding="finding",
            evidence="evidence",
            action="action",
            confidence=0.85,
            category="retention",
        )
        d = card.to_dict()
        assert d["title"] == "Test"
        assert d["severity"] == "high"
        assert d["confidence"] == 0.85

    def test_from_dict(self):
        d = {
            "title": "Test",
            "severity": "critical",
            "finding": "f",
            "evidence": "e",
            "action": "a",
            "confidence": 0.9,
            "category": "anomaly",
        }
        card = InsightCard.from_dict(d)
        assert card.severity == InsightSeverity.CRITICAL
        assert card.confidence == 0.9

    def test_roundtrip(self):
        card = InsightCard(
            title="Roundtrip",
            severity=InsightSeverity.MEDIUM,
            finding="f",
            evidence="e",
            action="a",
            confidence=0.7,
            category="retention",
            estimated_impact="~5% improvement",
            related_metrics=["dau", "retention_d7"],
        )
        restored = InsightCard.from_dict(card.to_dict())
        assert restored.title == card.title
        assert restored.estimated_impact == card.estimated_impact
        assert restored.related_metrics == card.related_metrics


# ─── Prompt Tests ───


class TestPrompts:
    def test_v1_exists(self):
        p = PromptRegistry.get("v1")
        assert p.version == "v1"
        assert "{stats_summary}" in p.user_template
        assert len(p.system) > 100

    def test_v2_exists(self):
        p = PromptRegistry.get("v2")
        assert p.version == "v2"
        assert "associated with" in p.system.lower() or "causal" in p.system.lower()

    def test_unknown_version_raises(self):
        with pytest.raises(ValueError, match="Unknown prompt"):
            PromptRegistry.get("v99")

    def test_list_versions(self):
        versions = PromptRegistry.list_versions()
        assert "v1" in versions
        assert "v2" in versions

    def test_render_user(self):
        p = PromptRegistry.get("v1")
        rendered = p.render_user("test summary data")
        assert "test summary data" in rendered
        assert "{stats_summary}" not in rendered

    def test_at_least_two_versions(self):
        """Acceptance criteria: at least 2 prompt versions testable."""
        assert len(PROMPT_VERSIONS) >= 2


# ─── Cache Tests ───


class TestInsightCache:
    def test_cache_miss(self, tmp_path):
        cache = InsightCache(tmp_path / "cache")
        assert cache.get("nonexistent") is None

    def test_cache_put_get(self, tmp_path):
        cache = InsightCache(tmp_path / "cache")
        cache.put("key1", '{"cards": []}', "v1", "claude-sonnet-4")
        result = cache.get("key1")
        assert result == '{"cards": []}'

    def test_cache_key_deterministic(self, tmp_path):
        cache = InsightCache(tmp_path / "cache")
        k1 = cache.cache_key("v1", "summary A")
        k2 = cache.cache_key("v1", "summary A")
        assert k1 == k2

    def test_cache_key_different_versions(self, tmp_path):
        cache = InsightCache(tmp_path / "cache")
        k1 = cache.cache_key("v1", "summary A", "model-a")
        k2 = cache.cache_key("v2", "summary A", "model-a")
        assert k1 != k2

    def test_cache_key_different_summaries(self, tmp_path):
        cache = InsightCache(tmp_path / "cache")
        k1 = cache.cache_key("v1", "summary A", "model-a")
        k2 = cache.cache_key("v1", "summary B", "model-a")
        assert k1 != k2

    def test_cache_key_different_models(self, tmp_path):
        cache = InsightCache(tmp_path / "cache")
        k1 = cache.cache_key("v1", "summary A", "claude-sonnet")
        k2 = cache.cache_key("v1", "summary A", "gpt-4o")
        assert k1 != k2

    def test_cache_ttl_expiry(self, tmp_path):
        import time
        cache = InsightCache(tmp_path / "cache", ttl_seconds=1)
        cache.put("k1", "response", "v1", "model")
        assert cache.get("k1") == "response"  # fresh
        time.sleep(1.1)
        assert cache.get("k1") is None  # expired

    def test_cache_clear(self, tmp_path):
        cache = InsightCache(tmp_path / "cache")
        cache.put("k1", "r1", "v1", "model")
        cache.put("k2", "r2", "v1", "model")
        assert cache.size == 2
        removed = cache.clear()
        assert removed == 2
        assert cache.size == 0

    def test_cache_size(self, tmp_path):
        cache = InsightCache(tmp_path / "cache")
        assert cache.size == 0
        cache.put("k1", "r1", "v1", "model")
        assert cache.size == 1


# ─── Engine Tests ───


class TestInsightEngine:
    def test_dry_run(self):
        engine = InsightEngine(config=InsightEngineConfig(cache_enabled=False))
        response = engine.generate(_sample_summary(), dry_run=True)

        assert isinstance(response, InsightResponse)
        assert len(response.cards) == 0
        assert response.prompt_version == "v2"  # default
        assert response.token_usage["input"] > 0

    def test_generate_with_mock(self, tmp_path):
        mock = MockLLMClient(response=_sample_llm_response())
        config = InsightEngineConfig(cache_dir=str(tmp_path / "cache"))
        engine = InsightEngine(client=mock, config=config)

        response = engine.generate(_sample_summary())

        assert len(response.cards) == 2
        assert response.cards[0].severity == InsightSeverity.CRITICAL
        assert response.cards[0].category == "anomaly"
        assert response.cached is False
        assert mock.calls[0]["model"] == config.model

    def test_cache_hit(self, tmp_path):
        mock = MockLLMClient(response=_sample_llm_response())
        config = InsightEngineConfig(cache_dir=str(tmp_path / "cache"))
        engine = InsightEngine(client=mock, config=config)

        # First call: cache miss
        r1 = engine.generate(_sample_summary())
        assert r1.cached is False
        assert len(mock.calls) == 1

        # Second call: cache hit
        r2 = engine.generate(_sample_summary())
        assert r2.cached is True
        assert len(mock.calls) == 1  # no additional LLM call

        # Same cards
        assert len(r2.cards) == len(r1.cards)
        assert r2.cards[0].title == r1.cards[0].title

    def test_cache_reduces_api_calls(self, tmp_path):
        """Acceptance criteria: caching reduces API costs by >50%."""
        mock = MockLLMClient(response=_sample_llm_response())
        config = InsightEngineConfig(cache_dir=str(tmp_path / "cache"))
        engine = InsightEngine(client=mock, config=config)

        summary = _sample_summary()
        # 10 calls with same input
        for _ in range(10):
            engine.generate(summary)

        # Should have only 1 actual LLM call
        assert len(mock.calls) == 1  # 90% reduction

    def test_no_client_raises(self):
        engine = InsightEngine(config=InsightEngineConfig(cache_enabled=False))
        with pytest.raises(RuntimeError, match="No LLM client"):
            engine.generate(_sample_summary())

    def test_confidence_filtering(self, tmp_path):
        low_confidence = json.dumps([
            {
                "title": "Low confidence",
                "severity": "info",
                "finding": "f",
                "evidence": "e",
                "action": "a",
                "confidence": 0.1,  # below threshold
                "category": "overview",
            },
            {
                "title": "High confidence",
                "severity": "high",
                "finding": "f",
                "evidence": "e",
                "action": "a",
                "confidence": 0.9,
                "category": "retention",
            },
        ])
        mock = MockLLMClient(response=low_confidence)
        config = InsightEngineConfig(
            cache_enabled=False, min_confidence=0.3,
        )
        engine = InsightEngine(client=mock, config=config)
        response = engine.generate(_sample_summary())

        assert len(response.cards) == 1
        assert response.cards[0].title == "High confidence"

    def test_max_cards_limit(self, tmp_path):
        many_cards = json.dumps([
            {
                "title": f"Card {i}",
                "severity": "low",
                "finding": "f",
                "evidence": "e",
                "action": "a",
                "confidence": 0.8,
                "category": "overview",
            }
            for i in range(20)
        ])
        mock = MockLLMClient(response=many_cards)
        config = InsightEngineConfig(cache_enabled=False, max_cards=5)
        engine = InsightEngine(client=mock, config=config)
        response = engine.generate(_sample_summary())

        assert len(response.cards) == 5

    def test_handles_malformed_json(self, tmp_path):
        mock = MockLLMClient(response="This is not JSON at all")
        config = InsightEngineConfig(cache_enabled=False)
        engine = InsightEngine(client=mock, config=config)
        response = engine.generate(_sample_summary())

        assert len(response.cards) == 0

    def test_handles_markdown_wrapped_json(self, tmp_path):
        wrapped = "```json\n" + _sample_llm_response() + "\n```"
        mock = MockLLMClient(response=wrapped)
        config = InsightEngineConfig(cache_enabled=False)
        engine = InsightEngine(client=mock, config=config)
        response = engine.generate(_sample_summary())

        assert len(response.cards) == 2

    def test_handles_partial_cards(self, tmp_path):
        partial = json.dumps([
            {"title": "Good", "severity": "high", "finding": "f",
             "evidence": "e", "action": "a", "confidence": 0.8, "category": "retention"},
            {"title": "Missing required fields"},  # malformed
        ])
        mock = MockLLMClient(response=partial)
        config = InsightEngineConfig(cache_enabled=False)
        engine = InsightEngine(client=mock, config=config)
        response = engine.generate(_sample_summary())

        assert len(response.cards) == 1

    def test_custom_prompt_version(self, tmp_path):
        mock = MockLLMClient(response=_sample_llm_response())
        config = InsightEngineConfig(
            cache_enabled=False, prompt_version="v1",
        )
        engine = InsightEngine(client=mock, config=config)
        response = engine.generate(_sample_summary())

        assert response.prompt_version == "v1"
        # v1 system prompt should be used
        assert "OPINIONATED" in mock.calls[0]["system"]

    def test_response_to_dict(self, tmp_path):
        mock = MockLLMClient(response=_sample_llm_response())
        config = InsightEngineConfig(cache_enabled=False)
        engine = InsightEngine(client=mock, config=config)
        response = engine.generate(_sample_summary())

        d = response.to_dict()
        assert "cards" in d
        assert "prompt_version" in d
        assert "model" in d
        assert "generated_at" in d
        assert len(d["cards"]) == 2


class TestEdgeCases:
    """Test edge cases from acceptance criteria."""

    def test_no_anomalies(self, tmp_path):
        """Handle summary with no anomalies gracefully."""
        summary = _sample_summary()
        summary["findings"] = []
        summary["summary"]["finding_count"] = 0
        summary["summary"]["critical_count"] = 0

        mock = MockLLMClient(response="[]")
        config = InsightEngineConfig(cache_enabled=False)
        engine = InsightEngine(client=mock, config=config)
        response = engine.generate(summary)

        assert len(response.cards) == 0
        assert len(mock.calls) == 1  # still called LLM

    def test_insufficient_data(self, tmp_path):
        """Handle summary with very little data."""
        summary = {
            "schema_version": "1.0.0",
            "freshness": {
                "event_count": 5,
                "user_count": 2,
                "date_range": {"start": "2026-03-30", "end": "2026-03-31"},
                "modules_available": ["retention"],
                "modules_missing": ["anomaly", "feature_correlation"],
            },
            "summary": {"finding_count": 0, "critical_count": 0, "categories": []},
            "findings": [],
        }

        mock = MockLLMClient(response="[]")
        config = InsightEngineConfig(cache_enabled=False)
        engine = InsightEngine(client=mock, config=config)
        response = engine.generate(summary)

        assert isinstance(response, InsightResponse)

    def test_single_user_cohorts(self, tmp_path):
        """Summary with single-user cohorts."""
        summary = _sample_summary()
        summary["freshness"]["user_count"] = 1

        response_json = json.dumps([{
            "title": "Insufficient data",
            "severity": "info",
            "finding": "Only 1 user — no statistical significance possible.",
            "evidence": "user_count = 1",
            "action": "Wait for more users before analyzing.",
            "confidence": 0.1,
            "category": "overview",
        }])
        mock = MockLLMClient(response=response_json)
        config = InsightEngineConfig(cache_enabled=False, min_confidence=0.0)
        engine = InsightEngine(client=mock, config=config)
        response = engine.generate(summary)

        assert len(response.cards) == 1
