"""Tests for the digest layer: composer, renderers, scheduler."""

import json
from datetime import date
from pathlib import Path

import pytest

from digest.composer import DigestComposer, DigestConfig, Digest, DigestFrequency
from digest.renderer import render_markdown, render_html_email, render_slack_blocks
from digest.scheduler import (
    DigestScheduler,
    DigestSchedule,
    Recipient,
    DeliveryChannel,
    DigestRunResult,
)
from intelligence.models import InsightCard, InsightResponse, InsightSeverity


# ─── Test Data ───


def _make_cards() -> list[InsightCard]:
    return [
        InsightCard(
            title="Critical DAU spike needs investigation",
            severity=InsightSeverity.CRITICAL,
            finding="DAU increased 50% on Feb 15 (1500 vs expected 1000, 4.2σ).",
            evidence="anomaly detection: 4.2σ deviation",
            action="Check for bot traffic or viral event on Feb 15.",
            confidence=0.9,
            category="anomaly",
            estimated_impact="Understanding this spike could reveal a growth channel",
            related_metrics=["dau", "sessions"],
        ),
        InsightCard(
            title="D30 retention below SaaS benchmark",
            severity=InsightSeverity.HIGH,
            finding="D30 retention is 12%, below the 20% SaaS benchmark.",
            evidence="retention analysis: D1=45%, D7=28%, D30=12%",
            action="Run user interviews with D7-retained users who churned before D30.",
            confidence=0.95,
            category="retention",
            estimated_impact="Improving D30 from 12% to 20% would increase LTV by ~67%",
        ),
        InsightCard(
            title="Search feature correlated with higher retention",
            severity=InsightSeverity.MEDIUM,
            finding="Users of search have 8% higher D7 retention (45% vs 37%).",
            evidence="feature correlation: search, D7 retention",
            action="Promote search in onboarding flow.",
            confidence=0.7,
            category="feature_correlation",
        ),
        InsightCard(
            title="Mobile sessions growing steadily",
            severity=InsightSeverity.LOW,
            finding="Mobile traffic up 5% WoW for 4 consecutive weeks.",
            evidence="daily metrics: mobile sessions trend",
            action="Audit mobile UX for friction points.",
            confidence=0.6,
            category="overview",
        ),
    ]


def _make_response(cards=None) -> InsightResponse:
    return InsightResponse(
        cards=_make_cards() if cards is None else cards,
        prompt_version="v2",
        model="claude-sonnet-4-20250514",
    )


def _make_summary() -> dict:
    return {
        "schema_version": "1.0.0",
        "freshness": {
            "event_count": 50000,
            "user_count": 2000,
            "date_range": {"start": "2026-01-01", "end": "2026-03-31"},
            "modules_available": ["retention", "anomaly", "feature_correlation"],
            "modules_missing": [],
        },
        "summary": {"finding_count": 4, "critical_count": 1},
        "findings": [],
    }


# ─── Composer Tests ───


class TestDigestComposer:
    def test_basic_compose(self):
        composer = DigestComposer()
        digest = composer.compose(_make_response(), _make_summary())

        assert isinstance(digest, Digest)
        assert digest.has_insights
        assert len(digest.insights) == 3  # default max_insights=3

    def test_max_insights(self):
        config = DigestConfig(max_insights=2)
        composer = DigestComposer(config)
        digest = composer.compose(_make_response())

        assert len(digest.insights) == 2

    def test_ranking_order(self):
        """Highest severity × confidence first."""
        composer = DigestComposer(DigestConfig(max_insights=4))
        digest = composer.compose(_make_response())

        # CRITICAL (0.9 conf) should be first, HIGH (0.95 conf) second
        assert digest.insights[0].card.severity == InsightSeverity.CRITICAL
        assert digest.insights[1].card.severity == InsightSeverity.HIGH

    def test_rank_numbers(self):
        composer = DigestComposer()
        digest = composer.compose(_make_response())

        for i, insight in enumerate(digest.insights):
            assert insight.rank == i + 1

    def test_date_range_from_summary(self):
        composer = DigestComposer()
        digest = composer.compose(_make_response(), _make_summary())

        assert digest.date_range_start == "2026-01-01"
        assert digest.date_range_end == "2026-03-31"

    def test_period_label(self):
        composer = DigestComposer()
        digest = composer.compose(_make_response(), _make_summary())

        assert "2026-01-01" in digest.period_label
        assert "2026-03-31" in digest.period_label

    def test_tenant_name(self):
        config = DigestConfig(tenant_name="Acme Corp")
        composer = DigestComposer(config)
        digest = composer.compose(_make_response())

        assert digest.tenant_name == "Acme Corp"

    def test_feedback_urls(self):
        config = DigestConfig(feedback_url="https://app.example.com/feedback")
        composer = DigestComposer(config)
        digest = composer.compose(_make_response())

        assert digest.insights[0].feedback_url is not None
        assert "feedback" in digest.insights[0].feedback_url
        assert "insight=1" in digest.insights[0].feedback_url

    def test_evidence_urls(self):
        config = DigestConfig(evidence_url="https://app.example.com/evidence")
        composer = DigestComposer(config)
        digest = composer.compose(_make_response())

        assert digest.insights[0].evidence_url is not None
        assert "evidence" in digest.insights[0].evidence_url

    def test_no_insights(self):
        composer = DigestComposer()
        digest = composer.compose(_make_response(cards=[]))

        assert not digest.has_insights
        assert len(digest.insights) == 0
        assert digest.total_findings == 0

    def test_to_dict(self):
        composer = DigestComposer()
        digest = composer.compose(_make_response(), _make_summary())
        d = digest.to_dict()

        assert "insights" in d
        assert "period" in d
        assert d["total_findings"] == 4
        assert d["insights_shown"] == 3

    def test_total_findings_vs_shown(self):
        config = DigestConfig(max_insights=2)
        composer = DigestComposer(config)
        digest = composer.compose(_make_response())

        assert digest.total_findings == 4  # all cards
        assert len(digest.insights) == 2   # only top 2


# ─── Markdown Renderer Tests ───


class TestMarkdownRenderer:
    def test_renders_insights(self):
        composer = DigestComposer()
        digest = composer.compose(_make_response(), _make_summary())
        md = render_markdown(digest)

        assert "# " in md  # has header
        assert "CRITICAL" in md
        assert "DAU" in md
        assert "Action:" in md

    def test_includes_severity_emoji(self):
        composer = DigestComposer()
        digest = composer.compose(_make_response())
        md = render_markdown(digest)

        assert "🔴" in md  # critical
        assert "🟠" in md  # high

    def test_includes_evidence(self):
        composer = DigestComposer()
        digest = composer.compose(_make_response())
        md = render_markdown(digest)

        assert "Evidence:" in md
        assert "4.2σ" in md

    def test_includes_feedback_link(self):
        config = DigestConfig(feedback_url="https://example.com/fb")
        composer = DigestComposer(config)
        digest = composer.compose(_make_response())
        md = render_markdown(digest)

        assert "Was this useful?" in md

    def test_no_insights_message(self):
        composer = DigestComposer()
        digest = composer.compose(_make_response(cards=[]))
        md = render_markdown(digest)

        assert "No significant findings" in md

    def test_tenant_name_in_header(self):
        config = DigestConfig(tenant_name="Acme")
        composer = DigestComposer(config)
        digest = composer.compose(_make_response())
        md = render_markdown(digest)

        assert "Acme" in md

    def test_confidence_percentage(self):
        composer = DigestComposer()
        digest = composer.compose(_make_response())
        md = render_markdown(digest)

        assert "90%" in md or "95%" in md


# ─── HTML Email Renderer Tests ───


class TestHTMLEmailRenderer:
    def test_valid_html(self):
        composer = DigestComposer()
        digest = composer.compose(_make_response(), _make_summary())
        html = render_html_email(digest)

        assert html.startswith("<!DOCTYPE html>")
        assert "</html>" in html
        assert "<body" in html

    def test_inline_styles_only(self):
        """Email HTML must use inline styles, not <style> blocks."""
        composer = DigestComposer()
        digest = composer.compose(_make_response())
        html = render_html_email(digest)

        assert "<style" not in html

    def test_contains_insight_content(self):
        composer = DigestComposer()
        digest = composer.compose(_make_response())
        html = render_html_email(digest)

        assert "CRITICAL" in html
        assert "DAU" in html
        assert "4.2σ" in html

    def test_severity_colors(self):
        composer = DigestComposer()
        digest = composer.compose(_make_response())
        html = render_html_email(digest)

        assert "#dc2626" in html  # critical red
        assert "#ea580c" in html  # high orange

    def test_feedback_buttons(self):
        config = DigestConfig(feedback_url="https://example.com/fb")
        composer = DigestComposer(config)
        digest = composer.compose(_make_response())
        html = render_html_email(digest)

        assert "Useful" in html
        assert "Not useful" in html

    def test_no_insights_html(self):
        composer = DigestComposer()
        digest = composer.compose(_make_response(cards=[]))
        html = render_html_email(digest)

        assert "No significant findings" in html
        assert "✅" in html

    def test_table_layout_for_email(self):
        """Email should use table-based layout for Outlook compatibility."""
        composer = DigestComposer()
        digest = composer.compose(_make_response())
        html = render_html_email(digest)

        assert '<table role="presentation"' in html

    def test_meta_charset(self):
        composer = DigestComposer()
        digest = composer.compose(_make_response())
        html = render_html_email(digest)

        assert 'charset="utf-8"' in html


# ─── Slack Blocks Renderer Tests ───


class TestSlackBlocksRenderer:
    def test_valid_blocks(self):
        composer = DigestComposer()
        digest = composer.compose(_make_response(), _make_summary())
        blocks = render_slack_blocks(digest)

        assert isinstance(blocks, list)
        assert len(blocks) > 0
        assert all(isinstance(b, dict) for b in blocks)

    def test_has_header(self):
        composer = DigestComposer()
        digest = composer.compose(_make_response())
        blocks = render_slack_blocks(digest)

        header = blocks[0]
        assert header["type"] == "header"

    def test_has_dividers(self):
        composer = DigestComposer()
        digest = composer.compose(_make_response())
        blocks = render_slack_blocks(digest)

        dividers = [b for b in blocks if b.get("type") == "divider"]
        assert len(dividers) >= 1

    def test_insight_sections(self):
        composer = DigestComposer()
        digest = composer.compose(_make_response())
        blocks = render_slack_blocks(digest)

        sections = [b for b in blocks if b.get("type") == "section"]
        assert len(sections) >= 1
        # Should contain mrkdwn text with severity
        first_section = sections[0]
        assert "CRITICAL" in first_section["text"]["text"]

    def test_feedback_buttons(self):
        config = DigestConfig(feedback_url="https://example.com/fb")
        composer = DigestComposer(config)
        digest = composer.compose(_make_response())
        blocks = render_slack_blocks(digest)

        actions = [b for b in blocks if b.get("type") == "actions"]
        assert len(actions) >= 1
        assert "Useful" in actions[0]["elements"][0]["text"]["text"]

    def test_no_insights_slack(self):
        composer = DigestComposer()
        digest = composer.compose(_make_response(cards=[]))
        blocks = render_slack_blocks(digest)

        all_text = json.dumps(blocks)
        assert "No significant findings" in all_text

    def test_footer_context(self):
        composer = DigestComposer()
        digest = composer.compose(_make_response())
        blocks = render_slack_blocks(digest)

        # Last block should be context with generation info
        last = blocks[-1]
        assert last["type"] == "context"
        assert "Quorum Insights" in last["elements"][0]["text"]


# ─── Scheduler Tests ───


class MockHTTPResponse:
    def __init__(self, status_code=200):
        self.status_code = status_code


class TestDigestScheduler:
    def test_markdown_to_file(self, tmp_path):
        output = tmp_path / "digest.md"
        schedule = DigestSchedule(
            recipients=[Recipient(DeliveryChannel.MARKDOWN, str(output))],
        )
        scheduler = DigestScheduler(schedule)
        result = scheduler.run(_make_response(), _make_summary())

        assert result.all_delivered
        assert output.exists()
        content = output.read_text()
        assert "CRITICAL" in content
        assert "DAU" in content

    def test_email_delivery(self):
        calls = []

        def mock_post(url, json=None):
            calls.append({"url": url, "json": json})
            return MockHTTPResponse(200)

        schedule = DigestSchedule(
            recipients=[Recipient(DeliveryChannel.EMAIL, "alice@example.com")],
            email_webhook_url="https://api.sendgrid.com/v3/mail",
        )
        scheduler = DigestScheduler(schedule, http_post=mock_post)
        result = scheduler.run(_make_response())

        assert result.all_delivered
        assert len(calls) == 1
        assert calls[0]["json"]["to"] == "alice@example.com"
        assert "Insights Digest" in calls[0]["json"]["subject"]
        assert "<html" in calls[0]["json"]["html"]

    def test_slack_delivery(self):
        calls = []

        def mock_post(url, json=None):
            calls.append({"url": url, "json": json})
            return MockHTTPResponse(200)

        webhook = "https://hooks.slack.com/services/T00/B00/xxx"
        schedule = DigestSchedule(
            recipients=[Recipient(DeliveryChannel.SLACK, webhook)],
        )
        scheduler = DigestScheduler(schedule, http_post=mock_post)
        result = scheduler.run(_make_response())

        assert result.all_delivered
        assert len(calls) == 1
        assert calls[0]["url"] == webhook
        assert "blocks" in calls[0]["json"]

    def test_multiple_recipients(self, tmp_path):
        calls = []

        def mock_post(url, json=None):
            calls.append({"url": url, "json": json})
            return MockHTTPResponse(200)

        md_file = tmp_path / "digest.md"
        schedule = DigestSchedule(
            recipients=[
                Recipient(DeliveryChannel.MARKDOWN, str(md_file)),
                Recipient(DeliveryChannel.EMAIL, "bob@example.com"),
                Recipient(DeliveryChannel.SLACK, "https://hooks.slack.com/x"),
            ],
            email_webhook_url="https://sendgrid.example.com",
        )
        scheduler = DigestScheduler(schedule, http_post=mock_post)
        result = scheduler.run(_make_response())

        assert result.all_delivered
        assert len(result.deliveries) == 3
        assert md_file.exists()
        assert len(calls) == 2  # email + slack

    def test_email_failure(self):
        def mock_post(url, json=None):
            return MockHTTPResponse(500)

        schedule = DigestSchedule(
            recipients=[Recipient(DeliveryChannel.EMAIL, "fail@example.com")],
            email_webhook_url="https://api.example.com/send",
        )
        scheduler = DigestScheduler(schedule, http_post=mock_post)
        result = scheduler.run(_make_response())

        assert not result.all_delivered
        assert result.failure_count == 1
        assert "500" in result.deliveries[0].error

    def test_no_http_post_for_email(self):
        schedule = DigestSchedule(
            recipients=[Recipient(DeliveryChannel.EMAIL, "x@y.com")],
            email_webhook_url="https://api.example.com",
        )
        scheduler = DigestScheduler(schedule, http_post=None)
        result = scheduler.run(_make_response())

        assert not result.all_delivered
        assert "http_post" in result.deliveries[0].error

    def test_no_webhook_url_for_email(self):
        def mock_post(url, json=None):
            return MockHTTPResponse(200)

        schedule = DigestSchedule(
            recipients=[Recipient(DeliveryChannel.EMAIL, "x@y.com")],
            # no email_webhook_url
        )
        scheduler = DigestScheduler(schedule, http_post=mock_post)
        result = scheduler.run(_make_response())

        assert not result.all_delivered
        assert "email_webhook_url" in result.deliveries[0].error

    def test_tenant_name_in_email_subject(self):
        calls = []

        def mock_post(url, json=None):
            calls.append(json)
            return MockHTTPResponse(200)

        schedule = DigestSchedule(
            recipients=[Recipient(DeliveryChannel.EMAIL, "x@y.com")],
            email_webhook_url="https://api.example.com",
            tenant_name="Acme Corp",
        )
        scheduler = DigestScheduler(schedule, http_post=mock_post)
        scheduler.run(_make_response())

        assert "[Acme Corp]" in calls[0]["subject"]

    def test_result_to_dict(self, tmp_path):
        output = tmp_path / "digest.md"
        schedule = DigestSchedule(
            recipients=[Recipient(DeliveryChannel.MARKDOWN, str(output))],
        )
        scheduler = DigestScheduler(schedule)
        result = scheduler.run(_make_response())
        d = result.to_dict()

        assert "deliveries" in d
        assert d["all_delivered"] is True
        assert d["has_insights"] is True

    def test_no_insights_still_delivers(self, tmp_path):
        output = tmp_path / "digest.md"
        schedule = DigestSchedule(
            recipients=[Recipient(DeliveryChannel.MARKDOWN, str(output))],
        )
        scheduler = DigestScheduler(schedule)
        result = scheduler.run(_make_response(cards=[]))

        assert result.all_delivered
        content = output.read_text()
        assert "No significant findings" in content
