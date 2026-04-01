"""
Digest scheduler — configurable schedule, recipients, and delivery.

The scheduler manages:
- When digests run (daily/weekly/monthly)
- Who receives them (email addresses, Slack channels)
- How they're delivered (email via webhook, Slack via webhook)

Delivery is via webhooks — no SMTP or Slack SDK dependency.
The caller provides webhook URLs; the scheduler POSTs rendered content.

For MVP, the scheduler is a simple config + run() method.
Production would use cron, Temporal, or a task queue.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import Any, Callable, Optional

from digest.composer import Digest, DigestComposer, DigestConfig, DigestFrequency
from digest.renderer import render_html_email, render_markdown, render_slack_blocks
from intelligence.models import InsightResponse

logger = logging.getLogger(__name__)


class DeliveryChannel(str, Enum):
    EMAIL = "email"
    SLACK = "slack"
    MARKDOWN = "markdown"  # write to file or stdout


@dataclass
class Recipient:
    """A digest recipient."""

    channel: DeliveryChannel
    address: str  # email address, Slack webhook URL, or file path


@dataclass
class DigestSchedule:
    """Full digest schedule configuration."""

    frequency: DigestFrequency = DigestFrequency.WEEKLY
    recipients: list[Recipient] = field(default_factory=list)
    tenant_name: Optional[str] = None
    max_insights: int = 3
    feedback_base_url: Optional[str] = None
    evidence_base_url: Optional[str] = None
    email_webhook_url: Optional[str] = None  # e.g. SendGrid webhook


@dataclass
class DeliveryResult:
    """Result of a single delivery attempt."""

    channel: DeliveryChannel
    address: str
    success: bool
    error: Optional[str] = None


@dataclass
class DigestRunResult:
    """Result of a full digest run."""

    digest: Digest
    deliveries: list[DeliveryResult]
    generated_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @property
    def all_delivered(self) -> bool:
        return all(d.success for d in self.deliveries)

    @property
    def failure_count(self) -> int:
        return sum(1 for d in self.deliveries if not d.success)

    def to_dict(self) -> dict:
        return {
            "generated_at": self.generated_at,
            "has_insights": self.digest.has_insights,
            "insights_count": len(self.digest.insights),
            "deliveries": [
                {
                    "channel": d.channel.value,
                    "address": d.address,
                    "success": d.success,
                    "error": d.error,
                }
                for d in self.deliveries
            ],
            "all_delivered": self.all_delivered,
        }


class DigestScheduler:
    """Run and deliver digests.

    Usage:
        scheduler = DigestScheduler(schedule, http_post=httpx.post)
        result = scheduler.run(insight_response, stats_summary)
    """

    def __init__(
        self,
        schedule: DigestSchedule,
        http_post: Optional[Callable] = None,
    ):
        self.schedule = schedule
        self._http_post = http_post  # inject for testing

        self._composer = DigestComposer(DigestConfig(
            max_insights=schedule.max_insights,
            frequency=schedule.frequency,
            tenant_name=schedule.tenant_name,
            feedback_url=schedule.feedback_base_url,
            evidence_url=schedule.evidence_base_url,
        ))

    def run(
        self,
        insight_response: InsightResponse,
        stats_summary: Optional[dict] = None,
    ) -> DigestRunResult:
        """Compose digest and deliver to all recipients."""
        digest = self._composer.compose(insight_response, stats_summary)

        deliveries: list[DeliveryResult] = []
        for recipient in self.schedule.recipients:
            result = self._deliver(digest, recipient)
            deliveries.append(result)

        return DigestRunResult(digest=digest, deliveries=deliveries)

    def _deliver(self, digest: Digest, recipient: Recipient) -> DeliveryResult:
        """Deliver digest to a single recipient."""
        try:
            if recipient.channel == DeliveryChannel.MARKDOWN:
                return self._deliver_markdown(digest, recipient)
            elif recipient.channel == DeliveryChannel.EMAIL:
                return self._deliver_email(digest, recipient)
            elif recipient.channel == DeliveryChannel.SLACK:
                return self._deliver_slack(digest, recipient)
            else:
                return DeliveryResult(
                    channel=recipient.channel,
                    address=recipient.address,
                    success=False,
                    error=f"Unknown channel: {recipient.channel}",
                )
        except Exception as e:
            logger.exception("Delivery failed for %s", recipient.address)
            return DeliveryResult(
                channel=recipient.channel,
                address=recipient.address,
                success=False,
                error=str(e),
            )

    def _deliver_markdown(
        self, digest: Digest, recipient: Recipient
    ) -> DeliveryResult:
        """Write markdown to file or stdout."""
        md = render_markdown(digest)
        if recipient.address == "stdout":
            print(md)
        else:
            from pathlib import Path
            Path(recipient.address).write_text(md, encoding="utf-8")

        return DeliveryResult(
            channel=DeliveryChannel.MARKDOWN,
            address=recipient.address,
            success=True,
        )

    def _deliver_email(
        self, digest: Digest, recipient: Recipient
    ) -> DeliveryResult:
        """Send HTML email via webhook (e.g. SendGrid)."""
        if not self._http_post:
            return DeliveryResult(
                channel=DeliveryChannel.EMAIL,
                address=recipient.address,
                success=False,
                error="No http_post function configured for email delivery",
            )

        html = render_html_email(digest)
        webhook_url = self.schedule.email_webhook_url
        if not webhook_url:
            return DeliveryResult(
                channel=DeliveryChannel.EMAIL,
                address=recipient.address,
                success=False,
                error="No email_webhook_url configured",
            )

        subject = f"Insights Digest — {digest.period_label}"
        if digest.tenant_name:
            subject = f"[{digest.tenant_name}] {subject}"

        payload = {
            "to": recipient.address,
            "subject": subject,
            "html": html,
        }

        resp = self._http_post(webhook_url, json=payload)
        if hasattr(resp, "status_code") and resp.status_code >= 400:
            return DeliveryResult(
                channel=DeliveryChannel.EMAIL,
                address=recipient.address,
                success=False,
                error=f"Email webhook returned {resp.status_code}",
            )

        return DeliveryResult(
            channel=DeliveryChannel.EMAIL,
            address=recipient.address,
            success=True,
        )

    def _deliver_slack(
        self, digest: Digest, recipient: Recipient
    ) -> DeliveryResult:
        """Send Slack blocks via incoming webhook."""
        if not self._http_post:
            return DeliveryResult(
                channel=DeliveryChannel.SLACK,
                address=recipient.address,
                success=False,
                error="No http_post function configured for Slack delivery",
            )

        blocks = render_slack_blocks(digest)
        payload = {"blocks": blocks}

        resp = self._http_post(recipient.address, json=payload)
        if hasattr(resp, "status_code") and resp.status_code >= 400:
            return DeliveryResult(
                channel=DeliveryChannel.SLACK,
                address=recipient.address,
                success=False,
                error=f"Slack webhook returned {resp.status_code}",
            )

        return DeliveryResult(
            channel=DeliveryChannel.SLACK,
            address=recipient.address,
            success=True,
        )
