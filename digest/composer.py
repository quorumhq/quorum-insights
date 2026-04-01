"""
Digest composer — assembles stats + LLM insights into a ranked digest.

The composer is the orchestration layer:
1. Takes an InsightResponse (from the LLM engine)
2. Selects top N insights by severity × confidence
3. Adds metadata: date range, data freshness, tenant info
4. Produces a Digest object ready for rendering

The composer does NOT call the LLM or run stats — those are upstream.
It also does NOT deliver — renderers and delivery are downstream.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import Optional

from intelligence.models import InsightCard, InsightResponse, InsightSeverity


class DigestFrequency(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


_SEVERITY_WEIGHT = {
    InsightSeverity.CRITICAL: 1.0,
    InsightSeverity.HIGH: 0.8,
    InsightSeverity.MEDIUM: 0.5,
    InsightSeverity.LOW: 0.3,
    InsightSeverity.INFO: 0.1,
}


@dataclass
class DigestConfig:
    """Configuration for digest generation."""

    max_insights: int = 3
    frequency: DigestFrequency = DigestFrequency.WEEKLY
    include_no_insights_message: bool = True
    tenant_name: Optional[str] = None
    feedback_url: Optional[str] = None  # base URL for "was this useful?" links
    evidence_url: Optional[str] = None  # base URL for "view full evidence" links


@dataclass
class DigestInsight:
    """A single insight in the digest, enriched with ranking metadata."""

    card: InsightCard
    rank: int  # 1-indexed
    rank_score: float
    feedback_url: Optional[str] = None  # "was this useful?" link
    evidence_url: Optional[str] = None  # link to full evidence view

    def to_dict(self) -> dict:
        return {
            "rank": self.rank,
            "rank_score": round(self.rank_score, 4),
            "card": self.card.to_dict(),
            "feedback_url": self.feedback_url,
            "evidence_url": self.evidence_url,
        }


@dataclass
class Digest:
    """A complete digest ready for rendering."""

    insights: list[DigestInsight]
    generated_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    date_range_start: Optional[str] = None
    date_range_end: Optional[str] = None
    tenant_name: Optional[str] = None
    frequency: DigestFrequency = DigestFrequency.WEEKLY
    total_findings: int = 0  # how many findings before filtering to top N
    data_freshness: Optional[dict] = None  # from StatsSummary
    prompt_version: Optional[str] = None
    model: Optional[str] = None

    @property
    def has_insights(self) -> bool:
        return len(self.insights) > 0

    @property
    def period_label(self) -> str:
        """Human-readable period label for the digest header."""
        if self.date_range_start and self.date_range_end:
            return f"{self.date_range_start} — {self.date_range_end}"
        return self.frequency.value.capitalize()

    def to_dict(self) -> dict:
        return {
            "generated_at": self.generated_at,
            "period": self.period_label,
            "date_range": {
                "start": self.date_range_start,
                "end": self.date_range_end,
            },
            "tenant_name": self.tenant_name,
            "frequency": self.frequency.value,
            "total_findings": self.total_findings,
            "insights_shown": len(self.insights),
            "insights": [i.to_dict() for i in self.insights],
            "prompt_version": self.prompt_version,
            "model": self.model,
        }


class DigestComposer:
    """Compose a digest from LLM insight response + stats metadata.

    Usage:
        composer = DigestComposer(config)
        digest = composer.compose(
            insight_response=engine.generate(summary),
            stats_summary=summary,
        )
    """

    def __init__(self, config: Optional[DigestConfig] = None):
        self.config = config or DigestConfig()

    def compose(
        self,
        insight_response: InsightResponse,
        stats_summary: Optional[dict] = None,
    ) -> Digest:
        """Compose a digest from insight response and optional stats metadata."""
        # Rank cards by severity × confidence
        ranked = self._rank_cards(insight_response.cards)

        # Take top N
        top = ranked[: self.config.max_insights]

        # Build DigestInsight objects
        insights = []
        for i, (card, score) in enumerate(top):
            feedback_url = None
            evidence_url = None
            if self.config.feedback_url:
                feedback_url = f"{self.config.feedback_url}?insight={i + 1}"
            if self.config.evidence_url:
                evidence_url = f"{self.config.evidence_url}?category={card.category}"

            insights.append(DigestInsight(
                card=card,
                rank=i + 1,
                rank_score=score,
                feedback_url=feedback_url,
                evidence_url=evidence_url,
            ))

        # Extract metadata from stats summary
        date_start = None
        date_end = None
        data_freshness = None
        if stats_summary:
            freshness = stats_summary.get("freshness", {})
            dr = freshness.get("date_range", {})
            date_start = dr.get("start")
            date_end = dr.get("end")
            data_freshness = freshness

        return Digest(
            insights=insights,
            date_range_start=date_start,
            date_range_end=date_end,
            tenant_name=self.config.tenant_name,
            frequency=self.config.frequency,
            total_findings=len(insight_response.cards),
            data_freshness=data_freshness,
            prompt_version=insight_response.prompt_version,
            model=insight_response.model,
        )

    def _rank_cards(
        self, cards: list[InsightCard]
    ) -> list[tuple[InsightCard, float]]:
        """Rank cards by severity weight × confidence. Highest first."""
        scored = []
        for card in cards:
            weight = _SEVERITY_WEIGHT.get(card.severity, 0.1)
            score = weight * card.confidence
            scored.append((card, score))

        scored.sort(key=lambda x: x[1], reverse=True)
        return scored
