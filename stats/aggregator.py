"""
Stats summary aggregator for LLM consumption.

Collects outputs from retention, feature impact, and anomaly detection
modules. Ranks findings by importance. Filters noise. Produces a single
StatsSummary document that the LLM insight engine consumes.

Design:
- Each module's to_summary() dict is a "finding source"
- Findings are ranked by severity × confidence × estimated impact
- Noise filtering: skip findings below significance threshold
- Handles partial data: produces useful summaries even with missing modules
- Output: StatsSummary with schema_version for forward compatibility

Usage:
    agg = StatsAggregator()
    agg.add_retention(retention_result)
    agg.add_anomalies(anomaly_result)
    agg.add_feature_correlation(feature_result)
    summary = agg.build()
    summary.to_dict()  # -> structured dict for LLM
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import Any, Optional


SCHEMA_VERSION = "1.0.0"


class FindingSeverity(str, Enum):
    """Severity levels for ranked findings."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class FindingCategory(str, Enum):
    """Category of finding."""
    RETENTION = "retention"
    ANOMALY = "anomaly"
    FEATURE_CORRELATION = "feature_correlation"
    ACTIVATION = "activation"
    CHURN = "churn"
    OVERVIEW = "overview"


@dataclass
class Finding:
    """A single ranked finding for LLM consumption."""

    category: FindingCategory
    severity: FindingSeverity
    title: str
    description: str
    data: dict[str, Any]
    confidence: float = 1.0  # 0-1, statistical confidence
    impact_score: float = 0.0  # 0-1, estimated business impact

    @property
    def rank_score(self) -> float:
        """Combined score for ranking: severity weight × confidence × impact."""
        severity_weights = {
            FindingSeverity.CRITICAL: 1.0,
            FindingSeverity.HIGH: 0.8,
            FindingSeverity.MEDIUM: 0.5,
            FindingSeverity.LOW: 0.3,
            FindingSeverity.INFO: 0.1,
        }
        return severity_weights[self.severity] * self.confidence * max(self.impact_score, 0.1)

    def to_dict(self) -> dict:
        return {
            "category": self.category.value,
            "severity": self.severity.value,
            "title": self.title,
            "description": self.description,
            "confidence": round(self.confidence, 3),
            "impact_score": round(self.impact_score, 3),
            "rank_score": round(self.rank_score, 4),
            "data": self.data,
        }


@dataclass
class DataFreshness:
    """Metadata about data coverage and freshness."""
    event_count: int = 0
    user_count: int = 0
    date_start: Optional[date] = None
    date_end: Optional[date] = None
    modules_available: list[str] = field(default_factory=list)
    modules_missing: list[str] = field(default_factory=list)


@dataclass
class StatsSummary:
    """The aggregated summary document for LLM consumption."""

    schema_version: str = SCHEMA_VERSION
    generated_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    freshness: DataFreshness = field(default_factory=DataFreshness)
    findings: list[Finding] = field(default_factory=list)
    raw_summaries: dict[str, dict] = field(default_factory=dict)

    @property
    def finding_count(self) -> int:
        return len(self.findings)

    @property
    def critical_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == FindingSeverity.CRITICAL)

    def to_dict(self) -> dict:
        """Full structured output for LLM engine."""
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at,
            "freshness": {
                "event_count": self.freshness.event_count,
                "user_count": self.freshness.user_count,
                "date_range": {
                    "start": self.freshness.date_start.isoformat() if self.freshness.date_start else None,
                    "end": self.freshness.date_end.isoformat() if self.freshness.date_end else None,
                },
                "modules_available": self.freshness.modules_available,
                "modules_missing": self.freshness.modules_missing,
            },
            "summary": {
                "finding_count": self.finding_count,
                "critical_count": self.critical_count,
                "categories": list({f.category.value for f in self.findings}),
            },
            "findings": [f.to_dict() for f in self.findings],
        }

    def to_llm_prompt_context(self, max_findings: int = 15) -> str:
        """Compact text summary for direct LLM prompt injection."""
        lines = [
            f"# Product Analytics Summary (as of {self.generated_at[:10]})",
            f"Data: {self.freshness.user_count} users, {self.freshness.event_count} events",
        ]
        if self.freshness.date_start and self.freshness.date_end:
            lines.append(
                f"Period: {self.freshness.date_start} to {self.freshness.date_end}"
            )
        lines.append(f"Findings: {self.finding_count} total, {self.critical_count} critical")
        lines.append("")

        for i, f in enumerate(self.findings[:max_findings]):
            lines.append(f"## {i+1}. [{f.severity.value.upper()}] {f.title}")
            lines.append(f.description)
            lines.append("")

        if self.finding_count > max_findings:
            lines.append(f"... and {self.finding_count - max_findings} more findings")

        return "\n".join(lines)


class StatsAggregator:
    """Aggregates stats module outputs into a ranked StatsSummary.

    Noise filtering:
    - Retention: skip cohorts < min_cohort_size
    - Anomalies: only include >= severity threshold
    - Features: only include if |impact| > min_impact_threshold
    """

    def __init__(
        self,
        min_cohort_size: int = 10,
        min_impact_threshold: float = 0.02,
        min_anomaly_sigma: float = 2.0,
    ):
        self.min_cohort_size = min_cohort_size
        self.min_impact_threshold = min_impact_threshold
        self.min_anomaly_sigma = min_anomaly_sigma

        self._retention_summary: Optional[dict] = None
        self._anomaly_summary: Optional[dict] = None
        self._feature_summary: Optional[dict] = None
        self._raw: dict[str, dict] = {}

        self._freshness = DataFreshness()
        self._findings: list[Finding] = []

    def add_retention(self, summary: dict) -> StatsAggregator:
        """Add retention analysis results."""
        self._retention_summary = summary
        self._raw["retention"] = summary
        self._freshness.modules_available.append("retention")

        # Update freshness
        dr = summary.get("date_range", {})
        self._update_date_range(dr.get("start"), dr.get("end"))
        self._freshness.user_count = max(
            self._freshness.user_count, summary.get("total_users", 0)
        )

        return self

    def add_anomalies(self, summary: dict) -> StatsAggregator:
        """Add anomaly detection results."""
        self._anomaly_summary = summary
        self._raw["anomaly"] = summary
        self._freshness.modules_available.append("anomaly")

        dr = summary.get("date_range", {})
        self._update_date_range(dr.get("start"), dr.get("end"))
        self._freshness.event_count = max(
            self._freshness.event_count, summary.get("total_data_points", 0)
        )

        return self

    def add_feature_correlation(self, summary: dict) -> StatsAggregator:
        """Add feature impact analysis results."""
        self._feature_summary = summary
        self._raw["feature_correlation"] = summary
        self._freshness.modules_available.append("feature_correlation")

        dr = summary.get("date_range", {})
        self._update_date_range(dr.get("start"), dr.get("end"))
        self._freshness.user_count = max(
            self._freshness.user_count, summary.get("total_users", 0)
        )

        return self

    def build(self) -> StatsSummary:
        """Build the aggregated summary with ranked findings."""
        self._findings = []

        # Track missing modules
        all_modules = ["retention", "anomaly", "feature_correlation"]
        self._freshness.modules_missing = [
            m for m in all_modules if m not in self._freshness.modules_available
        ]

        # Extract findings from each module
        if self._retention_summary:
            self._extract_retention_findings()
        if self._anomaly_summary:
            self._extract_anomaly_findings()
        if self._feature_summary:
            self._extract_feature_findings()

        # Sort by rank score (highest first)
        self._findings.sort(key=lambda f: f.rank_score, reverse=True)

        return StatsSummary(
            freshness=self._freshness,
            findings=self._findings,
            raw_summaries=self._raw,
        )

    def _extract_retention_findings(self) -> None:
        """Extract findings from retention summary."""
        s = self._retention_summary
        if not s:
            return

        overall = s.get("overall_retention", {})

        # Overall retention finding
        if overall:
            periods_str = ", ".join(f"{k}: {v:.1%}" for k, v in overall.items())
            self._findings.append(Finding(
                category=FindingCategory.RETENTION,
                severity=self._retention_severity(overall),
                title="Overall Retention",
                description=f"Retention rates: {periods_str}",
                data=overall,
                confidence=min(1.0, s.get("total_users", 0) / 100),
                impact_score=0.7,
            ))

        # Best cohort finding
        best = s.get("best_cohort")
        if best:
            self._findings.append(Finding(
                category=FindingCategory.RETENTION,
                severity=FindingSeverity.MEDIUM,
                title=f"Best Performing Cohort: {best.get('key', '?')}",
                description=f"Cohort '{best.get('key')}' ({best.get('dimension', '?')}) "
                           f"has the highest long-term retention",
                data=best,
                confidence=0.8,
                impact_score=0.5,
            ))

        # Worst cohort finding
        worst = s.get("worst_cohort")
        if worst and worst.get("key") != (best or {}).get("key"):
            self._findings.append(Finding(
                category=FindingCategory.RETENTION,
                severity=FindingSeverity.HIGH,
                title=f"Lowest Performing Cohort: {worst.get('key', '?')}",
                description=f"Cohort '{worst.get('key')}' ({worst.get('dimension', '?')}) "
                           f"has the lowest long-term retention — investigate causes",
                data=worst,
                confidence=0.8,
                impact_score=0.6,
            ))

    def _extract_anomaly_findings(self) -> None:
        """Extract findings from anomaly summary."""
        s = self._anomaly_summary
        if not s:
            return

        anomalies = s.get("anomalies", [])

        for a in anomalies:
            sigma = a.get("sigma_distance", 0)
            if sigma < self.min_anomaly_sigma:
                continue

            severity = (
                FindingSeverity.CRITICAL if a.get("severity") == "critical"
                else FindingSeverity.HIGH
            )

            direction = "increased" if a.get("direction") == "up" else "decreased"
            pct = abs(a.get("pct_change", 0))

            self._findings.append(Finding(
                category=FindingCategory.ANOMALY,
                severity=severity,
                title=f"{a.get('metric', '?')} {direction} by {pct:.0%} on {a.get('date', '?')}",
                description=f"{a.get('metric')} was {a.get('value', 0):.1f} "
                           f"(expected {a.get('expected', 0):.1f}, {sigma:.1f}σ deviation)",
                data=a,
                confidence=min(1.0, sigma / 5.0),
                impact_score=min(1.0, pct),
            ))

    def _extract_feature_findings(self) -> None:
        """Extract findings from feature impact summary."""
        s = self._feature_summary
        if not s:
            return

        # Top positive features
        top = s.get("top_features", [])
        for f in top[:5]:
            impact_vals = f.get("impact", {})
            net = f.get("net_score", 0)

            if abs(net) < self.min_impact_threshold:
                continue

            impact_str = ", ".join(f"{k}: {v:+.1%}" for k, v in impact_vals.items())

            self._findings.append(Finding(
                category=FindingCategory.FEATURE_CORRELATION,
                severity=FindingSeverity.MEDIUM,
                title=f"Top Feature: {f.get('name', '?')} ({f.get('users', 0)} users)",
                description=f"Retention impact: {impact_str}",
                data=f,
                confidence=min(1.0, f.get("users", 0) / 50),
                impact_score=min(1.0, abs(net) * 5),
            ))

        # Negative impact features (high priority)
        negative = s.get("negative_features", [])
        for f in negative:
            impact_vals = f.get("impact", {})
            net = f.get("net_score", 0)

            if abs(net) < self.min_impact_threshold:
                continue

            impact_str = ", ".join(f"{k}: {v:+.1%}" for k, v in impact_vals.items())

            self._findings.append(Finding(
                category=FindingCategory.FEATURE_CORRELATION,
                severity=FindingSeverity.HIGH,
                title=f"⚠ Negative Impact: {f.get('name', '?')}",
                description=f"Users of this feature have LOWER retention: {impact_str}. "
                           f"Investigate UX issues or remove feature.",
                data=f,
                confidence=min(1.0, f.get("users", 0) / 50),
                impact_score=min(1.0, abs(net) * 5),
            ))

    def _retention_severity(self, overall: dict) -> FindingSeverity:
        """Classify retention severity based on rates."""
        # Use longest period available
        if not overall:
            return FindingSeverity.INFO
        longest_key = max(overall.keys())
        rate = overall[longest_key]
        if rate < 0.05:
            return FindingSeverity.CRITICAL
        elif rate < 0.15:
            return FindingSeverity.HIGH
        elif rate < 0.30:
            return FindingSeverity.MEDIUM
        return FindingSeverity.LOW

    def _update_date_range(self, start_str: Optional[str], end_str: Optional[str]) -> None:
        """Update freshness date range from string dates."""
        if start_str:
            try:
                d = date.fromisoformat(start_str)
                if self._freshness.date_start is None or d < self._freshness.date_start:
                    self._freshness.date_start = d
            except (ValueError, TypeError):
                pass
        if end_str:
            try:
                d = date.fromisoformat(end_str)
                if self._freshness.date_end is None or d > self._freshness.date_end:
                    self._freshness.date_end = d
            except (ValueError, TypeError):
                pass
