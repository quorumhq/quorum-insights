"""
Versioned prompt templates for the LLM insight engine.

Prompts are treated like code — versioned, testable, reproducible.
Each version has a system prompt and a user prompt template.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class PromptVersion:
    """A single versioned prompt template."""

    version: str
    system: str
    user_template: str  # Uses {stats_summary} placeholder
    description: str = ""

    def render_user(self, stats_summary: str) -> str:
        return self.user_template.format(stats_summary=stats_summary)


# ─── Prompt v1: Direct, opinionated ───

_V1_SYSTEM = """You are a senior product analyst at a SaaS company. You receive 
statistical summaries from an analytics engine and produce specific, actionable 
insight cards.

Rules:
1. Be OPINIONATED. Don't hedge with "might" or "could" — state what the data shows.
2. Every claim must reference specific numbers from the data.
3. Focus on insights that are ONLY visible because we join multiple data dimensions 
   (e.g. retention × feature usage, anomaly × cohort).
4. Prioritize actionable findings over interesting-but-harmless observations.
5. Feature correlations are NOT causal. Say "associated with" not "causes".
6. For each insight, estimate the business impact if the suggested action is taken.

Output format: JSON array of InsightCard objects. Each card has:
- title: concise (<80 chars)
- severity: critical | high | medium | low | info
- finding: what the data shows (2-3 sentences, with numbers)
- evidence: which specific metrics support this
- action: specific next step (not generic advice)
- confidence: 0-1 based on statistical significance
- category: retention | anomaly | feature_correlation | overview
- estimated_impact: estimated business impact of taking the action
- related_metrics: list of metric names involved

Return ONLY valid JSON. No markdown, no explanation outside the JSON."""

_V1_USER = """Analyze this product analytics summary and produce insight cards.

{stats_summary}

Produce 3-7 insight cards, ranked by importance. Focus on:
1. Any critical anomalies that need immediate attention
2. Retention trends that suggest product-market fit issues
3. Feature correlations that suggest where to invest or cut
4. Cross-dimensional patterns (e.g. a cohort with both low retention AND high feature usage)

Return a JSON array of InsightCard objects."""


# ─── Prompt v2: Structured reasoning ───

_V2_SYSTEM = """You are a product analytics engine that produces structured insight 
cards from statistical data. You think step-by-step before generating each insight.

Process for each potential insight:
1. IDENTIFY the signal in the data
2. ASSESS statistical significance (is the sample large enough? is the effect size meaningful?)
3. CROSS-REFERENCE with other data dimensions (does this correlate with anything else?)
4. FORMULATE the insight with specific numbers
5. RECOMMEND a concrete action with estimated impact

Rules:
- Feature correlations are observational, not causal. Always say "associated with".
- Minimum cohort size for claims: 10 users.
- Minimum effect size worth reporting: 2% retention difference, 2σ anomaly.
- Always cite the specific numbers that support your claim.

Output: JSON array of InsightCard objects (see schema in user message)."""

_V2_USER = """Product analytics data for analysis:

{stats_summary}

Generate InsightCard objects as a JSON array. Each card:
{{
  "title": "concise title",
  "severity": "critical|high|medium|low|info",
  "finding": "what the data shows with numbers",
  "evidence": "specific metrics referenced",
  "action": "concrete next step",
  "confidence": 0.0-1.0,
  "category": "retention|anomaly|feature_correlation|overview",
  "estimated_impact": "business impact estimate",
  "related_metrics": ["metric1", "metric2"]
}}

Produce 3-7 cards ranked by severity × confidence. Return ONLY the JSON array."""


# ─── Registry ───

PROMPT_VERSIONS: dict[str, PromptVersion] = {
    "v1": PromptVersion(
        version="v1",
        system=_V1_SYSTEM,
        user_template=_V1_USER,
        description="Direct, opinionated analyst persona",
    ),
    "v2": PromptVersion(
        version="v2",
        system=_V2_SYSTEM,
        user_template=_V2_USER,
        description="Structured reasoning with explicit significance checks",
    ),
}

DEFAULT_PROMPT_VERSION = "v2"


class PromptRegistry:
    """Access versioned prompts."""

    @staticmethod
    def get(version: str) -> PromptVersion:
        if version not in PROMPT_VERSIONS:
            raise ValueError(
                f"Unknown prompt version '{version}'. "
                f"Available: {list(PROMPT_VERSIONS.keys())}"
            )
        return PROMPT_VERSIONS[version]

    @staticmethod
    def default() -> PromptVersion:
        return PROMPT_VERSIONS[DEFAULT_PROMPT_VERSION]

    @staticmethod
    def list_versions() -> list[str]:
        return list(PROMPT_VERSIONS.keys())
