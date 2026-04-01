"""
InsightEngine — orchestrates LLM insight generation.

Flow:
1. Receives StatsSummary from aggregator
2. Checks cache for identical inputs
3. Renders versioned prompt with stats context
4. Calls LLM (Anthropic Claude) with structured output
5. Parses response into InsightCard objects
6. Caches result for future identical inputs

The engine does NOT analyze raw data. It interprets pre-computed
statistical summaries and produces specific, opinionated recommendations.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Protocol

from intelligence.cache import InsightCache
from intelligence.models import InsightCard, InsightResponse, InsightSeverity
from intelligence.prompts import DEFAULT_PROMPT_VERSION, PromptRegistry

logger = logging.getLogger(__name__)


class LLMClient(Protocol):
    """Protocol for LLM API clients. Implement this to swap providers."""

    def generate(
        self, system: str, user: str, model: str
    ) -> tuple[str, dict]:
        """Generate a response. Returns (text, usage_dict)."""
        ...


@dataclass
class InsightEngineConfig:
    """Configuration for the insight engine."""

    model: str = "claude-sonnet-4-20250514"
    prompt_version: str = DEFAULT_PROMPT_VERSION
    cache_dir: Optional[str] = None
    cache_enabled: bool = True
    max_cards: int = 10
    min_confidence: float = 0.3  # skip low-confidence findings


class InsightEngine:
    """Orchestrates LLM-powered insight generation.

    Usage:
        engine = InsightEngine(client=anthropic_client)
        response = engine.generate(stats_summary_dict)
        for card in response.cards:
            print(card.title, card.severity)

    Or without a real LLM client (for testing):
        engine = InsightEngine()  # uses no-op client
        response = engine.generate(summary, dry_run=True)
    """

    def __init__(
        self,
        client: Optional[LLMClient] = None,
        config: Optional[InsightEngineConfig] = None,
    ):
        self.config = config or InsightEngineConfig()
        self._client = client
        self._cache = InsightCache(self.config.cache_dir) if self.config.cache_enabled else None
        self._prompt = PromptRegistry.get(self.config.prompt_version)

    def generate(
        self,
        stats_summary: dict,
        dry_run: bool = False,
    ) -> InsightResponse:
        """Generate insight cards from a stats summary.

        Args:
            stats_summary: Output from StatsAggregator.build().to_dict()
            dry_run: If True, return the prompt without calling the LLM
        """
        # Serialize summary for prompt + cache key
        summary_text = json.dumps(stats_summary, indent=2, default=str)

        # Check cache (key includes prompt version + model + summary)
        cache_key = None
        if self._cache:
            cache_key = self._cache.cache_key(
                self.config.prompt_version, summary_text, self.config.model,
            )
            cached = self._cache.get(cache_key)
            if cached:
                logger.info("Cache hit for key %s", cache_key)
                cards = self._parse_cards(cached)
                return InsightResponse(
                    cards=cards,
                    prompt_version=self.config.prompt_version,
                    model=self.config.model,
                    cached=True,
                )

        # Render prompt
        system = self._prompt.system
        user = self._prompt.render_user(summary_text)

        if dry_run:
            return InsightResponse(
                cards=[],
                prompt_version=self.config.prompt_version,
                model=self.config.model,
                token_usage={"input": len(system) + len(user), "output": 0},
            )

        if self._client is None:
            raise RuntimeError(
                "No LLM client configured. Pass client= to InsightEngine() "
                "or use dry_run=True."
            )

        # Call LLM
        response_text, usage = self._client.generate(
            system=system,
            user=user,
            model=self.config.model,
        )

        # Parse cards
        cards = self._parse_cards(response_text)

        # Filter by confidence
        cards = [c for c in cards if c.confidence >= self.config.min_confidence]

        # Limit
        cards = cards[: self.config.max_cards]

        # Cache
        if self._cache and cache_key:
            self._cache.put(
                cache_key, response_text,
                self.config.prompt_version, self.config.model,
            )

        return InsightResponse(
            cards=cards,
            prompt_version=self.config.prompt_version,
            model=self.config.model,
            token_usage=usage,
        )

    def _parse_cards(self, response_text: str) -> list[InsightCard]:
        """Parse LLM response into InsightCard objects.

        Handles:
        - Clean JSON array
        - JSON wrapped in markdown code blocks
        - Partial/malformed JSON (returns empty list)
        """
        text = response_text.strip()

        # Strip markdown code block if present
        if text.startswith("```"):
            lines = text.split("\n")
            # Remove first and last lines (```json and ```)
            lines = [l for l in lines if not l.strip().startswith("```")]
            text = "\n".join(lines).strip()

        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            logger.warning("Failed to parse LLM response as JSON: %s...", text[:200])
            return []

        if not isinstance(data, list):
            logger.warning("LLM response is not a JSON array")
            return []

        cards = []
        for item in data:
            try:
                card = InsightCard.from_dict(item)
                cards.append(card)
            except (KeyError, ValueError) as e:
                logger.warning("Skipping malformed card: %s — %s", e, item)

        return cards


# JSON Schema for InsightCard (used by Claude tool_use)
INSIGHT_CARDS_TOOL = {
    "name": "submit_insight_cards",
    "description": (
        "Submit a ranked list of product analytics insight cards. "
        "Each card must have specific, data-grounded findings."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "cards": {
                "type": "array",
                "description": "3-7 insight cards ranked by importance",
                "items": {
                    "type": "object",
                    "properties": {
                        "title": {
                            "type": "string",
                            "description": "Concise title under 80 chars",
                        },
                        "severity": {
                            "type": "string",
                            "enum": ["critical", "high", "medium", "low", "info"],
                        },
                        "finding": {
                            "type": "string",
                            "description": "What the data shows, 2-3 sentences with numbers",
                        },
                        "evidence": {
                            "type": "string",
                            "description": "Which specific metrics support this",
                        },
                        "action": {
                            "type": "string",
                            "description": "Specific next step, not generic advice",
                        },
                        "confidence": {
                            "type": "number",
                            "minimum": 0,
                            "maximum": 1,
                            "description": "Statistical confidence 0-1",
                        },
                        "category": {
                            "type": "string",
                            "enum": ["retention", "anomaly", "feature_correlation", "overview"],
                        },
                        "estimated_impact": {
                            "type": "string",
                            "description": "Estimated business impact of the action",
                        },
                        "related_metrics": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Metric names involved",
                        },
                    },
                    "required": [
                        "title", "severity", "finding", "evidence",
                        "action", "confidence", "category",
                    ],
                },
            },
        },
        "required": ["cards"],
    },
}


class AnthropicClient:
    """LLM client using Claude tool_use for guaranteed structured output.

    Uses tool_choice to force Claude to call submit_insight_cards,
    which returns schema-validated JSON. This is Anthropic's recommended
    approach for structured output (not raw JSON prompting).

    Usage:
        import anthropic
        client = AnthropicClient(anthropic.Anthropic())
        engine = InsightEngine(client=client)
    """

    def __init__(self, client):
        """Args: client — an anthropic.Anthropic() instance."""
        self._client = client

    def generate(
        self, system: str, user: str, model: str
    ) -> tuple[str, dict]:
        response = self._client.messages.create(
            model=model,
            max_tokens=4096,
            system=system,
            messages=[{"role": "user", "content": user}],
            tools=[INSIGHT_CARDS_TOOL],
            tool_choice={"type": "tool", "name": "submit_insight_cards"},
        )

        usage = {
            "input": response.usage.input_tokens,
            "output": response.usage.output_tokens,
        }

        # Extract structured data from tool_use block
        for block in response.content:
            if block.type == "tool_use" and block.name == "submit_insight_cards":
                # block.input is already a parsed dict from the API
                cards_data = block.input.get("cards", [])
                return json.dumps(cards_data), usage

        # Fallback: no tool_use block found (shouldn't happen with tool_choice)
        logger.warning("No tool_use block in Claude response, falling back to text")
        text = next(
            (b.text for b in response.content if hasattr(b, "text")),
            "[]",
        )
        return text, usage
