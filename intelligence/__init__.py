"""
Quorum Insights — Intelligence Layer

LLM-powered insight generation from statistical summaries.

- engine: InsightEngine orchestrates the generation pipeline
- prompts: versioned prompt templates
- models: InsightCard data model
- cache: content-addressable LLM response caching
"""

from intelligence.models import InsightCard, InsightSeverity
from intelligence.engine import InsightEngine, InsightEngineConfig
from intelligence.prompts import PromptRegistry, PROMPT_VERSIONS

__all__ = [
    "InsightCard",
    "InsightSeverity",
    "InsightEngine",
    "InsightEngineConfig",
    "PromptRegistry",
    "PROMPT_VERSIONS",
]
