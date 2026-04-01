"""
Quorum Insights — Digest Layer

Weekly/daily digest generation and delivery.

- composer: assembles stats + LLM insights into a ranked digest
- renderer: renders digest to HTML email, Slack blocks, or markdown
- scheduler: configurable schedule and recipient management
"""

from digest.composer import DigestComposer, Digest, DigestConfig
from digest.renderer import (
    render_markdown,
    render_html_email,
    render_slack_blocks,
)

__all__ = [
    "DigestComposer",
    "Digest",
    "DigestConfig",
    "render_markdown",
    "render_html_email",
    "render_slack_blocks",
]
