"""
Content-addressable LLM response cache.

Identical statistical inputs → cached LLM response.
Uses SHA-256 of (prompt_version + stats_summary) as cache key.
File-based for simplicity; swap for Redis in production.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class CacheEntry:
    """A cached LLM response."""

    key: str
    response_json: str
    prompt_version: str
    model: str
    created_at: str


class InsightCache:
    """File-based content-addressable cache for LLM responses."""

    def __init__(self, cache_dir: Path | str | None = None):
        if cache_dir is None:
            self._dir = Path.home() / ".cache" / "quorum-insights"
        else:
            self._dir = Path(cache_dir)
        self._dir.mkdir(parents=True, exist_ok=True)

    def cache_key(self, prompt_version: str, stats_summary: str) -> str:
        """SHA-256 of prompt version + normalized summary."""
        content = f"{prompt_version}:{stats_summary}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def get(self, key: str) -> Optional[str]:
        """Get cached response JSON, or None if miss."""
        path = self._dir / f"{key}.json"
        if path.exists():
            try:
                data = json.loads(path.read_text())
                return data.get("response_json")
            except (json.JSONDecodeError, KeyError):
                return None
        return None

    def put(self, key: str, response_json: str, prompt_version: str, model: str) -> None:
        """Store a response in the cache."""
        from datetime import datetime, timezone

        entry = {
            "key": key,
            "response_json": response_json,
            "prompt_version": prompt_version,
            "model": model,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        path = self._dir / f"{key}.json"
        path.write_text(json.dumps(entry, indent=2))

    def clear(self) -> int:
        """Clear all cache entries. Returns count of entries removed."""
        count = 0
        for f in self._dir.glob("*.json"):
            f.unlink()
            count += 1
        return count

    @property
    def size(self) -> int:
        """Number of cached entries."""
        return len(list(self._dir.glob("*.json")))
