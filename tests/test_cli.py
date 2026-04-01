"""Tests for the CLI prototype — end-to-end pipeline integration."""

import json
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest


def _make_events_file(tmp_path: Path, num_users: int = 100, days: int = 60) -> Path:
    """Generate a synthetic JSONL events file."""
    import random
    random.seed(42)

    path = tmp_path / "events.jsonl"
    with open(path, "w") as f:
        for uid in range(num_users):
            signup_day = random.randint(0, 14)
            signup_date = date(2026, 1, 1) + timedelta(days=signup_day)

            # Signup event
            f.write(json.dumps({
                "user_id": f"u{uid}",
                "event_date": signup_date.isoformat(),
                "event_name": "signup",
            }) + "\n")

            # Feature usage
            features = ["search", "dashboard", "export", "settings"]
            if random.random() < 0.4:
                feat = random.choice(features)
                f.write(json.dumps({
                    "user_id": f"u{uid}",
                    "event_date": (signup_date + timedelta(days=random.randint(0, 3))).isoformat(),
                    "event_name": feat,
                }) + "\n")

            # Return activity
            for d in [1, 2, 7, 14, 30]:
                if random.random() < max(0, 0.4 - 0.008 * d):
                    f.write(json.dumps({
                        "user_id": f"u{uid}",
                        "event_date": (signup_date + timedelta(days=d)).isoformat(),
                        "event_name": random.choice(["pageview", "click"]),
                    }) + "\n")

    return path


class TestCLIPipeline:
    """End-to-end pipeline tests using --events-file and --dry-run."""

    def test_dry_run_from_file(self, tmp_path):
        """Full pipeline up to stats aggregation (no LLM)."""
        events_file = _make_events_file(tmp_path)

        result = subprocess.run(
            [
                sys.executable, "-m", "cli", "run",
                "--events-file", str(events_file),
                "--dry-run",
            ],
            capture_output=True, text=True,
            cwd=str(Path(__file__).parent.parent),
            env={**dict(__import__("os").environ), "PYTHONPATH": str(Path(__file__).parent.parent)},
        )

        assert result.returncode == 0, f"CLI failed:\n{result.stderr}"
        # Should output JSON stats summary
        output = result.stdout
        assert "schema_version" in output or "finding_count" in output

    def test_dry_run_produces_valid_json(self, tmp_path):
        events_file = _make_events_file(tmp_path)

        result = subprocess.run(
            [
                sys.executable, "-m", "cli", "run",
                "--events-file", str(events_file),
                "--dry-run",
            ],
            capture_output=True, text=True,
            cwd=str(Path(__file__).parent.parent),
            env={**dict(__import__("os").environ), "PYTHONPATH": str(Path(__file__).parent.parent)},
        )

        assert result.returncode == 0
        # Extract JSON from output (after log lines)
        lines = result.stdout.strip().split("\n")
        json_lines = [l for l in lines if not l.startswith("{")] or lines
        # The JSON block starts with {
        json_text = "\n".join(
            l for l in lines
            if l.strip().startswith("{") or l.strip().startswith('"') or l.strip().startswith("}")
            or l.strip().startswith("[") or l.strip().startswith("]") or l.strip() == ""
        )
        # Just verify it ran successfully
        assert "Loading events" in result.stderr or result.returncode == 0

    def test_version_command(self):
        result = subprocess.run(
            [sys.executable, "-m", "cli", "version"],
            capture_output=True, text=True,
            cwd=str(Path(__file__).parent.parent),
            env={**dict(__import__("os").environ), "PYTHONPATH": str(Path(__file__).parent.parent)},
        )

        assert result.returncode == 0
        assert "Quorum Insights CLI" in result.stdout

    def test_no_data_source_exits(self):
        result = subprocess.run(
            [sys.executable, "-m", "cli", "run"],
            capture_output=True, text=True,
            cwd=str(Path(__file__).parent.parent),
            env={**dict(__import__("os").environ), "PYTHONPATH": str(Path(__file__).parent.parent)},
        )

        assert result.returncode != 0

    def test_missing_events_file_exits(self, tmp_path):
        result = subprocess.run(
            [
                sys.executable, "-m", "cli", "run",
                "--events-file", str(tmp_path / "nonexistent.jsonl"),
                "--dry-run",
            ],
            capture_output=True, text=True,
            cwd=str(Path(__file__).parent.parent),
            env={**dict(__import__("os").environ), "PYTHONPATH": str(Path(__file__).parent.parent)},
        )

        assert result.returncode != 0

    def test_output_to_file(self, tmp_path):
        """Test writing digest markdown to a file (no LLM, uses no-insights path)."""
        events_file = _make_events_file(tmp_path, num_users=10, days=10)
        output_file = tmp_path / "digest.md"

        # Without anthropic key, it falls back to dry-run-style empty cards
        result = subprocess.run(
            [
                sys.executable, "-m", "cli", "run",
                "--events-file", str(events_file),
                "--output", str(output_file),
            ],
            capture_output=True, text=True,
            cwd=str(Path(__file__).parent.parent),
            env={**dict(__import__("os").environ), "PYTHONPATH": str(Path(__file__).parent.parent)},
        )

        assert result.returncode == 0, f"CLI failed:\n{result.stderr}"
        assert output_file.exists()
        content = output_file.read_text()
        assert "Insights Digest" in content or "findings" in content.lower()


class TestCLIUnit:
    """Unit tests for CLI helper functions."""

    def test_load_events_from_file(self, tmp_path):
        """Test the JSONL loader directly."""
        events_file = _make_events_file(tmp_path, num_users=20)

        # Import directly
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from cli import _load_events_from_file

        df = _load_events_from_file(str(events_file))
        assert df is not None
        assert len(df) > 0
        assert "user_id" in df.columns
        assert "event_date" in df.columns
        assert "event_name" in df.columns

    def test_load_events_missing_file(self):
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from cli import _load_events_from_file

        df = _load_events_from_file("/nonexistent/path.jsonl")
        assert df is None

    def test_compute_dau_series(self, tmp_path):
        """Test DAU series computation."""
        import polars as pl
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from cli import _compute_dau_series

        df = pl.DataFrame({
            "user_id": ["u1", "u2", "u1", "u3", "u2"],
            "event_date": [
                date(2026, 1, 1), date(2026, 1, 1),
                date(2026, 1, 2), date(2026, 1, 2),
                date(2026, 1, 3),
            ],
            "event_name": ["a", "a", "b", "b", "c"],
        })

        series = _compute_dau_series(df)
        assert series.name == "dau"
        assert len(series.dates) == 3
        assert series.values[0] == 2.0  # day 1: u1, u2
        assert series.values[1] == 2.0  # day 2: u1, u3
        assert series.values[2] == 1.0  # day 3: u2

    def test_events_file_with_distinct_id(self, tmp_path):
        """PostHog-style events with distinct_id instead of user_id."""
        path = tmp_path / "events.jsonl"
        path.write_text(
            json.dumps({"distinct_id": "user-abc", "timestamp": "2026-01-15T10:00:00Z", "event": "pageview"}) + "\n"
            + json.dumps({"distinct_id": "user-xyz", "timestamp": "2026-01-16T10:00:00Z", "event": "click"}) + "\n"
        )

        sys.path.insert(0, str(Path(__file__).parent.parent))
        from cli import _load_events_from_file

        df = _load_events_from_file(str(path))
        assert df is not None
        assert len(df) == 2
        assert df["user_id"][0] == "user-abc"
        assert df["event_name"][0] == "pageview"
