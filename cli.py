"""
Quorum Insights CLI — standalone prototype.

Runs the full pipeline: PostHog → stats → LLM insights → digest.

Usage:
    # Generate insights from PostHog data (requires ANTHROPIC_API_KEY)
    python cli.py run --posthog-key phx_... --posthog-project 12345 --tenant my-app

    # Dry run (no LLM call, shows stats summary)
    python cli.py run --posthog-key phx_... --posthog-project 12345 --dry-run

    # From local event data (JSON lines file)
    python cli.py run --events-file events.jsonl --tenant my-app

    # Deliver digest to Slack
    python cli.py run --posthog-key phx_... --posthog-project 12345 \\
        --slack-webhook https://hooks.slack.com/services/T.../B.../xxx

Environment variables:
    POSTHOG_API_KEY       PostHog personal API key
    POSTHOG_PROJECT_ID    PostHog project ID
    ANTHROPIC_API_KEY     Anthropic API key (for LLM insights)
    SLACK_WEBHOOK_URL     Slack incoming webhook URL
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import polars as pl

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("insights.cli")


def main():
    parser = argparse.ArgumentParser(
        description="Quorum Insights — AI-powered product analytics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command")

    # ─── run command ───
    run_p = sub.add_parser("run", help="Run the full insights pipeline")
    run_p.add_argument("--tenant", default="default", help="Tenant ID (default: 'default')")

    # Data source (one of)
    src = run_p.add_argument_group("data source")
    src.add_argument("--posthog-key", default=os.environ.get("POSTHOG_API_KEY", ""),
                     help="PostHog API key (or POSTHOG_API_KEY env var)")
    src.add_argument("--posthog-project", default=os.environ.get("POSTHOG_PROJECT_ID", ""),
                     help="PostHog project ID (or POSTHOG_PROJECT_ID env var)")
    src.add_argument("--posthog-host", default="https://us.i.posthog.com",
                     help="PostHog host (default: us.i.posthog.com)")
    src.add_argument("--events-file", help="Path to JSON lines event file (alternative to PostHog)")
    src.add_argument("--lookback-days", type=int, default=90,
                     help="Days of history to fetch (default: 90)")

    # LLM
    llm = run_p.add_argument_group("LLM settings")
    llm.add_argument("--anthropic-key", default=os.environ.get("ANTHROPIC_API_KEY", ""),
                     help="Anthropic API key (or ANTHROPIC_API_KEY env var)")
    llm.add_argument("--model", default="claude-sonnet-4-20250514",
                     help="LLM model to use")
    llm.add_argument("--prompt-version", default="v2", choices=["v1", "v2"],
                     help="Prompt version (default: v2)")
    llm.add_argument("--dry-run", action="store_true",
                     help="Skip LLM call, show stats summary only")

    # Delivery
    dlv = run_p.add_argument_group("delivery")
    dlv.add_argument("--output", default="-",
                     help="Output file for markdown digest (default: stdout)")
    dlv.add_argument("--slack-webhook", default=os.environ.get("SLACK_WEBHOOK_URL", ""),
                     help="Slack incoming webhook URL")
    dlv.add_argument("--max-insights", type=int, default=3,
                     help="Max insights in digest (default: 3)")

    # ─── version command ───
    sub.add_parser("version", help="Show version info")

    args = parser.parse_args()

    if args.command == "version":
        _print_version()
    elif args.command == "run":
        asyncio.run(_run_pipeline(args))
    else:
        parser.print_help()


def _print_version():
    print("Quorum Insights CLI v0.1.0")
    print(f"Python {sys.version}")
    print("Modules: schema, connectors, query, stats, intelligence, digest")


async def _run_pipeline(args):
    """Run the full pipeline: data → stats → LLM → digest."""

    # ── Step 1: Load events ──
    logger.info("Step 1/5: Loading events...")
    events_df = await _load_events(args)

    if events_df is None or events_df.is_empty():
        logger.error("No events loaded. Check your data source configuration.")
        sys.exit(1)

    logger.info(
        "Loaded %d events, %d unique users, date range: %s to %s",
        len(events_df),
        events_df["user_id"].n_unique(),
        events_df["event_date"].min(),
        events_df["event_date"].max(),
    )

    # ── Step 2: Compute stats ──
    logger.info("Step 2/5: Computing retention curves...")
    from stats.retention import RetentionComputer
    retention_result = RetentionComputer(events_df).compute(periods=[1, 7, 30])

    logger.info("Step 2/5: Computing anomaly detection...")
    from stats.anomaly import AnomalyDetector, MetricSeries
    dau_series = _compute_dau_series(events_df)
    anomaly_result = AnomalyDetector(sigma_threshold=2.0, window=4).detect(dau_series)

    logger.info("Step 2/5: Computing feature correlations...")
    from stats.features import FeatureCorrelationAnalyzer
    feature_result = FeatureCorrelationAnalyzer(
        events_df, min_feature_users=5,
    ).analyze(retention_periods=[7, 30])

    # ── Step 3: Aggregate ──
    logger.info("Step 3/5: Aggregating stats summary...")
    from stats.aggregator import StatsAggregator
    summary = (
        StatsAggregator()
        .add_retention(retention_result.to_summary())
        .add_anomalies(anomaly_result.to_summary())
        .add_feature_correlation(feature_result.to_summary())
        .build()
    )
    summary_dict = summary.to_dict()

    logger.info(
        "Summary: %d findings (%d critical), %d users",
        summary.finding_count,
        summary.critical_count,
        summary.freshness.user_count,
    )

    if args.dry_run:
        logger.info("Dry run — printing stats summary (no LLM call)")
        print(json.dumps(summary_dict, indent=2, default=str))
        return

    # ── Step 4: Generate LLM insights ──
    logger.info("Step 4/5: Generating LLM insights...")
    insight_response = _generate_insights(args, summary_dict)

    logger.info("Generated %d insight cards", len(insight_response.cards))
    for card in insight_response.cards:
        logger.info("  [%s] %s", card.severity.value.upper(), card.title)

    # ── Step 5: Compose and deliver digest ──
    logger.info("Step 5/5: Composing and delivering digest...")
    _deliver_digest(args, insight_response, summary_dict)

    logger.info("Done!")


async def _load_events(args) -> Optional[pl.DataFrame]:
    """Load events from PostHog or local file."""

    if args.events_file:
        return _load_events_from_file(args.events_file)

    if args.posthog_key and args.posthog_project:
        return await _load_events_from_posthog(args)

    logger.error(
        "No data source configured. Use --posthog-key + --posthog-project "
        "or --events-file"
    )
    return None


def _load_events_from_file(path: str) -> Optional[pl.DataFrame]:
    """Load events from a JSON lines file."""
    filepath = Path(path)
    if not filepath.exists():
        logger.error("Events file not found: %s", path)
        return None

    rows = []
    for line in filepath.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
            rows.append({
                "user_id": event.get("user_id", event.get("distinct_id", "")),
                "event_date": date.fromisoformat(event.get("event_date", event.get("timestamp", "")[:10])),
                "event_name": event.get("event_name", event.get("event", "")),
            })
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning("Skipping malformed line: %s", e)

    if not rows:
        return None

    return pl.DataFrame(rows)


async def _load_events_from_posthog(args) -> Optional[pl.DataFrame]:
    """Load events from PostHog via the connector."""
    from connectors.posthog import PostHogConnector, PostHogConfig

    config = PostHogConfig(
        tenant_id=args.tenant,
        api_key=args.posthog_key,
        project_id=args.posthog_project,
        host=args.posthog_host,
        lookback_days=args.lookback_days,
    )
    connector = PostHogConnector(config)

    try:
        valid = await connector.validate()
        if not valid:
            return None

        all_events = []
        async for batch in connector.fetch_events():
            for event in batch:
                all_events.append({
                    "user_id": event.user_id or event.anonymous_id or "",
                    "event_date": event.timestamp.date() if hasattr(event.timestamp, 'date') else date.fromisoformat(str(event.timestamp)[:10]),
                    "event_name": event.event_name,
                })

        if not all_events:
            return None

        return pl.DataFrame(all_events)
    finally:
        await connector.close()


def _compute_dau_series(df: pl.DataFrame):
    """Compute DAU time series from events DataFrame."""
    from stats.anomaly import MetricSeries

    dau = (
        df.filter(pl.col("user_id") != "")
        .group_by("event_date")
        .agg(pl.col("user_id").n_unique().alias("dau"))
        .sort("event_date")
    )

    if dau.is_empty():
        return MetricSeries("dau", [date.today()], [0.0])

    return MetricSeries(
        name="dau",
        dates=dau["event_date"].to_list(),
        values=[float(v) for v in dau["dau"].to_list()],
    )


def _generate_insights(args, summary_dict: dict):
    """Generate LLM insights using the intelligence engine."""
    from intelligence.engine import InsightEngine, InsightEngineConfig

    if not args.anthropic_key:
        logger.warning(
            "No ANTHROPIC_API_KEY set. Using dry run mode. "
            "Set --anthropic-key or ANTHROPIC_API_KEY env var."
        )
        engine = InsightEngine(config=InsightEngineConfig(
            model=args.model,
            prompt_version=args.prompt_version,
            cache_enabled=True,
        ))
        return engine.generate(summary_dict, dry_run=True)

    import anthropic
    from intelligence.engine import InsightEngine, InsightEngineConfig, AnthropicClient

    client = AnthropicClient(anthropic.Anthropic(api_key=args.anthropic_key))
    config = InsightEngineConfig(
        model=args.model,
        prompt_version=args.prompt_version,
        cache_enabled=True,
    )
    engine = InsightEngine(client=client, config=config)
    return engine.generate(summary_dict)


def _deliver_digest(args, insight_response, summary_dict: dict):
    """Compose and deliver the digest."""
    from digest.composer import DigestComposer, DigestConfig
    from digest.renderer import render_markdown, render_slack_blocks

    config = DigestConfig(
        max_insights=args.max_insights,
        tenant_name=args.tenant if args.tenant != "default" else None,
    )
    composer = DigestComposer(config)
    digest = composer.compose(insight_response, summary_dict)

    # Markdown output
    md = render_markdown(digest)
    if args.output == "-":
        print("\n" + md)
    else:
        Path(args.output).write_text(md, encoding="utf-8")
        logger.info("Digest written to %s", args.output)

    # Slack delivery
    if args.slack_webhook:
        import httpx
        blocks = render_slack_blocks(digest)
        try:
            resp = httpx.post(args.slack_webhook, json={"blocks": blocks}, timeout=10)
            if resp.status_code == 200:
                logger.info("Digest delivered to Slack")
            else:
                logger.error("Slack delivery failed: %d %s", resp.status_code, resp.text[:200])
        except httpx.ConnectError as e:
            logger.error("Slack webhook connection failed: %s", e)


if __name__ == "__main__":
    main()
