# Quorum Insights — Quickstart

Get AI-powered product analytics from your PostHog data in 5 minutes.

## Prerequisites

- Docker and Docker Compose
- A PostHog account with a [Personal API key](https://posthog.com/docs/api#personal-api-keys) (scope: `query:read`)
- An [Anthropic API key](https://console.anthropic.com/) (for AI insights)

## 1. Clone and configure

```bash
git clone https://github.com/quorumhq/quorum-insights.git
cd quorum-insights

cp .env.example .env
```

Edit `.env` with your keys:

```env
POSTHOG_API_KEY=phx_your_key_here
POSTHOG_PROJECT_ID=12345
ANTHROPIC_API_KEY=sk-ant-your_key_here
```

## 2. Run

```bash
docker compose up
```

This will:
1. Start ClickHouse (analytics database)
2. Initialize the schema (tables + materialized views)
3. Sync events from your PostHog instance (last 90 days)
4. Compute retention curves, anomaly detection, and feature correlations
5. Generate AI insight cards via Claude
6. Write the digest to `insights_data` volume

The first run takes 1-5 minutes depending on your event volume.

## 3. Read your insights

```bash
# View the generated digest
docker compose exec insights cat /data/digest.md

# Or run interactively
docker compose run --rm insights run \
  --posthog-key=$POSTHOG_API_KEY \
  --posthog-project=$POSTHOG_PROJECT_ID \
  --anthropic-key=$ANTHROPIC_API_KEY
```

## 4. Deliver to Slack (optional)

Add your Slack webhook to `.env`:

```env
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T.../B.../xxx
```

Then add `--slack-webhook=${SLACK_WEBHOOK_URL}` to the insights command in `docker-compose.yml`.

## Without Docker

Run directly with Python 3.10+:

```bash
pip install -r requirements.txt

# From a PostHog instance
PYTHONPATH=. python cli.py run \
  --posthog-key phx_... \
  --posthog-project 12345 \
  --anthropic-key sk-ant-...

# From a local events file (JSON lines)
PYTHONPATH=. python cli.py run \
  --events-file events.jsonl \
  --anthropic-key sk-ant-...

# Dry run (no LLM call, shows stats summary)
PYTHONPATH=. python cli.py run \
  --events-file events.jsonl \
  --dry-run
```

## Event file format

For `--events-file`, use JSON lines with these fields:

```json
{"user_id": "user-123", "event_date": "2026-01-15", "event_name": "signup"}
{"user_id": "user-123", "event_date": "2026-01-16", "event_name": "search"}
{"user_id": "user-456", "event_date": "2026-01-15", "event_name": "signup"}
```

PostHog-style events also work:

```json
{"distinct_id": "user-123", "timestamp": "2026-01-15T10:00:00Z", "event": "signup"}
```

## What you get

The digest includes your top 3 insights ranked by severity × confidence:

- **Anomaly detection**: DAU/session spikes and drops (>2σ)
- **Retention analysis**: D1/D7/D30 cohort retention curves
- **Feature correlations**: which features are associated with better/worse retention

Each insight card includes:
- What the data shows (with specific numbers)
- Supporting evidence
- A concrete recommended action
- Estimated business impact

## Configuration reference

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTHOG_API_KEY` | (required) | PostHog personal API key |
| `POSTHOG_PROJECT_ID` | (required) | PostHog project ID |
| `POSTHOG_HOST` | `https://us.i.posthog.com` | PostHog instance URL |
| `ANTHROPIC_API_KEY` | (required for insights) | Anthropic API key |
| `SLACK_WEBHOOK_URL` | (optional) | Slack incoming webhook |
| `LOOKBACK_DAYS` | `90` | Days of history to sync |
| `MAX_INSIGHTS` | `3` | Max insights per digest |
| `LLM_MODEL` | `claude-sonnet-4-20250514` | Claude model to use |
| `PROMPT_VERSION` | `v2` | Prompt version (v1 or v2) |
| `TENANT_ID` | `default` | Tenant identifier |

## Resource usage

- **ClickHouse**: ~512MB RAM, ~1-5GB disk per 1M events
- **Insights CLI**: ~256MB RAM during computation
- **Total**: <2GB RAM for typical deployments
