# Quorum Insights: Product Spec
## Proactive Product Analytics Intelligence for AI-Powered Apps
## March 31, 2026 — Revised (standalone-first)

---

## One-Liner

An AI growth co-pilot that connects to your existing analytics tools, tells you what's driving your metrics, and recommends what to do next — starting with AI-powered apps where nobody else plays.

---

## The Problem

Two gaps exist in the analytics landscape:

### Gap 1: Insights are passive

PostHog, Amplitude, Mixpanel are rearview mirrors. They show you what happened. Growth teams drown in dashboards and still can't answer "what should we do this week?"

Amplitude shipped AI agents (Feb 2026) — but they only work inside Amplitude, and PostHog killed their AI assistant because users didn't adopt it. The conversational "ask your data" approach has trust issues. What teams actually want is a **proactive navigator**: "here's what matters, here's the proof, here's what to do."

### Gap 2: AI app analytics is a blind spot

Millions of developers are building AI-powered apps. They have tools for LLM observability (Langfuse, Helicone — traces, latency, costs) and tools for product analytics (PostHog, Amplitude — DAU, retention, funnels). But nobody connects them:

- Langfuse tells you: "Your p95 latency is 3.2s and you spent $847 on GPT-4 this week."
- PostHog tells you: "Your DAU is 1,200 and your D7 retention is 34%."

**Nobody tells you:**
- "Users who get >3 bad AI responses in their first session never come back"
- "Your AI chat feature has 12% higher retention than autocomplete — but you're investing more in autocomplete"
- "French users churn at 2x rate — it's a model quality issue, not a UX issue"
- "Users who discover the /explain command retain 3x better — promote it in onboarding"

---

## The Product

### What It Is

A proactive product analytics intelligence platform that:
1. **Connects** to your existing data (PostHog, Segment, Amplitude, warehouses, Langfuse)
2. **Computes** retention, activation, churn, feature impact — statistical pre-processing before any LLM touches it
3. **Surfaces** insights proactively (push to Slack, weekly digest, always-on agents) — not just when you ask
4. **Recommends** specific actions with estimated impact
5. **Shows evidence** — every insight backed by the chart that proves it

### What It Is Not

- Not a replacement for PostHog/Amplitude (they handle event collection, funnels, session replay)
- Not a text-to-SQL chat (Nao, ClickHouse Agentic Stack already do this)
- Not a dashboard builder
- Not an LLM observability tool (Langfuse handles that)

### The Wedge → Platform Strategy

```
Phase 1 (Launch): AI App Product Analytics — THE WEDGE
  "How does your AI affect your users?"
  Unique insight nobody else has. Greenfield category.
  Connects to: PostHog + Langfuse/Helicone traces
  Buyer: Product teams at AI-forward companies

Phase 2 (Month 6+): General Product Analytics Intelligence
  "What's driving your metrics — and what should you do about it?"
  The open-source Amplitude AI agents story.
  Connects to: PostHog, Segment, Amplitude, any warehouse
  Buyer: Any product/growth team

Phase 3 (With Accuracy): Quorum-Enhanced
  AI quality as a first-class analytical dimension.
  Connects to: Quorum Accuracy traces
  Unique: retention × agent accuracy matrix, accuracy-gated funnels
```

The architecture is generic from day 1. The positioning starts vertical (AI apps), expands horizontal.

---

## Architecture

```
[Data Sources]
  PostHog / Segment / Amplitude / BigQuery / Snowflake / ClickHouse / Postgres
  Langfuse / Helicone (LLM traces — for AI app wedge)
  Quorum Accuracy (when connected — bonus dimension)
              ↓
  [Connector + Normalization Layer]
  Maps source schemas → canonical event model
  Runs continuously (not one-time import)
              ↓
         [ClickHouse]
         Own data store. Fast aggregations.
              ↓
  [Statistical Pre-Processing Layer]   ← THE CORE IP
  Retention curves by cohort
  Activation event candidates
  Churn signal detection
  Feature impact correlation
  Anomaly detection (WoW changes)
  AI quality × user behavior joins (for AI apps)
              ↓
  [LLM Intelligence Layer]
  Receives structured statistical summaries (NOT raw data)
  Produces: insight cards, recommendations, draft actions
  Confidence scoring. Evidence-linked.
              ↓
  [Delivery]
  ├── Always-on agents (background monitoring, push to Slack)
  ├── Weekly/daily digest (email + Slack)
  ├── Evidence-based UI (insight cards with supporting charts)
  └── API (for Actions integration or custom workflows)
```

### Why This Architecture Wins

1. **Statistical pre-processing is the moat.** LLMs interpreting raw data produce generic insights. Computing retention curves, activation sequences, and churn correlations FIRST means the LLM receives rich, structured summaries that produce genuinely useful recommendations.

2. **Proactive agents differentiate from everything open-source.** Nao, Agentic Data Stack — all on-demand. Nobody has built always-on product analytics agents that monitor metrics and push insights.

3. **Product analytics semantic layer.** The system understands "retention," "activation," "churn," "cohort" as first-class concepts. Raw text-to-SQL doesn't.

---

## Canonical Event Schema

The normalization layer maps every source into a canonical model:

```json
{
  "user_id": "string",
  "anonymous_id": "string (optional)",
  "event_name": "string (original name preserved)",
  "event_type": "string (normalized category)",
  "timestamp": "ISO 8601",
  "session_id": "string (optional)",
  "properties": {},
  "user_properties": {
    "first_seen": "ISO 8601",
    "segment": "string",
    "plan": "string",
    "locale": "string",
    "country": "string"
  },
  "ai_context": {
    "model": "string (optional)",
    "quality_score": "float 0-1 (optional)",
    "feature": "string (optional — which AI feature)",
    "tokens_used": "int (optional)",
    "latency_ms": "int (optional)",
    "verification_result": "string (optional — from Accuracy)"
  }
}
```

`ai_context` is populated when LLM trace data is available (Langfuse, Helicone, or Quorum Accuracy). For non-AI apps, it's empty — the rest of the analytics still works.

---

## Core Views

### 1. Insight Cards (Primary Interface)

Not dashboards. Proactive insight cards with evidence:

```
┌─── 🔴 Critical ────────────────────────────────────────────┐
│ Users who skip onboarding step 3 churn at 3x the base rate │
│                                                             │
│ [Retention curve chart: step 3 completers vs skippers]      │
│                                                             │
│ 847 users skipped step 3 in the last 7 days.                │
│ Projected churns: 340 within 14 days.                       │
│ Projected revenue loss: $4,200/mo                           │
│                                                             │
│ Recommended: Email these users with step 3 completion CTA.  │
│ [Send to Actions →]  [View cohort →]  [Dismiss]  [Snooze]  │
└─────────────────────────────────────────────────────────────┘
```

### 2. Activation Moment Discovery

Auto-discovers which behavioral sequences predict long-term retention:

```
Activation Analysis (auto-discovered):

Users who [create a project] AND [invite a teammate]
within [first 24 hours] retain at [4.2x] the base rate.

Current state:
  - 61% of new signups never invite a teammate
  - Net activation rate: 24%

Recommended:
  - Prompt team invite during project creation flow
  - Projected activation rate: 41% (+71% improvement)
```

### 3. Feature Impact Ranking

Which features help vs. hurt retention:

```
Feature              Usage      Retention     Revenue
                     Impact     Impact        Impact
─────────────────────────────────────────────────────
AI Search            High       +3.2x         +$18/user    ✅
Recommendations      Medium     +0.8x         +$4/user
AI Chat Support      High       -1.4x         -$7/user     ⚠️ PROBLEM
Report Generator     Low        +1.1x         +$2/user
Data Analyzer        Very Low   -2.8x         -$12/user    🔴 KILL OR FIX
```

For AI apps with LLM trace data, this table also shows AI quality scores per feature — connecting model performance to user outcomes.

### 4. Churn Prediction

Pattern detection that predicts churn before it happens:

```
Churn Signals Detected:

Signal 1: Users whose weekly engagement drops >40% WoW
          churn within 14 days at 5.1x base rate.
          Currently affecting: 127 users (3.2% of active)

Signal 2: Users who encounter 3+ errors in one session
          never return.
          Last 7 days: 89 users hit this pattern.
```

### 5. Cohort Analysis

Segment users by behavior, not just demographics:

```
Cohort A: "Power users" — 3+ sessions/week, high feature adoption
  847 users, 78% D30 retention, $42/mo avg revenue, NPS 72

Cohort C: "At risk" — declining usage, low engagement
  1,112 users, 11% D30 retention, $12/mo avg revenue, NPS 18

Moving 500 users from C → B would generate $8K/mo additional revenue.
```

### 6. AI Quality × User Behavior (AI App Wedge)

The unique view nobody else can produce — only available when LLM trace data is connected:

```
AI Quality × Retention Matrix:

                    AI Feature Quality Score
                    Low (<70%)    Medium (70-90%)    High (>90%)
                 ┌─────────────┬────────────────┬──────────────┐
Retention        │             │                │              │
  D7             │    12%      │     38%        │     67%      │
  D30            │     3%      │     21%        │     52%      │
                 └─────────────┴────────────────┴──────────────┘

"Users who experience >90% AI quality retain at 5.6x the rate
of users who experience <70%. Your AI quality IS your retention."
```

### 7. Weekly Digest

Proactive — the insight comes to you:

```
📊 Weekly Insights — March 24-30

1. 🔴 Onboarding step 3 completion dropped 15% WoW.
   Contributing factors: new UI (60%), slower load time (25%), other (15%).
   Fix onboarding first. 847 users affected.

2. 🟡 AI Search quality improved 8% after model update.
   D7 retention for AI Search users up from 45% → 53%.

3. 🟢 New activation moment discovered: users who use Search AND
   Chat in same session retain 5.1x. Only 8% do this naturally.
   Recommend: cross-feature discovery in onboarding.
```

---

## Connectors

### Phase 1 (Launch)
| Source | Type | Priority |
|--------|------|----------|
| PostHog | Product analytics | P1 — our own dogfooding |
| Langfuse | LLM traces | P1 — AI app wedge |
| Segment | Event stream | P1 — covers ~70% of B2B SaaS |

### Phase 2 (Month 3+)
| Source | Type | Priority |
|--------|------|----------|
| Amplitude | Product analytics | P1 |
| Helicone | LLM traces | P2 |
| BigQuery / Snowflake | Warehouse | P2 |
| ClickHouse direct | Warehouse | P2 |
| Postgres | Database | P2 |

### Phase 3 (With Accuracy)
| Source | Type | Priority |
|--------|------|----------|
| Quorum Accuracy traces | OTel with dar.* attributes | P1 |

When connected, AI quality becomes a first-class dimension in all views — retention × accuracy matrix, accuracy-gated funnels, locale quality maps, feature-level AI performance.

---

## Tech Stack

| Component | Technology | Why |
|-----------|-----------|-----|
| Data store | ClickHouse | Purpose-built for event analytics. What PostHog uses. |
| Stats layer | Python (polars, scipy) | Best ML/stats ecosystem. Pre-processes before LLM. |
| LLM layer | Claude API | Best analytical reasoning + structured output |
| Backend API | Python (FastAPI) | Same language as stats layer. Simple. |
| Frontend | React + TypeScript | Standard. Recharts/Tremor for viz components. |
| Connectors | Python | Async ingestion pipelines (httpx + asyncio) |
| App metadata | Postgres | User accounts, insight history, connector configs |
| Caching | Redis | LLM response caching, rate limiting |
| Digest delivery | Sendgrid (email) + Slack API | Push delivery |

### NOT using (per tech-choices.md)
- Node.js/TypeScript for backend
- MongoDB, Elasticsearch
- pandas (use polars)
- requests (use httpx)
- Kafka (use async Python pipelines)

---

## Open Source Strategy

| Open Source (MIT) | Commercial (Insights Cloud) |
|-------------------|---------------------------|
| Statistical pre-processing engine | Managed hosting |
| Connector framework + PostHog connector | Premium connectors (Amplitude, Salesforce) |
| LLM insight generation engine | Fleet benchmarking (cross-customer patterns) |
| Insight card UI components | Advanced ML models (activation discovery, churn prediction) |
| Weekly digest (email + Slack) | Always-on background agents |
| CLI for local analysis | Enterprise features (SSO, SOC 2) |
| Canonical event schema | Historical trend analysis |

A product team can run Insights forever for free: connect PostHog, get weekly digests, see insight cards. Commercial version adds managed hosting, premium connectors, and advanced ML.

---

## Pricing

| Tier | Price | Includes |
|------|-------|---------|
| **Free (OSS)** | $0 | Self-hosted. PostHog + Langfuse connectors. Basic insights + digest. |
| **Starter** | $300/mo | Managed hosting. Up to 50K events/day. 3 insight cards/week. |
| **Pro** | $1,500/mo | Unlimited events. All connectors. Activation/churn models. Daily digests. Always-on agents. |
| **Enterprise** | $5,000-15,000/mo | Fleet analytics. Custom models. SLA. Warehouse connectors. |

---

## Build Plan

### Phase 1: MVP (Week 2-6) — Dogfood on Own PostHog

| Week | What | Task ID |
|------|------|---------|
| 2 | Canonical event schema (JSON schema + PostHog mapping) | dar-9x5 |
| 2-3 | PostHog connector | dar-qsu |
| 3-4 | ClickHouse data model + materialized views | dar-g2c |
| 3-4 | Statistical pre-processing layer | dar-neg |
| 4-5 | LLM insight generation engine | dar-bna |
| 5-6 | Weekly digest (email + Slack) | dar-s6d |
| 5-6 | Standalone prototype (integrated) | dar-tyv |

### Phase 2: Beta → GA (Week 7-12)

| Week | What | Task ID |
|------|------|---------|
| 7-8 | Insights UI (cards, feature ranking, cohorts) | dar-ff2 |
| 7-8 | Activation moment discovery | dar-1t5 |
| 9-10 | Churn prediction | dar-ztx |
| 9-10 | Docker Compose deployment | dar-2dm |
| 10-11 | Segment connector | dar-9t8 |
| 11-12 | Langfuse connector (AI app wedge) | NEW TASK |

---

## Success Metrics

| Metric | Week 6 (MVP) | Week 12 (GA) | Month 6 |
|--------|-------------|-------------|---------|
| Founder uses Insights daily | Yes | — | — |
| Weekly digest open rate | 60%+ | 55%+ | 50%+ |
| "Insight was actionable" rate | >40% | >60% | >70% |
| Beta teams | — | 5-10 | 30+ |
| First paying customer | — | Yes | 15+ |
| Insights MRR | — | $10-50K | $150K+ |

### One Metric That Matters

**Week 1-6:** "Founder uses Insights daily — it's genuinely useful on our own PostHog data."

**Week 7-12:** "5+ teams find an insight they wouldn't have found in PostHog alone."

**Month 6+:** "Paying customer count."

---

## Competitive Position

| Player | What They Do | Our Differentiation |
|--------|-------------|-------------------|
| **Amplitude AI Agents** | AI analysis inside Amplitude (free) | We work with ANY data source. Proactive agents, not just chat. |
| **PostHog LLM Analytics** | Trace grouping for AI apps (very early) | Full product analytics intelligence, not just trace views. |
| **Langfuse** | LLM observability (engineering) | We're the growth/product layer on top. Complementary. |
| **Nao** (OSS, 755⭐) | Text-to-SQL analytics agent | We understand product concepts natively. Proactive, not on-demand. |
| **ClickHouse Agentic Stack** | General-purpose AI data chat | No product analytics semantic layer. No proactive agents. |
| **QuadSci** ($8M) | Predictive customer intelligence | Proprietary, enterprise-only. We're open source. |

**Positioning:** "Your data already lives in PostHog. Your LLM traces are in Langfuse. Quorum Insights connects both and tells you what's actually driving your metrics — proactively, with evidence, and with recommended actions."
