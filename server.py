"""
Quorum Insights — FastAPI server.

Thin wrapper around the Python stats modules. Loads events from a JSONL
file or PostHog, runs stats computations, and serves JSON API endpoints
for the React dashboard.

Usage:
    # Development
    PYTHONPATH=. uvicorn server:app --reload --port 8080

    # With data
    EVENTS_FILE=events.jsonl uvicorn server:app --port 8080

    # With PostHog
    POSTHOG_API_KEY=phx_... POSTHOG_PROJECT_ID=12345 uvicorn server:app --port 8080

All endpoints accept:
    ?tenant_id=...  (default: "default")
    ?start_date=...&end_date=...  (ISO format, optional)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Optional

import polars as pl
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("insights.server")

app = FastAPI(
    title="Quorum Insights API",
    version="0.1.0",
    description="AI-powered product analytics API",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Data Loading ───


class DataStore:
    """Lazy-loaded, cached event data."""

    def __init__(self):
        self._df: Optional[pl.DataFrame] = None
        self._loaded_at: float = 0
        self._ttl = 300  # reload every 5 minutes

    def get_events(self) -> pl.DataFrame:
        now = time.time()
        if self._df is not None and (now - self._loaded_at) < self._ttl:
            return self._df

        self._df = self._load()
        self._loaded_at = now
        return self._df

    def _load(self) -> pl.DataFrame:
        events_file = os.environ.get("EVENTS_FILE")
        if events_file and Path(events_file).exists():
            logger.info("Loading events from %s", events_file)
            if events_file.endswith(".csv"):
                return self._load_from_csv(events_file)
            return self._load_from_jsonl(events_file)

        logger.warning("No EVENTS_FILE set. Using empty dataset. Set EVENTS_FILE=path/to/events.jsonl")
        return self._empty()

    @staticmethod
    def _empty() -> pl.DataFrame:
        return pl.DataFrame({
            "user_id": pl.Series([], dtype=pl.Utf8),
            "event_date": pl.Series([], dtype=pl.Date),
            "event_name": pl.Series([], dtype=pl.Utf8),
        })

    def _load_from_csv(self, path: str) -> pl.DataFrame:
        """Load from REES46/ecommerce CSV (or any CSV with event_time, event_type, user_id).

        For large datasets (>10M rows), samples to MAX_USERS to keep
        stats computation interactive (<15s per endpoint).
        """
        max_users = int(os.environ.get("MAX_USERS", "200000"))

        try:
            df = pl.read_csv(
                path,
                columns=["event_time", "event_type", "user_id", "category_code"],
                dtypes={"user_id": pl.Utf8, "category_code": pl.Utf8},
                n_rows=None,
                ignore_errors=True,
            )
            # Parse event_time → date
            df = df.with_columns(
                pl.col("event_time").str.slice(0, 10).str.to_date().alias("event_date"),
                pl.col("event_type").alias("event_name"),
            ).select("user_id", "event_date", "event_name")

            n_users = df["user_id"].n_unique()
            if n_users > max_users:
                logger.info(
                    "Sampling %d users from %d (set MAX_USERS to increase)",
                    max_users, n_users,
                )
                sampled_ids = (
                    df.select("user_id").unique()
                    .sample(n=max_users, seed=42)
                )
                df = df.join(sampled_ids, on="user_id", how="semi")

            logger.info("Loaded %d events, %d users from CSV", len(df), df["user_id"].n_unique())
            return df
        except Exception as e:
            logger.error("Failed to load CSV: %s", e)
            return self._empty()

    def _load_from_jsonl(self, path: str) -> pl.DataFrame:
        rows = []
        for line in Path(path).read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
                rows.append({
                    "user_id": event.get("user_id", event.get("distinct_id", "")),
                    "event_date": date.fromisoformat(
                        event.get("event_date", event.get("timestamp", "")[:10])
                    ),
                    "event_name": event.get("event_name", event.get("event", "")),
                })
            except (json.JSONDecodeError, ValueError):
                continue

        if not rows:
            return self._empty()
        return pl.DataFrame(rows)


_store = DataStore()


# ─── Stats Cache ───


class StatsCache:
    """In-memory cache for computed stats. TTL-based invalidation."""

    def __init__(self, ttl: int = 300):
        self._cache: dict[str, tuple[float, dict]] = {}
        self._ttl = ttl

    def get(self, key: str) -> Optional[dict]:
        if key in self._cache:
            ts, data = self._cache[key]
            if time.time() - ts < self._ttl:
                return data
            del self._cache[key]
        return None

    def put(self, key: str, data: dict):
        self._cache[key] = (time.time(), data)


_cache = StatsCache(ttl=300)


def _date_params(
    start_date: Optional[str],
    end_date: Optional[str],
    df: pl.DataFrame,
) -> tuple[date, date]:
    """Parse date range from query params, or use data range."""
    if start_date:
        sd = date.fromisoformat(start_date)
    elif not df.is_empty():
        sd = df["event_date"].min()
    else:
        sd = date.today() - timedelta(days=90)

    if end_date:
        ed = date.fromisoformat(end_date)
    elif not df.is_empty():
        ed = df["event_date"].max()
    else:
        ed = date.today()

    return sd, ed


# ─── API Endpoints ───


@app.get("/api/overview")
def get_overview(
    tenant_id: str = Query("default"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """High-level KPIs: DAU, total users, total events, anomaly count."""
    cache_key = f"overview:{tenant_id}:{start_date}:{end_date}"
    cached = _cache.get(cache_key)
    if cached:
        return cached

    df = _store.get_events()
    sd, ed = _date_params(start_date, end_date, df)
    filtered = df.filter(
        (pl.col("event_date") >= sd) & (pl.col("event_date") <= ed)
    )

    total_events = len(filtered)
    total_users = filtered["user_id"].n_unique() if not filtered.is_empty() else 0

    # DAU series
    dau = (
        filtered.filter(pl.col("user_id") != "")
        .group_by("event_date")
        .agg(pl.col("user_id").n_unique().alias("dau"))
        .sort("event_date")
    )

    dau_series = [
        {"date": row["event_date"].isoformat(), "dau": row["dau"]}
        for row in dau.iter_rows(named=True)
    ] if not dau.is_empty() else []

    # Quick anomaly check
    from stats.anomaly import AnomalyDetector, MetricSeries
    if len(dau_series) >= 4:
        series = MetricSeries(
            "dau",
            [date.fromisoformat(d["date"]) for d in dau_series],
            [float(d["dau"]) for d in dau_series],
        )
        anomaly_result = AnomalyDetector(sigma_threshold=2.0, window=4).detect(series)
        anomaly_count = anomaly_result.anomaly_count
    else:
        anomaly_count = 0

    result = {
        "total_events": total_events,
        "total_users": total_users,
        "date_range": {"start": sd.isoformat(), "end": ed.isoformat()},
        "dau_series": dau_series,
        "anomaly_count": anomaly_count,
        "avg_dau": round(sum(d["dau"] for d in dau_series) / max(len(dau_series), 1), 1),
    }
    _cache.put(cache_key, result)
    return result


@app.get("/api/retention")
def get_retention(
    tenant_id: str = Query("default"),
    periods: str = Query("1,7,30"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Retention curves by cohort."""
    cache_key = f"retention:{tenant_id}:{periods}:{start_date}:{end_date}"
    cached = _cache.get(cache_key)
    if cached:
        return cached

    df = _store.get_events()
    sd, ed = _date_params(start_date, end_date, df)
    filtered = df.filter(
        (pl.col("event_date") >= sd) & (pl.col("event_date") <= ed)
    )

    period_list = [int(p.strip()) for p in periods.split(",")]

    from stats.retention import RetentionComputer
    result = RetentionComputer(filtered).compute(periods=period_list)
    summary = result.to_summary()
    _cache.put(cache_key, summary)
    return summary


@app.get("/api/features")
def get_features(
    tenant_id: str = Query("default"),
    periods: str = Query("7,30"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Feature correlation ranking."""
    cache_key = f"features:{tenant_id}:{periods}:{start_date}:{end_date}"
    cached = _cache.get(cache_key)
    if cached:
        return cached

    df = _store.get_events()
    sd, ed = _date_params(start_date, end_date, df)
    filtered = df.filter(
        (pl.col("event_date") >= sd) & (pl.col("event_date") <= ed)
    )

    period_list = [int(p.strip()) for p in periods.split(",")]

    from stats.features import FeatureCorrelationAnalyzer
    result = FeatureCorrelationAnalyzer(filtered, min_feature_users=5).analyze(
        retention_periods=period_list
    )
    summary = result.to_summary()
    _cache.put(cache_key, summary)
    return summary


@app.get("/api/insights")
def get_insights(
    tenant_id: str = Query("default"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """LLM-generated insight cards (from aggregated stats)."""
    cache_key = f"insights:{tenant_id}:{start_date}:{end_date}"
    cached = _cache.get(cache_key)
    if cached:
        return cached

    df = _store.get_events()
    sd, ed = _date_params(start_date, end_date, df)
    filtered = df.filter(
        (pl.col("event_date") >= sd) & (pl.col("event_date") <= ed)
    )

    # Run all stats
    from stats.retention import RetentionComputer
    from stats.anomaly import AnomalyDetector, MetricSeries
    from stats.features import FeatureCorrelationAnalyzer
    from stats.aggregator import StatsAggregator

    ret = RetentionComputer(filtered).compute(periods=[1, 7, 30])

    # DAU for anomaly
    dau = (
        filtered.filter(pl.col("user_id") != "")
        .group_by("event_date")
        .agg(pl.col("user_id").n_unique().alias("dau"))
        .sort("event_date")
    )
    if not dau.is_empty() and len(dau) >= 4:
        series = MetricSeries(
            "dau",
            dau["event_date"].to_list(),
            [float(v) for v in dau["dau"].to_list()],
        )
        anom = AnomalyDetector(sigma_threshold=2.0, window=4).detect(series)
    else:
        anom = AnomalyDetector().detect(MetricSeries("dau", [date.today()], [0.0]))

    feat = FeatureCorrelationAnalyzer(filtered, min_feature_users=5).analyze(
        retention_periods=[7, 30]
    )

    summary = (
        StatsAggregator()
        .add_retention(ret.to_summary())
        .add_anomalies(anom.to_summary())
        .add_feature_correlation(feat.to_summary())
        .build()
    )

    # Try LLM if key available
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY")
    if anthropic_key:
        try:
            import anthropic
            from intelligence.engine import InsightEngine, InsightEngineConfig, AnthropicClient

            client = AnthropicClient(anthropic.Anthropic(api_key=anthropic_key))
            engine = InsightEngine(
                client=client,
                config=InsightEngineConfig(cache_enabled=True),
            )
            response = engine.generate(summary.to_dict())
            result = response.to_dict()
        except Exception as e:
            logger.warning("LLM insight generation failed: %s", e)
            result = {"cards": [], "error": str(e), "stats_summary": summary.to_dict()}
    else:
        result = {
            "cards": [],
            "note": "Set ANTHROPIC_API_KEY for AI-generated insights",
            "stats_summary": summary.to_dict(),
        }

    _cache.put(cache_key, result)
    return result


@app.get("/api/churn")
def get_churn(
    tenant_id: str = Query("default"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Churn risk analysis: at-risk users, decay stages, cohort alerts."""
    cache_key = f"churn:{tenant_id}:{start_date}:{end_date}"
    cached = _cache.get(cache_key)
    if cached:
        return cached

    df = _store.get_events()
    sd, ed = _date_params(start_date, end_date, df)

    from stats.churn import ChurnDetector
    result = ChurnDetector(
        df.filter((pl.col("event_date") >= sd) & (pl.col("event_date") <= ed)),
        analysis_date=ed,
    ).analyze()
    summary = result.to_summary()
    _cache.put(cache_key, summary)
    return summary


@app.get("/api/activation")
def get_activation(
    tenant_id: str = Query("default"),
    window: int = Query(7),
    retention_period: int = Query(30),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Activation moment discovery."""
    cache_key = f"activation:{tenant_id}:{window}:{retention_period}:{start_date}:{end_date}"
    cached = _cache.get(cache_key)
    if cached:
        return cached

    df = _store.get_events()
    sd, ed = _date_params(start_date, end_date, df)
    filtered = df.filter(
        (pl.col("event_date") >= sd) & (pl.col("event_date") <= ed)
    )

    from stats.activation import ActivationDiscovery
    result = ActivationDiscovery(filtered, min_adopters=5).discover(
        activation_window=window,
        retention_period=retention_period,
    )
    summary = result.to_summary()
    _cache.put(cache_key, summary)
    return summary


@app.get("/api/anomalies")
def get_anomalies(
    tenant_id: str = Query("default"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Anomaly detection on DAU series."""
    cache_key = f"anomalies:{tenant_id}:{start_date}:{end_date}"
    cached = _cache.get(cache_key)
    if cached:
        return cached

    df = _store.get_events()
    sd, ed = _date_params(start_date, end_date, df)
    filtered = df.filter(
        (pl.col("event_date") >= sd) & (pl.col("event_date") <= ed)
    )

    dau = (
        filtered.filter(pl.col("user_id") != "")
        .group_by("event_date")
        .agg(pl.col("user_id").n_unique().alias("dau"))
        .sort("event_date")
    )

    from stats.anomaly import AnomalyDetector, MetricSeries
    if not dau.is_empty() and len(dau) >= 4:
        series = MetricSeries(
            "dau",
            dau["event_date"].to_list(),
            [float(v) for v in dau["dau"].to_list()],
        )
        result = AnomalyDetector(sigma_threshold=2.0, window=4).detect(series)
    else:
        result = AnomalyDetector().detect(MetricSeries("dau", [date.today()], [0.0]))

    summary = result.to_summary()
    _cache.put(cache_key, summary)
    return summary


@app.post("/api/feedback")
def post_feedback(
    insight_rank: int = Query(...),
    vote: str = Query(..., pattern="^(useful|not_useful)$"),
    tenant_id: str = Query("default"),
):
    """Record insight feedback."""
    feedback_dir = Path(os.environ.get("FEEDBACK_DIR", "/tmp/insights-feedback"))
    feedback_dir.mkdir(parents=True, exist_ok=True)

    entry = {
        "tenant_id": tenant_id,
        "insight_rank": insight_rank,
        "vote": vote,
        "timestamp": date.today().isoformat(),
    }

    feedback_file = feedback_dir / "feedback.jsonl"
    with open(feedback_file, "a") as f:
        f.write(json.dumps(entry) + "\n")

    return {"status": "ok", "recorded": entry}


@app.get("/api/health")
def health():
    """Health check."""
    df = _store.get_events()
    return {
        "status": "ok",
        "events_loaded": len(df),
        "users": df["user_id"].n_unique() if not df.is_empty() else 0,
    }
