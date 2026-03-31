"""
Quorum Insights — Stats Layer

Post-processing of ClickHouse query results using polars.
Produces structured summaries ready for the LLM insight engine.

- retention: retention curve computation with cohort dimensions
- anomaly: WoW anomaly detection on core metrics (>2σ threshold)
"""

from stats.retention import RetentionComputer, RetentionResult, CohortRetention
from stats.anomaly import AnomalyDetector, AnomalyResult, Anomaly, MetricSeries

__all__ = [
    "RetentionComputer",
    "RetentionResult",
    "CohortRetention",
    "AnomalyDetector",
    "AnomalyResult",
    "Anomaly",
    "MetricSeries",
]
