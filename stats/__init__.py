"""
Quorum Insights — Stats Layer

Post-processing of ClickHouse query results using polars.
Produces structured summaries ready for the LLM insight engine.

- retention: retention curve computation with cohort dimensions
- anomaly: WoW anomaly detection on core metrics (>2σ threshold)
"""

from stats.retention import RetentionComputer, RetentionResult, CohortRetention
from stats.anomaly import AnomalyDetector, AnomalyResult, Anomaly, MetricSeries
from stats.features import FeatureCorrelationAnalyzer, FeatureCorrelationResult, FeatureCorrelation
from stats.activation import ActivationDiscovery, ActivationResult, ActivationMoment
from stats.churn import ChurnDetector, ChurnResult, UserChurnRisk, DecayStage, ChurnSignal
from stats.aggregator import StatsAggregator, StatsSummary, Finding, FindingSeverity, FindingCategory

__all__ = [
    "RetentionComputer",
    "RetentionResult",
    "CohortRetention",
    "AnomalyDetector",
    "AnomalyResult",
    "Anomaly",
    "MetricSeries",
    "FeatureCorrelationAnalyzer",
    "FeatureCorrelationResult",
    "FeatureCorrelation",
    "ActivationDiscovery",
    "ActivationResult",
    "ActivationMoment",
    "ChurnDetector",
    "ChurnResult",
    "UserChurnRisk",
    "DecayStage",
    "ChurnSignal",
    "StatsAggregator",
    "StatsSummary",
    "Finding",
    "FindingSeverity",
    "FindingCategory",
]
