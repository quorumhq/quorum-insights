"""
Quorum Insights — Query Layer

Parameterized query builders for ClickHouse analytics:
- retention: retention curves using ClickHouse retention() function
- funnel: funnel analysis using windowFunnel() function
- cohort: cohort segmentation and comparison
- metrics: daily/weekly metrics from pre-aggregated MVs

All builders produce (sql, params) tuples. They do NOT execute queries.
Execution is the caller's responsibility via clickhouse-connect or clickhouse-driver.
"""

from query.retention import RetentionQuery, RetentionPeriod
from query.funnel import FunnelQuery
from query.cohort import CohortQuery
from query.metrics import MetricsQuery

__all__ = [
    "RetentionQuery",
    "RetentionPeriod",
    "FunnelQuery",
    "CohortQuery",
    "MetricsQuery",
]
