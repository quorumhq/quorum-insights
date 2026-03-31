"""
Week-over-week anomaly detection on core metrics.

Detects significant changes (>Nσ from rolling mean) in:
- DAU, WAU, MAU
- Retention rates (D1, D7, D30)
- Session counts
- Feature usage
- AI quality scores

Uses a rolling window for baseline computation, not global stats,
so seasonal trends don't trigger false positives.

Usage:
    detector = AnomalyDetector(sigma_threshold=2.0, min_data_points=4)
    series = MetricSeries("dau", dates, values)
    result = detector.detect(series)
    result.to_summary()  # -> dict for LLM engine
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Optional

import polars as pl


class Severity(str, Enum):
    """Anomaly severity based on sigma distance."""
    WARNING = "warning"   # 2-3σ
    CRITICAL = "critical"  # >3σ


class Direction(str, Enum):
    """Direction of the anomaly."""
    UP = "up"
    DOWN = "down"


@dataclass
class Anomaly:
    """A single detected anomaly."""

    metric_name: str
    date: date
    value: float
    expected: float  # rolling mean
    sigma_distance: float  # how many σ away
    direction: Direction
    severity: Severity
    pct_change: float  # % change from expected

    def to_dict(self) -> dict:
        return {
            "metric": self.metric_name,
            "date": self.date.isoformat(),
            "value": round(self.value, 4),
            "expected": round(self.expected, 4),
            "sigma_distance": round(self.sigma_distance, 2),
            "direction": self.direction.value,
            "severity": self.severity.value,
            "pct_change": round(self.pct_change, 4),
        }


@dataclass
class MetricSeries:
    """A time series of metric values."""

    name: str
    dates: list[date]
    values: list[float]

    def __post_init__(self):
        if len(self.dates) != len(self.values):
            raise ValueError(
                f"dates ({len(self.dates)}) and values ({len(self.values)}) must have same length"
            )

    def to_polars(self) -> pl.DataFrame:
        return pl.DataFrame({
            "date": self.dates,
            "value": self.values,
        }).sort("date")

    @classmethod
    def from_polars(cls, name: str, df: pl.DataFrame,
                    date_col: str = "date", value_col: str = "value") -> MetricSeries:
        """Create from a polars DataFrame."""
        sorted_df = df.sort(date_col)
        return cls(
            name=name,
            dates=sorted_df[date_col].to_list(),
            values=sorted_df[value_col].to_list(),
        )


@dataclass
class AnomalyResult:
    """Result of anomaly detection on one or more metric series."""

    anomalies: list[Anomaly]
    metrics_checked: list[str]
    date_range: tuple[date, date]
    total_data_points: int

    def to_summary(self) -> dict:
        """Structured summary for the LLM insight engine."""
        return {
            "metric": "anomaly_detection",
            "date_range": {
                "start": self.date_range[0].isoformat(),
                "end": self.date_range[1].isoformat(),
            },
            "metrics_checked": self.metrics_checked,
            "total_data_points": self.total_data_points,
            "anomaly_count": len(self.anomalies),
            "critical_count": sum(1 for a in self.anomalies if a.severity == Severity.CRITICAL),
            "warning_count": sum(1 for a in self.anomalies if a.severity == Severity.WARNING),
            "anomalies": [a.to_dict() for a in self.anomalies],
        }

    @property
    def anomaly_count(self) -> int:
        return len(self.anomalies)

    @property
    def has_critical(self) -> bool:
        return any(a.severity == Severity.CRITICAL for a in self.anomalies)


class AnomalyDetector:
    """Detect week-over-week anomalies using rolling statistics.

    Algorithm:
    1. Compute rolling mean and std over `window` periods
    2. For each point, compute z-score = (value - rolling_mean) / rolling_std
    3. Flag points where |z-score| > sigma_threshold
    4. Classify severity: 2-3σ = warning, >3σ = critical
    """

    def __init__(
        self,
        sigma_threshold: float = 2.0,
        window: int = 4,
        min_data_points: int = 4,
        min_std_pct: float = 0.01,
    ):
        if sigma_threshold <= 0:
            raise ValueError("sigma_threshold must be positive")
        if window < 2:
            raise ValueError("window must be >= 2")
        if min_data_points < 2:
            raise ValueError("min_data_points must be >= 2")

        self.sigma_threshold = sigma_threshold
        self.window = window
        self.min_data_points = min_data_points
        self.min_std_pct = min_std_pct  # floor: std must be >= this % of mean

    def detect(self, series: MetricSeries) -> AnomalyResult:
        """Detect anomalies in a single metric series."""
        df = series.to_polars()

        if len(df) < self.min_data_points:
            date_range = (
                (series.dates[0] if series.dates else date.today()),
                (series.dates[-1] if series.dates else date.today()),
            )
            return AnomalyResult(
                anomalies=[],
                metrics_checked=[series.name],
                date_range=date_range,
                total_data_points=len(df),
            )

        # Compute rolling stats (shift by 1 so current point isn't in its own baseline)
        df = df.with_columns([
            pl.col("value")
            .shift(1)
            .rolling_mean(window_size=self.window, min_samples=max(2, self.window // 2))
            .alias("rolling_mean"),
            pl.col("value")
            .shift(1)
            .rolling_std(window_size=self.window, min_samples=max(2, self.window // 2))
            .alias("rolling_std"),
        ])

        anomalies: list[Anomaly] = []

        for row in df.iter_rows(named=True):
            r_mean = row["rolling_mean"]
            r_std = row["rolling_std"]
            value = row["value"]
            dt = row["date"]

            if r_mean is None or r_std is None:
                continue

            # Floor the std at min_std_pct of the rolling mean to avoid
            # false positives when variance is near-zero (early window, stable data)
            std_floor = abs(r_mean) * self.min_std_pct if r_mean != 0 else 0
            effective_std = max(r_std, std_floor)

            if effective_std == 0:
                if value != r_mean:
                    sigma_dist = float("inf")
                else:
                    continue
            else:
                sigma_dist = abs(value - r_mean) / effective_std

            if sigma_dist >= self.sigma_threshold:
                direction = Direction.UP if value > r_mean else Direction.DOWN
                severity = Severity.CRITICAL if sigma_dist >= 3.0 else Severity.WARNING
                pct_change = ((value - r_mean) / r_mean) if r_mean != 0 else float("inf")

                anomalies.append(Anomaly(
                    metric_name=series.name,
                    date=dt,
                    value=value,
                    expected=r_mean,
                    sigma_distance=sigma_dist,
                    direction=direction,
                    severity=severity,
                    pct_change=pct_change,
                ))

        # Sort: critical first, then by sigma distance descending
        anomalies.sort(
            key=lambda a: (0 if a.severity == Severity.CRITICAL else 1, -a.sigma_distance)
        )

        return AnomalyResult(
            anomalies=anomalies,
            metrics_checked=[series.name],
            date_range=(series.dates[0], series.dates[-1]),
            total_data_points=len(df),
        )

    def detect_multiple(self, series_list: list[MetricSeries]) -> AnomalyResult:
        """Detect anomalies across multiple metric series."""
        all_anomalies: list[Anomaly] = []
        all_names: list[str] = []
        total_points = 0
        min_date = date.max
        max_date = date.min

        for series in series_list:
            result = self.detect(series)
            all_anomalies.extend(result.anomalies)
            all_names.append(series.name)
            total_points += result.total_data_points
            if result.date_range[0] < min_date:
                min_date = result.date_range[0]
            if result.date_range[1] > max_date:
                max_date = result.date_range[1]

        # Sort by severity (critical first), then by sigma distance
        all_anomalies.sort(
            key=lambda a: (0 if a.severity == Severity.CRITICAL else 1, -a.sigma_distance)
        )

        if min_date == date.max:
            min_date = date.today()
            max_date = date.today()

        return AnomalyResult(
            anomalies=all_anomalies,
            metrics_checked=all_names,
            date_range=(min_date, max_date),
            total_data_points=total_points,
        )
