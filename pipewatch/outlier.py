"""Outlier detection for pipeline error rates using IQR-based method."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class OutlierConfig:
    min_samples: int = 4
    iqr_multiplier: float = 1.5

    def validate(self) -> None:
        if self.min_samples < 2:
            raise ValueError("min_samples must be >= 2")
        if self.iqr_multiplier <= 0:
            raise ValueError("iqr_multiplier must be positive")


@dataclass
class OutlierResult:
    pipeline: str
    error_rate: float
    is_outlier: bool
    lower_fence: float
    upper_fence: float
    reason: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "error_rate": self.error_rate,
            "is_outlier": self.is_outlier,
            "lower_fence": self.lower_fence,
            "upper_fence": self.upper_fence,
            "reason": self.reason,
        }


def _quartiles(values: List[float]):
    """Return (Q1, Q3) for a sorted list."""
    s = sorted(values)
    n = len(s)
    mid = n // 2
    lower = s[:mid]
    upper = s[mid:] if n % 2 == 0 else s[mid + 1 :]
    q1 = sum(lower) / len(lower) if lower else s[0]
    q3 = sum(upper) / len(upper) if upper else s[-1]
    return q1, q3


def detect_outliers(
    metrics: List[PipelineMetric],
    config: Optional[OutlierConfig] = None,
) -> List[OutlierResult]:
    """Detect pipelines whose error rate is an outlier among peers."""
    if config is None:
        config = OutlierConfig()
    config.validate()

    if len(metrics) < config.min_samples:
        return []

    rates = [m.error_rate for m in metrics]
    q1, q3 = _quartiles(rates)
    iqr = q3 - q1
    lower = q1 - config.iqr_multiplier * iqr
    upper = q3 + config.iqr_multiplier * iqr

    results: List[OutlierResult] = []
    for m in metrics:
        is_out = m.error_rate < lower or m.error_rate > upper
        reason: Optional[str] = None
        if m.error_rate > upper:
            reason = "above upper fence"
        elif m.error_rate < lower:
            reason = "below lower fence"
        results.append(
            OutlierResult(
                pipeline=m.pipeline,
                error_rate=m.error_rate,
                is_outlier=is_out,
                lower_fence=round(lower, 6),
                upper_fence=round(upper, 6),
                reason=reason,
            )
        )
    return results
