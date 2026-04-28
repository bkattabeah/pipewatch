"""Metric decay analysis: detects pipelines whose error rates are slowly degrading over time."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class DecayConfig:
    min_samples: int = 5
    decay_threshold: float = 0.01  # minimum slope to be considered decaying
    window: int = 20  # number of recent metrics to consider

    def validate(self) -> None:
        if self.min_samples < 2:
            raise ValueError("min_samples must be at least 2")
        if self.decay_threshold <= 0:
            raise ValueError("decay_threshold must be positive")
        if self.window < self.min_samples:
            raise ValueError("window must be >= min_samples")


@dataclass
class DecayResult:
    pipeline: str
    slope: float
    is_decaying: bool
    sample_count: int
    first_error_rate: float
    last_error_rate: float

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "slope": round(self.slope, 6),
            "is_decaying": self.is_decaying,
            "sample_count": self.sample_count,
            "first_error_rate": round(self.first_error_rate, 4),
            "last_error_rate": round(self.last_error_rate, 4),
        }


def _slope(values: List[float]) -> float:
    """Compute linear regression slope for a list of values."""
    n = len(values)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    numerator = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    denominator = sum((i - x_mean) ** 2 for i in range(n))
    return numerator / denominator if denominator != 0 else 0.0


def analyze_decay(
    metrics: List[PipelineMetric],
    config: Optional[DecayConfig] = None,
) -> Optional[DecayResult]:
    """Analyze a series of metrics for a single pipeline to detect decay."""
    if config is None:
        config = DecayConfig()
    config.validate()

    recent = metrics[-config.window :]
    if len(recent) < config.min_samples:
        return None

    from pipewatch.metrics import error_rate

    rates = [error_rate(m) for m in recent]
    slope = _slope(rates)
    is_decaying = slope >= config.decay_threshold

    return DecayResult(
        pipeline=recent[0].pipeline,
        slope=slope,
        is_decaying=is_decaying,
        sample_count=len(recent),
        first_error_rate=rates[0],
        last_error_rate=rates[-1],
    )
