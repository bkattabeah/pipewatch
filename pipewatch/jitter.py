"""Jitter analysis: measures timing irregularity in pipeline execution intervals."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class JitterConfig:
    min_samples: int = 3
    high_jitter_threshold: float = 0.5  # coefficient of variation
    critical_jitter_threshold: float = 1.0

    def validate(self) -> None:
        if self.min_samples < 2:
            raise ValueError("min_samples must be at least 2")
        if self.high_jitter_threshold <= 0:
            raise ValueError("high_jitter_threshold must be positive")
        if self.critical_jitter_threshold <= self.high_jitter_threshold:
            raise ValueError("critical_jitter_threshold must exceed high_jitter_threshold")


@dataclass
class JitterResult:
    pipeline: str
    sample_count: int
    mean_interval: Optional[float]
    stddev_interval: Optional[float]
    coefficient_of_variation: Optional[float]
    level: str  # "ok", "high", "critical", "insufficient_data"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "sample_count": self.sample_count,
            "mean_interval": self.mean_interval,
            "stddev_interval": self.stddev_interval,
            "coefficient_of_variation": self.coefficient_of_variation,
            "level": self.level,
        }


def _mean(values: List[float]) -> float:
    return sum(values) / len(values)


def _stddev(values: List[float]) -> float:
    m = _mean(values)
    variance = sum((v - m) ** 2 for v in values) / len(values)
    return variance ** 0.5


def analyze_jitter(
    pipeline: str,
    metrics: List[PipelineMetric],
    config: Optional[JitterConfig] = None,
) -> JitterResult:
    """Compute timing jitter for a single pipeline from its metric history."""
    if config is None:
        config = JitterConfig()

    sorted_metrics = sorted(metrics, key=lambda m: m.timestamp)
    timestamps = [m.timestamp for m in sorted_metrics]

    if len(timestamps) < config.min_samples:
        return JitterResult(
            pipeline=pipeline,
            sample_count=len(timestamps),
            mean_interval=None,
            stddev_interval=None,
            coefficient_of_variation=None,
            level="insufficient_data",
        )

    intervals = [timestamps[i + 1] - timestamps[i] for i in range(len(timestamps) - 1)]
    mean_iv = _mean(intervals)
    std_iv = _stddev(intervals)
    cv = std_iv / mean_iv if mean_iv > 0 else 0.0

    if cv >= config.critical_jitter_threshold:
        level = "critical"
    elif cv >= config.high_jitter_threshold:
        level = "high"
    else:
        level = "ok"

    return JitterResult(
        pipeline=pipeline,
        sample_count=len(timestamps),
        mean_interval=round(mean_iv, 4),
        stddev_interval=round(std_iv, 4),
        coefficient_of_variation=round(cv, 4),
        level=level,
    )
