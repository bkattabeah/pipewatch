"""Surge detector: identifies pipelines with sudden spikes in error rate volume."""

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric, error_rate


@dataclass
class SurgeConfig:
    window: int = 10          # number of recent metrics to consider
    baseline_window: int = 30 # number of metrics for baseline
    multiplier: float = 2.0   # surge threshold: current_rate > baseline * multiplier

    def validate(self) -> None:
        if self.window < 2:
            raise ValueError("window must be >= 2")
        if self.baseline_window < self.window:
            raise ValueError("baseline_window must be >= window")
        if self.multiplier <= 1.0:
            raise ValueError("multiplier must be > 1.0")


@dataclass
class SurgeResult:
    pipeline: str
    current_rate: float
    baseline_rate: float
    multiplier_observed: float
    is_surge: bool

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "current_rate": round(self.current_rate, 4),
            "baseline_rate": round(self.baseline_rate, 4),
            "multiplier_observed": round(self.multiplier_observed, 4),
            "is_surge": self.is_surge,
        }


def _mean(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def detect_surge(
    pipeline: str,
    metrics: List[PipelineMetric],
    config: Optional[SurgeConfig] = None,
) -> Optional[SurgeResult]:
    """Detect whether a pipeline is experiencing a surge in error rate.

    Returns None if there is insufficient data.
    """
    if config is None:
        config = SurgeConfig()
    config.validate()

    if len(metrics) < config.window:
        return None

    recent = metrics[-config.window :]
    baseline_slice = metrics[-config.baseline_window : -config.window] if len(metrics) > config.window else []

    current_rate = _mean([error_rate(m) for m in recent])

    if not baseline_slice:
        return None

    baseline_rate = _mean([error_rate(m) for m in baseline_slice])
    multiplier_observed = (current_rate / baseline_rate) if baseline_rate > 0 else 0.0
    is_surge = baseline_rate > 0 and multiplier_observed >= config.multiplier

    return SurgeResult(
        pipeline=pipeline,
        current_rate=current_rate,
        baseline_rate=baseline_rate,
        multiplier_observed=multiplier_observed,
        is_surge=is_surge,
    )
