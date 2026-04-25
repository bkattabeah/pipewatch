"""Velocity tracking: measures how quickly pipeline error rates are changing."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class VelocityConfig:
    window: int = 10  # number of recent metrics to consider
    spike_threshold: float = 0.10  # delta per step that counts as a spike

    def validate(self) -> None:
        if self.window < 2:
            raise ValueError("window must be >= 2")
        if self.spike_threshold <= 0:
            raise ValueError("spike_threshold must be positive")


@dataclass
class VelocityResult:
    pipeline: str
    samples: int
    mean_delta: float  # average step-to-step change in error_rate
    max_delta: float   # largest single-step change
    is_spike: bool     # True if max_delta >= spike_threshold
    direction: str     # "rising", "falling", or "stable"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "samples": self.samples,
            "mean_delta": round(self.mean_delta, 6),
            "max_delta": round(self.max_delta, 6),
            "is_spike": self.is_spike,
            "direction": self.direction,
        }


def _direction(mean_delta: float, threshold: float = 0.001) -> str:
    if mean_delta > threshold:
        return "rising"
    if mean_delta < -threshold:
        return "falling"
    return "stable"


def compute_velocity(
    metrics: List[PipelineMetric],
    config: Optional[VelocityConfig] = None,
) -> Optional[VelocityResult]:
    """Compute velocity for a single pipeline's ordered metric history."""
    if config is None:
        config = VelocityConfig()
    config.validate()

    if not metrics:
        return None

    window_metrics = metrics[-config.window :]
    if len(window_metrics) < 2:
        return None

    rates = [m.error_rate for m in window_metrics]
    deltas = [rates[i + 1] - rates[i] for i in range(len(rates) - 1)]

    mean_delta = sum(deltas) / len(deltas)
    max_delta = max(abs(d) for d in deltas)
    is_spike = max_delta >= config.spike_threshold

    return VelocityResult(
        pipeline=window_metrics[0].pipeline,
        samples=len(window_metrics),
        mean_delta=mean_delta,
        max_delta=max_delta,
        is_spike=is_spike,
        direction=_direction(mean_delta),
    )
