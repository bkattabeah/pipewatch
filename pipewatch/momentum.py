"""Momentum analysis: measures rate-of-change acceleration in pipeline error rates."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class MomentumConfig:
    window: int = 10          # number of recent metrics to consider
    min_samples: int = 4      # minimum samples required for analysis
    accel_threshold: float = 0.05  # acceleration magnitude to flag as significant

    def validate(self) -> None:
        if self.window < 2:
            raise ValueError("window must be >= 2")
        if self.min_samples < 2:
            raise ValueError("min_samples must be >= 2")
        if self.min_samples > self.window:
            raise ValueError("min_samples must not exceed window")
        if self.accel_threshold < 0:
            raise ValueError("accel_threshold must be non-negative")


@dataclass
class MomentumResult:
    pipeline: str
    sample_count: int
    first_slope: float        # slope of first half
    second_slope: float       # slope of second half
    acceleration: float       # second_slope - first_slope
    is_accelerating: bool     # True if |acceleration| >= threshold
    direction: str            # "worsening", "improving", "stable"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "sample_count": self.sample_count,
            "first_slope": round(self.first_slope, 6),
            "second_slope": round(self.second_slope, 6),
            "acceleration": round(self.acceleration, 6),
            "is_accelerating": self.is_accelerating,
            "direction": self.direction,
        }


def _slope(values: List[float]) -> float:
    """Compute linear slope via least-squares over index positions."""
    n = len(values)
    if n < 2:
        return 0.0
    xs = list(range(n))
    x_mean = sum(xs) / n
    y_mean = sum(values) / n
    num = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, values))
    den = sum((x - x_mean) ** 2 for x in xs)
    return num / den if den != 0 else 0.0


def analyze_momentum(
    pipeline: str,
    metrics: List[PipelineMetric],
    config: Optional[MomentumConfig] = None,
) -> Optional[MomentumResult]:
    """Return a MomentumResult for the pipeline or None if insufficient data."""
    cfg = config or MomentumConfig()
    cfg.validate()

    recent = metrics[-cfg.window :]
    if len(recent) < cfg.min_samples:
        return None

    rates = [m.error_rate for m in recent]
    mid = len(rates) // 2
    first_slope = _slope(rates[:mid])
    second_slope = _slope(rates[mid:])
    acceleration = second_slope - first_slope
    is_accelerating = abs(acceleration) >= cfg.accel_threshold

    if acceleration > cfg.accel_threshold:
        direction = "worsening"
    elif acceleration < -cfg.accel_threshold:
        direction = "improving"
    else:
        direction = "stable"

    return MomentumResult(
        pipeline=pipeline,
        sample_count=len(recent),
        first_slope=first_slope,
        second_slope=second_slope,
        acceleration=acceleration,
        is_accelerating=is_accelerating,
        direction=direction,
    )
