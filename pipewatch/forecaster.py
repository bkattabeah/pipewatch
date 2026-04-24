"""Simple linear forecasting for pipeline error rates."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.trend import _linear_slope
from pipewatch.metrics import PipelineMetric


@dataclass
class ForecastConfig:
    horizon: int = 5          # number of future ticks to project
    min_samples: int = 3      # minimum history length required

    def validate(self) -> None:
        if self.horizon < 1:
            raise ValueError("horizon must be >= 1")
        if self.min_samples < 2:
            raise ValueError("min_samples must be >= 2")


@dataclass
class ForecastPoint:
    tick: int
    predicted_error_rate: float

    def to_dict(self) -> dict:
        return {
            "tick": self.tick,
            "predicted_error_rate": round(self.predicted_error_rate, 4),
        }


@dataclass
class ForecastResult:
    pipeline: str
    points: List[ForecastPoint] = field(default_factory=list)
    slope: float = 0.0
    insufficient_data: bool = False

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "slope": round(self.slope, 6),
            "insufficient_data": self.insufficient_data,
            "points": [p.to_dict() for p in self.points],
        }


def forecast(
    pipeline: str,
    history: List[PipelineMetric],
    config: Optional[ForecastConfig] = None,
) -> ForecastResult:
    """Project future error rates for *pipeline* based on recorded history."""
    if config is None:
        config = ForecastConfig()
    config.validate()

    rates = [m.error_rate for m in history]

    if len(rates) < config.min_samples:
        return ForecastResult(pipeline=pipeline, insufficient_data=True)

    slope = _linear_slope(rates)
    last_rate = rates[-1]
    last_tick = len(rates) - 1

    points: List[ForecastPoint] = []
    for i in range(1, config.horizon + 1):
        predicted = last_rate + slope * i
        predicted = max(0.0, min(1.0, predicted))
        points.append(ForecastPoint(tick=last_tick + i, predicted_error_rate=predicted))

    return ForecastResult(pipeline=pipeline, points=points, slope=slope)
