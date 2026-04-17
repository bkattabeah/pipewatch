"""Trend analysis for pipeline metrics over time."""
from dataclasses import dataclass
from typing import List, Optional
from pipewatch.metrics import PipelineMetric, error_rate


@dataclass
class TrendResult:
    pipeline_id: str
    sample_count: int
    avg_error_rate: float
    min_error_rate: float
    max_error_rate: float
    trend_direction: str  # 'improving', 'degrading', 'stable'
    slope: float

    def to_dict(self) -> dict:
        return {
            "pipeline_id": self.pipeline_id,
            "sample_count": self.sample_count,
            "avg_error_rate": round(self.avg_error_rate, 4),
            "min_error_rate": round(self.min_error_rate, 4),
            "max_error_rate": round(self.max_error_rate, 4),
            "trend_direction": self.trend_direction,
            "slope": round(self.slope, 6),
        }


def _linear_slope(values: List[float]) -> float:
    """Compute slope via simple linear regression."""
    n = len(values)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2
    y_mean = sum(values) / n
    numerator = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    denominator = sum((i - x_mean) ** 2 for i in range(n))
    return numerator / denominator if denominator else 0.0


def analyze_trend(
    pipeline_id: str,
    metrics: List[PipelineMetric],
    stable_threshold: float = 0.005,
) -> Optional[TrendResult]:
    """Analyze error rate trend for a pipeline from a list of metrics."""
    if not metrics:
        return None

    rates = [error_rate(m) for m in metrics]
    slope = _linear_slope(rates)

    if abs(slope) <= stable_threshold:
        direction = "stable"
    elif slope < 0:
        direction = "improving"
    else:
        direction = "degrading"

    return TrendResult(
        pipeline_id=pipeline_id,
        sample_count=len(rates),
        avg_error_rate=sum(rates) / len(rates),
        min_error_rate=min(rates),
        max_error_rate=max(rates),
        trend_direction=direction,
        slope=slope,
    )
