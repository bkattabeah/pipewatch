"""Pipeline metric profiler: computes statistical profiles over historical data."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class ProfileStats:
    pipeline: str
    count: int
    mean_error_rate: float
    min_error_rate: float
    max_error_rate: float
    stddev_error_rate: float
    mean_throughput: float
    p50_error_rate: float
    p95_error_rate: float

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "count": self.count,
            "mean_error_rate": round(self.mean_error_rate, 6),
            "min_error_rate": round(self.min_error_rate, 6),
            "max_error_rate": round(self.max_error_rate, 6),
            "stddev_error_rate": round(self.stddev_error_rate, 6),
            "mean_throughput": round(self.mean_throughput, 4),
            "p50_error_rate": round(self.p50_error_rate, 6),
            "p95_error_rate": round(self.p95_error_rate, 6),
        }


def _percentile(sorted_values: List[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    idx = (len(sorted_values) - 1) * pct
    lo, hi = int(idx), min(int(idx) + 1, len(sorted_values) - 1)
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (idx - lo)


def _mean(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _stddev(values: List[float], mean: float) -> float:
    if len(values) < 2:
        return 0.0
    variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return variance ** 0.5


def profile_metrics(pipeline: str, metrics: List[PipelineMetric]) -> Optional[ProfileStats]:
    """Compute a statistical profile for a pipeline from a list of metrics."""
    if not metrics:
        return None

    from pipewatch.metrics import error_rate as calc_error_rate

    error_rates = [calc_error_rate(m) for m in metrics]
    throughputs = [float(m.total_records) for m in metrics]
    sorted_er = sorted(error_rates)

    mean_er = _mean(error_rates)
    return ProfileStats(
        pipeline=pipeline,
        count=len(metrics),
        mean_error_rate=mean_er,
        min_error_rate=sorted_er[0],
        max_error_rate=sorted_er[-1],
        stddev_error_rate=_stddev(error_rates, mean_er),
        mean_throughput=_mean(throughputs),
        p50_error_rate=_percentile(sorted_er, 0.50),
        p95_error_rate=_percentile(sorted_er, 0.95),
    )
