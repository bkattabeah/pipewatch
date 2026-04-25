"""Error-rate histogram: bucket metrics by value range and compute distributions."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Sequence

from pipewatch.metrics import PipelineMetric


@dataclass
class HistogramBucket:
    low: float
    high: float
    count: int = 0
    pipelines: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "range": f"[{self.low:.2f}, {self.high:.2f})",
            "low": self.low,
            "high": self.high,
            "count": self.count,
            "pipelines": list(self.pipelines),
        }


@dataclass
class HistogramResult:
    buckets: List[HistogramBucket]
    total: int
    min_rate: Optional[float]
    max_rate: Optional[float]
    mean_rate: Optional[float]

    def to_dict(self) -> dict:
        return {
            "buckets": [b.to_dict() for b in self.buckets],
            "total": self.total,
            "min_rate": self.min_rate,
            "max_rate": self.max_rate,
            "mean_rate": self.mean_rate,
        }

    def peak_bucket(self) -> Optional[HistogramBucket]:
        if not self.buckets:
            return None
        return max(self.buckets, key=lambda b: b.count)


def build_histogram(
    metrics: Sequence[PipelineMetric],
    num_buckets: int = 10,
) -> HistogramResult:
    """Build an error-rate histogram from a sequence of pipeline metrics."""
    if not metrics:
        return HistogramResult(buckets=[], total=0, min_rate=None, max_rate=None, mean_rate=None)

    from pipewatch.metrics import error_rate

    rates = [(m.pipeline_name, error_rate(m)) for m in metrics]
    values = [r for _, r in rates]

    lo, hi = min(values), max(values)
    # Avoid zero-width range
    span = hi - lo if hi != lo else 1.0
    step = span / num_buckets

    buckets: List[HistogramBucket] = [
        HistogramBucket(low=lo + i * step, high=lo + (i + 1) * step)
        for i in range(num_buckets)
    ]

    for name, rate in rates:
        idx = min(int((rate - lo) / step), num_buckets - 1)
        buckets[idx].count += 1
        buckets[idx].pipelines.append(name)

    mean = sum(values) / len(values)
    return HistogramResult(
        buckets=buckets,
        total=len(metrics),
        min_rate=lo,
        max_rate=hi,
        mean_rate=round(mean, 6),
    )
