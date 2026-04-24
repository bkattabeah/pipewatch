"""Pipeline error-rate heatmap: buckets metrics by hour-of-day and computes
average error rates per bucket, useful for spotting recurring failure windows."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List

from pipewatch.metrics import PipelineMetric


@dataclass
class HeatmapBucket:
    hour: int  # 0-23
    sample_count: int
    avg_error_rate: float
    max_error_rate: float

    def to_dict(self) -> dict:
        return {
            "hour": self.hour,
            "sample_count": self.sample_count,
            "avg_error_rate": round(self.avg_error_rate, 4),
            "max_error_rate": round(self.max_error_rate, 4),
        }


@dataclass
class HeatmapResult:
    pipeline: str
    buckets: List[HeatmapBucket] = field(default_factory=list)

    def peak_hour(self) -> int | None:
        """Return the hour with the highest average error rate, or None."""
        if not self.buckets:
            return None
        return max(self.buckets, key=lambda b: b.avg_error_rate).hour

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "peak_hour": self.peak_hour(),
            "buckets": [b.to_dict() for b in self.buckets],
        }


def build_heatmap(pipeline: str, metrics: List[PipelineMetric]) -> HeatmapResult:
    """Aggregate *metrics* for *pipeline* into 24 hourly buckets."""
    buckets: Dict[int, List[float]] = {h: [] for h in range(24)}

    for m in metrics:
        if m.pipeline != pipeline:
            continue
        ts = m.timestamp if isinstance(m.timestamp, datetime) else datetime.fromisoformat(str(m.timestamp))
        rate = m.error_rate if m.error_rate is not None else 0.0
        buckets[ts.hour].append(rate)

    result_buckets: List[HeatmapBucket] = []
    for hour in range(24):
        samples = buckets[hour]
        if not samples:
            continue
        result_buckets.append(
            HeatmapBucket(
                hour=hour,
                sample_count=len(samples),
                avg_error_rate=sum(samples) / len(samples),
                max_error_rate=max(samples),
            )
        )

    return HeatmapResult(pipeline=pipeline, buckets=result_buckets)
