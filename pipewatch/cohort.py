"""Cohort analysis: group pipelines by time window and compare error rate trends."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric, error_rate


@dataclass
class CohortConfig:
    bucket_minutes: int = 60
    min_cohort_size: int = 1

    def validate(self) -> None:
        if self.bucket_minutes <= 0:
            raise ValueError("bucket_minutes must be positive")
        if self.min_cohort_size < 1:
            raise ValueError("min_cohort_size must be at least 1")


@dataclass
class CohortBucket:
    label: str
    start: datetime
    metrics: List[PipelineMetric] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.metrics)

    @property
    def avg_error_rate(self) -> float:
        if not self.metrics:
            return 0.0
        return sum(error_rate(m) for m in self.metrics) / len(self.metrics)

    def to_dict(self) -> dict:
        return {
            "label": self.label,
            "start": self.start.isoformat(),
            "count": self.count,
            "avg_error_rate": round(self.avg_error_rate, 4),
        }


@dataclass
class CohortResult:
    buckets: List[CohortBucket]
    config: CohortConfig

    def to_dict(self) -> dict:
        return {
            "bucket_minutes": self.config.bucket_minutes,
            "buckets": [b.to_dict() for b in self.buckets],
        }

    def peak_bucket(self) -> Optional[CohortBucket]:
        if not self.buckets:
            return None
        return max(self.buckets, key=lambda b: b.avg_error_rate)


def build_cohort(
    metrics: List[PipelineMetric],
    config: Optional[CohortConfig] = None,
) -> CohortResult:
    config = config or CohortConfig()
    config.validate()

    buckets: Dict[str, CohortBucket] = {}
    window = config.bucket_minutes * 60

    for m in metrics:
        ts = m.timestamp
        bucket_epoch = int(ts.timestamp() // window) * window
        bucket_dt = datetime.utcfromtimestamp(bucket_epoch)
        label = bucket_dt.strftime("%Y-%m-%dT%H:%M")
        if label not in buckets:
            buckets[label] = CohortBucket(label=label, start=bucket_dt)
        buckets[label].metrics.append(m)

    filtered = [
        b for b in buckets.values() if b.count >= config.min_cohort_size
    ]
    filtered.sort(key=lambda b: b.start)
    return CohortResult(buckets=filtered, config=config)
