"""Aggregate metrics across pipelines for summary statistics."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict
from pipewatch.metrics import PipelineMetric, PipelineStatus, error_rate, evaluate_status


@dataclass
class AggregateStats:
    total: int = 0
    healthy: int = 0
    warning: int = 0
    critical: int = 0
    unknown: int = 0
    avg_error_rate: float = 0.0
    max_error_rate: float = 0.0
    pipeline_names: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            "total": self.total,
            "healthy": self.healthy,
            "warning": self.warning,
            "critical": self.critical,
            "unknown": self.unknown,
            "avg_error_rate": round(self.avg_error_rate, 4),
            "max_error_rate": round(self.max_error_rate, 4),
            "pipeline_names": self.pipeline_names,
        }


def aggregate(metrics: List[PipelineMetric]) -> AggregateStats:
    """Compute aggregate statistics over a list of pipeline metrics."""
    if not metrics:
        return AggregateStats()

    stats = AggregateStats()
    stats.total = len(metrics)
    rates = []

    for m in metrics:
        stats.pipeline_names.append(m.pipeline_id)
        rate = error_rate(m)
        rates.append(rate)
        status = evaluate_status(m)
        if status == PipelineStatus.HEALTHY:
            stats.healthy += 1
        elif status == PipelineStatus.WARNING:
            stats.warning += 1
        elif status == PipelineStatus.CRITICAL:
            stats.critical += 1
        else:
            stats.unknown += 1

    stats.avg_error_rate = sum(rates) / len(rates)
    stats.max_error_rate = max(rates)
    return stats


def group_by_status(metrics: List[PipelineMetric]) -> Dict[str, List[PipelineMetric]]:
    """Group metrics by their evaluated status string."""
    groups: Dict[str, List[PipelineMetric]] = {}
    for m in metrics:
        key = evaluate_status(m).value
        groups.setdefault(key, []).append(m)
    return groups
