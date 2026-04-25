"""Group pipeline metrics by a chosen key and compute per-group statistics."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from pipewatch.metrics import PipelineMetric, PipelineStatus


@dataclass
class GroupStats:
    key: str
    total: int = 0
    healthy: int = 0
    warning: int = 0
    critical: int = 0
    unknown: int = 0
    avg_error_rate: float = 0.0
    pipelines: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "total": self.total,
            "healthy": self.healthy,
            "warning": self.warning,
            "critical": self.critical,
            "unknown": self.unknown,
            "avg_error_rate": round(self.avg_error_rate, 4),
            "pipelines": self.pipelines,
        }


@dataclass
class GroupReport:
    groups: Dict[str, GroupStats] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {k: v.to_dict() for k, v in self.groups.items()}

    def sorted_by(self, attr: str = "total", descending: bool = True) -> List[GroupStats]:
        return sorted(self.groups.values(), key=lambda g: getattr(g, attr), reverse=descending)


def _status_counter(stats: GroupStats, status: PipelineStatus) -> None:
    if status == PipelineStatus.HEALTHY:
        stats.healthy += 1
    elif status == PipelineStatus.WARNING:
        stats.warning += 1
    elif status == PipelineStatus.CRITICAL:
        stats.critical += 1
    else:
        stats.unknown += 1


def group_metrics(
    metrics: List[PipelineMetric],
    key_fn: Callable[[PipelineMetric], Optional[str]],
    fallback: str = "ungrouped",
) -> GroupReport:
    """Group metrics using *key_fn* and compute per-group stats."""
    report = GroupReport()

    for metric in metrics:
        key = key_fn(metric) or fallback
        if key not in report.groups:
            report.groups[key] = GroupStats(key=key)

        stats = report.groups[key]
        stats.total += 1
        stats.pipelines.append(metric.pipeline_id)
        _status_counter(stats, metric.status)

    for stats in report.groups.values():
        if stats.total > 0:
            pipeline_ids = set(stats.pipelines)
            matching = [m for m in metrics if m.pipeline_id in pipeline_ids]
            stats.avg_error_rate = sum(m.error_rate for m in matching) / len(matching)

    return report


def group_by_prefix(metrics: List[PipelineMetric], separator: str = "_") -> GroupReport:
    """Convenience: group by the first segment of the pipeline_id."""
    def key_fn(m: PipelineMetric) -> Optional[str]:
        parts = m.pipeline_id.split(separator, 1)
        return parts[0] if parts else None

    return group_metrics(metrics, key_fn)
