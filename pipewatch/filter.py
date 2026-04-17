"""Filtering utilities for pipeline metrics and reports."""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional
from pipewatch.metrics import PipelineMetric, PipelineStatus


@dataclass
class FilterCriteria:
    status: Optional[PipelineStatus] = None
    min_error_rate: Optional[float] = None
    max_error_rate: Optional[float] = None
    name_contains: Optional[str] = None

    def matches(self, metric: PipelineMetric) -> bool:
        from pipewatch.metrics import error_rate, evaluate_status
        rate = error_rate(metric)
        status = evaluate_status(metric)

        if self.status is not None and status != self.status:
            return False
        if self.min_error_rate is not None and rate < self.min_error_rate:
            return False
        if self.max_error_rate is not None and rate > self.max_error_rate:
            return False
        if self.name_contains is not None and self.name_contains.lower() not in metric.pipeline_name.lower():
            return False
        return True


def filter_metrics(metrics: List[PipelineMetric], criteria: FilterCriteria) -> List[PipelineMetric]:
    """Return only metrics that match the given criteria."""
    return [m for m in metrics if criteria.matches(m)]


def filter_by_status(metrics: List[PipelineMetric], status: PipelineStatus) -> List[PipelineMetric]:
    """Convenience: filter metrics by pipeline status."""
    return filter_metrics(metrics, FilterCriteria(status=status))


def filter_by_name(metrics: List[PipelineMetric], substring: str) -> List[PipelineMetric]:
    """Convenience: filter metrics whose pipeline name contains substring."""
    return filter_metrics(metrics, FilterCriteria(name_contains=substring))
