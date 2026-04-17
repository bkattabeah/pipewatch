"""Annotate pipeline metrics with human-readable notes based on status and thresholds."""

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric, PipelineStatus


@dataclass
class Annotation:
    pipeline: str
    level: str  # 'info', 'warning', 'critical'
    message: str

    def __str__(self) -> str:
        return f"[{self.level.upper()}] {self.pipeline}: {self.message}"

    def to_dict(self) -> dict:
        return {"pipeline": self.pipeline, "level": self.level, "message": self.message}


@dataclass
class AnnotationRule:
    level: str
    condition: callable
    message_fn: callable


DEFAULT_RULES: List[AnnotationRule] = [
    AnnotationRule(
        level="critical",
        condition=lambda m: m.status == PipelineStatus.CRITICAL,
        message_fn=lambda m: f"Pipeline is in CRITICAL state (error rate: {m.error_rate:.1%})",
    ),
    AnnotationRule(
        level="warning",
        condition=lambda m: m.status == PipelineStatus.WARNING,
        message_fn=lambda m: f"Pipeline has elevated error rate ({m.error_rate:.1%})",
    ),
    AnnotationRule(
        level="info",
        condition=lambda m: m.processed == 0,
        message_fn=lambda m: "No records processed in this interval",
    ),
    AnnotationRule(
        level="info",
        condition=lambda m: m.status == PipelineStatus.HEALTHY and m.processed > 0,
        message_fn=lambda m: f"Operating normally ({m.processed} records processed)",
    ),
]


def annotate(metric: PipelineMetric, rules: Optional[List[AnnotationRule]] = None) -> List[Annotation]:
    """Return annotations for a single metric."""
    rules = rules if rules is not None else DEFAULT_RULES
    return [
        Annotation(pipeline=metric.pipeline, level=r.level, message=r.message_fn(metric))
        for r in rules
        if r.condition(metric)
    ]


def annotate_many(metrics: List[PipelineMetric], rules: Optional[List[AnnotationRule]] = None) -> List[Annotation]:
    """Return all annotations across a list of metrics."""
    result = []
    for m in metrics:
        result.extend(annotate(m, rules))
    return result
