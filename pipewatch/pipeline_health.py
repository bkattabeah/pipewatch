"""Aggregate pipeline health scoring across all tracked metrics."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.aggregator import AggregateStats, aggregate, group_by_status


@dataclass
class PipelineHealthScore:
    """Overall health score for a set of pipeline metrics."""

    score: float  # 0.0 (worst) to 1.0 (best)
    grade: str    # A, B, C, D, F
    stats: AggregateStats
    by_status: Dict[str, List[str]]  # status -> list of pipeline names
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "score": round(self.score, 4),
            "grade": self.grade,
            "stats": self.stats.to_dict(),
            "by_status": self.by_status,
            "warnings": self.warnings,
        }


def _grade(score: float) -> str:
    """Convert a 0–1 score to a letter grade."""
    if score >= 0.90:
        return "A"
    if score >= 0.75:
        return "B"
    if score >= 0.60:
        return "C"
    if score >= 0.40:
        return "D"
    return "F"


def compute_health_score(metrics: List[PipelineMetric]) -> PipelineHealthScore:
    """Compute an aggregate health score from a list of pipeline metrics.

    Scoring logic:
    - Each healthy pipeline contributes 1.0
    - Each warning pipeline contributes 0.5
    - Each critical or unknown pipeline contributes 0.0
    - Score = weighted sum / total pipelines
    """
    if not metrics:
        stats = aggregate([])
        return PipelineHealthScore(
            score=0.0,
            grade="F",
            stats=stats,
            by_status={},
            warnings=["No metrics available to score."],
        )

    weights = {
        PipelineStatus.HEALTHY: 1.0,
        PipelineStatus.WARNING: 0.5,
        PipelineStatus.CRITICAL: 0.0,
        PipelineStatus.UNKNOWN: 0.0,
    }

    total_weight = sum(weights.get(m.status, 0.0) for m in metrics)
    score = total_weight / len(metrics)
    grade = _grade(score)

    stats = aggregate(metrics)
    grouped = group_by_status(metrics)
    by_status: Dict[str, List[str]] = {
        status.value: [m.pipeline_name for m in group]
        for status, group in grouped.items()
    }

    warnings: List[str] = []
    if stats.critical_count > 0:
        warnings.append(f"{stats.critical_count} pipeline(s) are in CRITICAL state.")
    if stats.avg_error_rate > 0.10:
        warnings.append(
            f"Average error rate is high: {stats.avg_error_rate:.1%}"
        )

    return PipelineHealthScore(
        score=score,
        grade=grade,
        stats=stats,
        by_status=by_status,
        warnings=warnings,
    )
