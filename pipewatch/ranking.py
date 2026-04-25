"""Pipeline ranking — score and rank pipelines by overall health risk."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric, PipelineStatus, error_rate


@dataclass
class RankEntry:
    pipeline_name: str
    score: float  # higher = worse / more at-risk
    status: PipelineStatus
    error_rate: float
    rank: int = 0

    def to_dict(self) -> dict:
        return {
            "rank": self.rank,
            "pipeline_name": self.pipeline_name,
            "score": round(self.score, 4),
            "status": self.status.value,
            "error_rate": round(self.error_rate, 4),
        }


@dataclass
class RankingResult:
    entries: List[RankEntry] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {"rankings": [e.to_dict() for e in self.entries]}

    def top(self, n: int = 5) -> List[RankEntry]:
        return self.entries[:n]


_STATUS_WEIGHT = {
    PipelineStatus.CRITICAL: 1.0,
    PipelineStatus.WARNING: 0.5,
    PipelineStatus.HEALTHY: 0.0,
    PipelineStatus.UNKNOWN: 0.2,
}


def _score_metric(metric: PipelineMetric) -> float:
    """Compute a risk score for a single metric (0.0 – 2.0 range)."""
    er = error_rate(metric)
    status_weight = _STATUS_WEIGHT.get(metric.status, 0.2)
    return er + status_weight


def rank_pipelines(
    metrics: List[PipelineMetric],
    descending: bool = True,
) -> RankingResult:
    """Rank pipelines from most-at-risk to least-at-risk."""
    if not metrics:
        return RankingResult()

    scored = [
        RankEntry(
            pipeline_name=m.pipeline_name,
            score=_score_metric(m),
            status=m.status,
            error_rate=error_rate(m),
        )
        for m in metrics
    ]

    scored.sort(key=lambda e: e.score, reverse=descending)

    for i, entry in enumerate(scored, start=1):
        entry.rank = i

    return RankingResult(entries=scored)
