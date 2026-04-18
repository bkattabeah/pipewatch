"""Periodic digest summaries of pipeline health."""
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.aggregator import aggregate, AggregateStats


@dataclass
class DigestConfig:
    title: str = "Pipeline Health Digest"
    include_healthy: bool = False
    top_n_worst: int = 5


@dataclass
class DigestEntry:
    pipeline_id: str
    status: str
    error_rate: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline_id": self.pipeline_id,
            "status": self.status,
            "error_rate": round(self.error_rate, 4),
        }


@dataclass
class Digest:
    title: str
    generated_at: str
    stats: AggregateStats
    entries: List[DigestEntry] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "title": self.title,
            "generated_at": self.generated_at,
            "stats": self.stats.to_dict(),
            "entries": [e.to_dict() for e in self.entries],
        }


def build_digest(metrics: List[PipelineMetric], config: DigestConfig = None) -> Digest:
    if config is None:
        config = DigestConfig()

    stats = aggregate(metrics)

    filtered = [
        m for m in metrics
        if config.include_healthy or m.status != PipelineStatus.HEALTHY
    ]

    from pipewatch.metrics import error_rate as calc_error_rate
    sorted_metrics = sorted(filtered, key=lambda m: calc_error_rate(m), reverse=True)
    top = sorted_metrics[: config.top_n_worst]

    entries = [
        DigestEntry(
            pipeline_id=m.pipeline_id,
            status=m.status.value,
            error_rate=calc_error_rate(m),
        )
        for m in top
    ]

    return Digest(
        title=config.title,
        generated_at=datetime.utcnow().isoformat(),
        stats=stats,
        entries=entries,
    )
