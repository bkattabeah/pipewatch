"""Capacity planning module: tracks metric throughput and estimates headroom."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class CapacityConfig:
    window_size: int = 60          # number of recent metrics to consider
    headroom_warn_pct: float = 0.75  # warn when utilisation >= 75 %
    headroom_crit_pct: float = 0.90  # critical when utilisation >= 90 %

    def validate(self) -> None:
        if self.window_size <= 0:
            raise ValueError("window_size must be positive")
        if not (0.0 < self.headroom_warn_pct < 1.0):
            raise ValueError("headroom_warn_pct must be between 0 and 1")
        if not (0.0 < self.headroom_crit_pct <= 1.0):
            raise ValueError("headroom_crit_pct must be between 0 and 1")
        if self.headroom_warn_pct >= self.headroom_crit_pct:
            raise ValueError("headroom_warn_pct must be less than headroom_crit_pct")


@dataclass
class CapacityResult:
    pipeline: str
    total_processed: int
    total_failed: int
    utilisation: float          # error_rate as a proxy for load pressure
    headroom: float             # 1.0 - utilisation
    status: str                 # "ok", "warn", "critical"
    sample_count: int

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "total_processed": self.total_processed,
            "total_failed": self.total_failed,
            "utilisation": round(self.utilisation, 4),
            "headroom": round(self.headroom, 4),
            "status": self.status,
            "sample_count": self.sample_count,
        }


def _status_for(utilisation: float, cfg: CapacityConfig) -> str:
    if utilisation >= cfg.headroom_crit_pct:
        return "critical"
    if utilisation >= cfg.headroom_warn_pct:
        return "warn"
    return "ok"


def compute_capacity(
    pipeline: str,
    metrics: List[PipelineMetric],
    cfg: Optional[CapacityConfig] = None,
) -> Optional[CapacityResult]:
    """Compute capacity result for *pipeline* from a list of its metrics."""
    cfg = cfg or CapacityConfig()
    relevant = [m for m in metrics if m.pipeline == pipeline]
    if not relevant:
        return None
    window = relevant[-cfg.window_size :]
    total_processed = sum(m.processed for m in window)
    total_failed = sum(m.failed for m in window)
    utilisation = total_failed / total_processed if total_processed else 0.0
    headroom = 1.0 - utilisation
    return CapacityResult(
        pipeline=pipeline,
        total_processed=total_processed,
        total_failed=total_failed,
        utilisation=utilisation,
        headroom=headroom,
        status=_status_for(utilisation, cfg),
        sample_count=len(window),
    )
