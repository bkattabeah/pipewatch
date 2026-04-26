"""Spillover detection: identifies pipelines whose error rates exceed
a rolling capacity threshold, signalling work is spilling over budget."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric, PipelineStatus


@dataclass
class SpilloverConfig:
    window: int = 10          # number of recent metrics to consider
    threshold: float = 0.25   # error-rate above which spillover is flagged
    min_samples: int = 3      # minimum samples required before flagging

    def validate(self) -> None:
        if self.window < 1:
            raise ValueError("window must be >= 1")
        if not (0.0 <= self.threshold <= 1.0):
            raise ValueError("threshold must be between 0.0 and 1.0")
        if self.min_samples < 1:
            raise ValueError("min_samples must be >= 1")


@dataclass
class SpilloverResult:
    pipeline: str
    avg_error_rate: float
    sample_count: int
    threshold: float
    spilling: bool

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "avg_error_rate": round(self.avg_error_rate, 4),
            "sample_count": self.sample_count,
            "threshold": self.threshold,
            "spilling": self.spilling,
        }


def _mean(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def detect_spillover(
    metrics: List[PipelineMetric],
    config: Optional[SpilloverConfig] = None,
) -> List[SpilloverResult]:
    """Analyse *metrics* and return one SpilloverResult per pipeline."""
    if config is None:
        config = SpilloverConfig()
    config.validate()

    # group by pipeline, keep only the most recent *window* entries
    groups: dict[str, List[PipelineMetric]] = {}
    for m in metrics:
        groups.setdefault(m.pipeline, []).append(m)

    results: List[SpilloverResult] = []
    for pipeline, entries in groups.items():
        recent = sorted(entries, key=lambda m: m.timestamp)[-config.window :]
        if len(recent) < config.min_samples:
            continue
        rates = [
            m.failed / m.total if m.total > 0 else 0.0 for m in recent
        ]
        avg = _mean(rates)
        results.append(
            SpilloverResult(
                pipeline=pipeline,
                avg_error_rate=avg,
                sample_count=len(recent),
                threshold=config.threshold,
                spilling=avg > config.threshold,
            )
        )
    return results
