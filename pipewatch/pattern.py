"""Pattern detection: identify recurring failure patterns across pipeline metrics."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric, PipelineStatus


@dataclass
class PatternConfig:
    min_occurrences: int = 3
    window_size: int = 20
    error_rate_threshold: float = 0.1

    def validate(self) -> None:
        if self.min_occurrences < 1:
            raise ValueError("min_occurrences must be >= 1")
        if self.window_size < 2:
            raise ValueError("window_size must be >= 2")
        if not (0.0 <= self.error_rate_threshold <= 1.0):
            raise ValueError("error_rate_threshold must be between 0.0 and 1.0")


@dataclass
class PatternMatch:
    pipeline: str
    occurrences: int
    avg_error_rate: float
    statuses: List[str]
    is_recurring: bool

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "occurrences": self.occurrences,
            "avg_error_rate": round(self.avg_error_rate, 4),
            "statuses": self.statuses,
            "is_recurring": self.is_recurring,
        }


@dataclass
class PatternResult:
    pipeline: str
    matches: List[PatternMatch] = field(default_factory=list)
    config: PatternConfig = field(default_factory=PatternConfig)

    @property
    def has_pattern(self) -> bool:
        return any(m.is_recurring for m in self.matches)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "has_pattern": self.has_pattern,
            "matches": [m.to_dict() for m in self.matches],
        }


def detect_pattern(
    pipeline: str,
    metrics: List[PipelineMetric],
    config: Optional[PatternConfig] = None,
) -> Optional[PatternResult]:
    """Detect recurring failure patterns for a single pipeline."""
    cfg = config or PatternConfig()
    cfg.validate()

    window = metrics[-cfg.window_size :] if len(metrics) > cfg.window_size else metrics
    if not window:
        return None

    failing = [
        m for m in window
        if m.status in (PipelineStatus.WARNING, PipelineStatus.CRITICAL)
        or m.error_rate >= cfg.error_rate_threshold
    ]

    occurrences = len(failing)
    avg_error_rate = (
        sum(m.error_rate for m in failing) / occurrences if occurrences else 0.0
    )
    statuses = [m.status.value for m in failing]
    is_recurring = occurrences >= cfg.min_occurrences

    match = PatternMatch(
        pipeline=pipeline,
        occurrences=occurrences,
        avg_error_rate=avg_error_rate,
        statuses=statuses,
        is_recurring=is_recurring,
    )
    return PatternResult(pipeline=pipeline, matches=[match], config=cfg)
