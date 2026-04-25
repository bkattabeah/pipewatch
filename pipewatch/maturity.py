"""Pipeline maturity scoring based on stability, error rate history, and alert frequency."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric, PipelineStatus


@dataclass
class MaturityConfig:
    min_samples: int = 10
    stable_window: int = 20
    max_critical_rate: float = 0.05
    max_warning_rate: float = 0.20

    def validate(self) -> None:
        if self.min_samples < 1:
            raise ValueError("min_samples must be >= 1")
        if self.stable_window < self.min_samples:
            raise ValueError("stable_window must be >= min_samples")
        if not (0.0 <= self.max_critical_rate <= 1.0):
            raise ValueError("max_critical_rate must be in [0, 1]")
        if not (0.0 <= self.max_warning_rate <= 1.0):
            raise ValueError("max_warning_rate must be in [0, 1]")
        if self.max_critical_rate > self.max_warning_rate:
            raise ValueError("max_critical_rate must be <= max_warning_rate")


@dataclass
class MaturityResult:
    pipeline: str
    score: float  # 0.0 – 1.0
    grade: str
    sample_count: int
    critical_ratio: float
    warning_ratio: float
    stable: bool

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "score": round(self.score, 4),
            "grade": self.grade,
            "sample_count": self.sample_count,
            "critical_ratio": round(self.critical_ratio, 4),
            "warning_ratio": round(self.warning_ratio, 4),
            "stable": self.stable,
        }


def _grade(score: float) -> str:
    if score >= 0.90:
        return "A"
    if score >= 0.75:
        return "B"
    if score >= 0.55:
        return "C"
    if score >= 0.35:
        return "D"
    return "F"


def compute_maturity(
    pipeline: str,
    metrics: List[PipelineMetric],
    config: Optional[MaturityConfig] = None,
) -> Optional[MaturityResult]:
    """Return a MaturityResult for *pipeline* or None if too few samples."""
    cfg = config or MaturityConfig()
    cfg.validate()

    if len(metrics) < cfg.min_samples:
        return None

    window = metrics[-cfg.stable_window :]
    n = len(window)

    critical_count = sum(1 for m in window if m.status == PipelineStatus.CRITICAL)
    warning_count = sum(1 for m in window if m.status == PipelineStatus.WARNING)

    critical_ratio = critical_count / n
    warning_ratio = warning_count / n

    stable = critical_ratio <= cfg.max_critical_rate and warning_ratio <= cfg.max_warning_rate

    score = max(0.0, 1.0 - (critical_ratio * 2.0) - (warning_ratio * 0.5))
    score = min(1.0, score)

    return MaturityResult(
        pipeline=pipeline,
        score=score,
        grade=_grade(score),
        sample_count=n,
        critical_ratio=critical_ratio,
        warning_ratio=warning_ratio,
        stable=stable,
    )
