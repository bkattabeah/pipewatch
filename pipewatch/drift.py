"""Drift detection: compare current metrics against a stable baseline window."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric, PipelineStatus


@dataclass
class DriftConfig:
    window: int = 10          # number of historical samples to use as baseline
    threshold: float = 0.15   # minimum error-rate delta to flag as drift

    def validate(self) -> None:
        if self.window < 2:
            raise ValueError("window must be >= 2")
        if not (0.0 < self.threshold <= 1.0):
            raise ValueError("threshold must be in (0, 1]")


@dataclass
class DriftResult:
    pipeline: str
    baseline_error_rate: float
    current_error_rate: float
    delta: float
    drifted: bool
    baseline_samples: int

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "baseline_error_rate": round(self.baseline_error_rate, 4),
            "current_error_rate": round(self.current_error_rate, 4),
            "delta": round(self.delta, 4),
            "drifted": self.drifted,
            "baseline_samples": self.baseline_samples,
        }


def _mean(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def detect_drift(
    history: List[PipelineMetric],
    current: PipelineMetric,
    config: Optional[DriftConfig] = None,
) -> DriftResult:
    """Compare *current* metric against the tail of *history*.

    The most recent entry in *history* is excluded so that *current* is always
    compared against prior observations only.
    """
    if config is None:
        config = DriftConfig()
    config.validate()

    baseline_window = history[-config.window :] if history else []
    baseline_rates = [
        m.failed / m.total if m.total > 0 else 0.0 for m in baseline_window
    ]
    baseline_mean = _mean(baseline_rates)
    current_rate = current.failed / current.total if current.total > 0 else 0.0
    delta = abs(current_rate - baseline_mean)

    return DriftResult(
        pipeline=current.pipeline,
        baseline_error_rate=baseline_mean,
        current_error_rate=current_rate,
        delta=delta,
        drifted=delta >= config.threshold,
        baseline_samples=len(baseline_window),
    )


def detect_drift_many(
    history: List[PipelineMetric],
    current_metrics: List[PipelineMetric],
    config: Optional[DriftConfig] = None,
) -> List[DriftResult]:
    """Run drift detection for each pipeline in *current_metrics*."""
    by_pipeline: dict[str, List[PipelineMetric]] = {}
    for m in history:
        by_pipeline.setdefault(m.pipeline, []).append(m)

    results = []
    for metric in current_metrics:
        pipe_history = by_pipeline.get(metric.pipeline, [])
        results.append(detect_drift(pipe_history, metric, config))
    return results
