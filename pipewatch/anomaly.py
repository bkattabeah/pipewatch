"""Anomaly detection for pipeline metrics using z-score analysis."""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class AnomalyConfig:
    z_score_threshold: float = 2.5
    min_samples: int = 5

    def validate(self) -> None:
        if self.z_score_threshold <= 0:
            raise ValueError("z_score_threshold must be positive")
        if self.min_samples < 2:
            raise ValueError("min_samples must be at least 2")


@dataclass
class AnomalyResult:
    pipeline: str
    current_error_rate: float
    mean: float
    std_dev: float
    z_score: float
    is_anomaly: bool

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "current_error_rate": round(self.current_error_rate, 4),
            "mean": round(self.mean, 4),
            "std_dev": round(self.std_dev, 4),
            "z_score": round(self.z_score, 4),
            "is_anomaly": self.is_anomaly,
        }


def _mean(values: List[float]) -> float:
    return sum(values) / len(values)


def _std_dev(values: List[float], mean: float) -> float:
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(variance)


def detect_anomaly(
    pipeline: str,
    history: List[PipelineMetric],
    config: Optional[AnomalyConfig] = None,
) -> Optional[AnomalyResult]:
    """Detect if the latest metric is anomalous relative to its history."""
    if config is None:
        config = AnomalyConfig()

    if len(history) < config.min_samples:
        return None

    from pipewatch.metrics import error_rate

    rates = [error_rate(m) for m in history]
    current = rates[-1]
    historical = rates[:-1]

    if not historical:
        return None

    mu = _mean(historical)
    sigma = _std_dev(historical, mu)

    if sigma == 0:
        z = 0.0
    else:
        z = (current - mu) / sigma

    return AnomalyResult(
        pipeline=pipeline,
        current_error_rate=current,
        mean=mu,
        std_dev=sigma,
        z_score=z,
        is_anomaly=abs(z) >= config.z_score_threshold,
    )


def detect_anomalies(
    history_map: dict,
    config: Optional[AnomalyConfig] = None,
) -> List[AnomalyResult]:
    """Run anomaly detection across multiple pipelines."""
    results = []
    for pipeline, metrics in history_map.items():
        result = detect_anomaly(pipeline, metrics, config)
        if result is not None:
            results.append(result)
    return results
