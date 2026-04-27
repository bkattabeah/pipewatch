"""Entropy analysis: measures variability/unpredictability of pipeline error rates."""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class EntropyConfig:
    min_samples: int = 5
    bucket_count: int = 10

    def validate(self) -> None:
        if self.min_samples < 2:
            raise ValueError("min_samples must be at least 2")
        if self.bucket_count < 2:
            raise ValueError("bucket_count must be at least 2")


@dataclass
class EntropyResult:
    pipeline: str
    sample_count: int
    entropy: float
    normalized_entropy: float  # 0.0 (uniform) to 1.0 (max variability)
    is_stable: bool
    label: str

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "sample_count": self.sample_count,
            "entropy": round(self.entropy, 4),
            "normalized_entropy": round(self.normalized_entropy, 4),
            "is_stable": self.is_stable,
            "label": self.label,
        }


def _shannon_entropy(values: List[float], bucket_count: int) -> float:
    """Compute Shannon entropy over bucketed float values in [0.0, 1.0]."""
    if not values:
        return 0.0
    counts = [0] * bucket_count
    for v in values:
        idx = min(int(v * bucket_count), bucket_count - 1)
        counts[idx] += 1
    total = len(values)
    entropy = 0.0
    for c in counts:
        if c > 0:
            p = c / total
            entropy -= p * math.log2(p)
    return entropy


def _label(normalized: float) -> str:
    if normalized < 0.25:
        return "stable"
    if normalized < 0.60:
        return "moderate"
    return "volatile"


def analyze_entropy(
    pipeline: str,
    metrics: List[PipelineMetric],
    config: Optional[EntropyConfig] = None,
) -> Optional[EntropyResult]:
    cfg = config or EntropyConfig()
    cfg.validate()

    if len(metrics) < cfg.min_samples:
        return None

    from pipewatch.metrics import error_rate as calc_error_rate

    rates = [calc_error_rate(m) for m in metrics]
    entropy = _shannon_entropy(rates, cfg.bucket_count)
    max_entropy = math.log2(cfg.bucket_count)
    normalized = entropy / max_entropy if max_entropy > 0 else 0.0
    stable = normalized < 0.25

    return EntropyResult(
        pipeline=pipeline,
        sample_count=len(metrics),
        entropy=entropy,
        normalized_entropy=normalized,
        is_stable=stable,
        label=_label(normalized),
    )
