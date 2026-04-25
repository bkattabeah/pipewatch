"""Signal detection: identify pipelines emitting notable behavioral signals."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric, PipelineStatus


@dataclass
class SignalConfig:
    min_error_rate_spike: float = 0.10   # delta above baseline to flag a spike
    min_recovery_drop: float = 0.10      # delta below previous to flag recovery
    min_samples: int = 2

    def validate(self) -> None:
        if not 0.0 < self.min_error_rate_spike <= 1.0:
            raise ValueError("min_error_rate_spike must be in (0, 1]")
        if not 0.0 < self.min_recovery_drop <= 1.0:
            raise ValueError("min_recovery_drop must be in (0, 1]")
        if self.min_samples < 2:
            raise ValueError("min_samples must be >= 2")


@dataclass
class SignalResult:
    pipeline: str
    signal: str          # "spike", "recovery", "flapping", "stable"
    current_error_rate: float
    previous_error_rate: float
    delta: float
    note: str = ""

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "signal": self.signal,
            "current_error_rate": round(self.current_error_rate, 4),
            "previous_error_rate": round(self.previous_error_rate, 4),
            "delta": round(self.delta, 4),
            "note": self.note,
        }


def _error_rate(m: PipelineMetric) -> float:
    if m.total == 0:
        return 0.0
    return m.failed / m.total


def detect_signals(
    current: List[PipelineMetric],
    previous: List[PipelineMetric],
    config: Optional[SignalConfig] = None,
) -> List[SignalResult]:
    """Compare current metrics against previous snapshot and emit signals."""
    if config is None:
        config = SignalConfig()
    config.validate()

    prev_map = {m.pipeline: m for m in previous}
    results: List[SignalResult] = []

    for metric in current:
        prev = prev_map.get(metric.pipeline)
        if prev is None:
            continue

        cur_rate = _error_rate(metric)
        prv_rate = _error_rate(prev)
        delta = cur_rate - prv_rate

        if delta >= config.min_error_rate_spike:
            signal = "spike"
            note = f"Error rate rose by {delta:.1%}"
        elif delta <= -config.min_recovery_drop:
            signal = "recovery"
            note = f"Error rate dropped by {abs(delta):.1%}"
        elif metric.status != prev.status:
            signal = "flapping"
            note = f"Status changed: {prev.status.value} -> {metric.status.value}"
        else:
            signal = "stable"
            note = "No significant change"

        results.append(
            SignalResult(
                pipeline=metric.pipeline,
                signal=signal,
                current_error_rate=cur_rate,
                previous_error_rate=prv_rate,
                delta=delta,
                note=note,
            )
        )

    return results
