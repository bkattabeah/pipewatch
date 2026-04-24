"""SLA (Service Level Agreement) tracking for pipeline error rates."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric, error_rate


@dataclass
class SLAConfig:
    max_error_rate: float = 0.05  # 5% allowed error rate
    window_minutes: int = 60
    min_samples: int = 5

    def validate(self) -> None:
        if not 0.0 <= self.max_error_rate <= 1.0:
            raise ValueError("max_error_rate must be between 0.0 and 1.0")
        if self.window_minutes <= 0:
            raise ValueError("window_minutes must be positive")
        if self.min_samples <= 0:
            raise ValueError("min_samples must be positive")


@dataclass
class SLAResult:
    pipeline: str
    compliant: bool
    error_rate: float
    max_error_rate: float
    sample_count: int
    checked_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    message: str = ""

    def to_dict(self) -> Dict:
        return {
            "pipeline": self.pipeline,
            "compliant": self.compliant,
            "error_rate": round(self.error_rate, 4),
            "max_error_rate": self.max_error_rate,
            "sample_count": self.sample_count,
            "checked_at": self.checked_at.isoformat(),
            "message": self.message,
        }


def check_sla(pipeline: str, metrics: List[PipelineMetric], config: SLAConfig) -> SLAResult:
    """Evaluate SLA compliance for a pipeline given a list of recent metrics."""
    if len(metrics) < config.min_samples:
        return SLAResult(
            pipeline=pipeline,
            compliant=True,
            error_rate=0.0,
            max_error_rate=config.max_error_rate,
            sample_count=len(metrics),
            message=f"Insufficient samples ({len(metrics)} < {config.min_samples}); SLA not evaluated.",
        )

    total_processed = sum(m.records_processed for m in metrics)
    total_failed = sum(m.records_failed for m in metrics)
    rate = total_failed / total_processed if total_processed > 0 else 0.0
    compliant = rate <= config.max_error_rate
    msg = "SLA met." if compliant else f"SLA breached: error rate {rate:.2%} exceeds {config.max_error_rate:.2%}."
    return SLAResult(
        pipeline=pipeline,
        compliant=compliant,
        error_rate=rate,
        max_error_rate=config.max_error_rate,
        sample_count=len(metrics),
        message=msg,
    )


def check_all_slas(
    collector_history: Dict[str, List[PipelineMetric]],
    config: SLAConfig,
) -> List[SLAResult]:
    """Run SLA checks across all tracked pipelines."""
    return [check_sla(pipeline, metrics, config) for pipeline, metrics in collector_history.items()]
