"""Sliding window aggregation over pipeline metrics."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional

from pipewatch.metrics import PipelineMetric, error_rate


@dataclass
class WindowConfig:
    size_seconds: int = 300  # 5-minute default
    min_samples: int = 1

    def validate(self) -> None:
        if self.size_seconds <= 0:
            raise ValueError("size_seconds must be positive")
        if self.min_samples < 1:
            raise ValueError("min_samples must be at least 1")


@dataclass
class WindowStats:
    pipeline: str
    window_seconds: int
    sample_count: int
    avg_error_rate: float
    max_error_rate: float
    min_error_rate: float
    total_processed: int
    total_failed: int
    start: Optional[datetime] = None
    end: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "window_seconds": self.window_seconds,
            "sample_count": self.sample_count,
            "avg_error_rate": round(self.avg_error_rate, 4),
            "max_error_rate": round(self.max_error_rate, 4),
            "min_error_rate": round(self.min_error_rate, 4),
            "total_processed": self.total_processed,
            "total_failed": self.total_failed,
            "start": self.start.isoformat() if self.start else None,
            "end": self.end.isoformat() if self.end else None,
        }


def compute_window(
    pipeline: str,
    metrics: List[PipelineMetric],
    config: Optional[WindowConfig] = None,
) -> Optional[WindowStats]:
    """Compute sliding window stats for a single pipeline."""
    cfg = config or WindowConfig()
    cfg.validate()

    cutoff = datetime.utcnow() - timedelta(seconds=cfg.size_seconds)
    window = [m for m in metrics if m.pipeline == pipeline and m.timestamp >= cutoff]

    if len(window) < cfg.min_samples:
        return None

    rates = [error_rate(m) for m in window]
    timestamps = [m.timestamp for m in window]

    return WindowStats(
        pipeline=pipeline,
        window_seconds=cfg.size_seconds,
        sample_count=len(window),
        avg_error_rate=sum(rates) / len(rates),
        max_error_rate=max(rates),
        min_error_rate=min(rates),
        total_processed=sum(m.processed for m in window),
        total_failed=sum(m.failed for m in window),
        start=min(timestamps),
        end=max(timestamps),
    )
