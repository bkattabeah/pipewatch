"""Core metrics data structures for pipeline health monitoring."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class PipelineStatus(str, Enum):
    OK = "ok"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


@dataclass
class PipelineMetric:
    pipeline_id: str
    records_processed: int
    records_failed: int
    throughput_per_sec: float
    latency_ms: float
    timestamp: datetime = field(default_factory=datetime.utcnow)
    error_message: Optional[str] = None

    @property
    def error_rate(self) -> float:
        total = self.records_processed + self.records_failed
        if total == 0:
            return 0.0
        return self.records_failed / total

    def evaluate_status(
        self,
        max_error_rate: float = 0.05,
        max_latency_ms: float = 1000.0,
        min_throughput: float = 1.0,
    ) -> PipelineStatus:
        if (
            self.error_rate > max_error_rate * 2
            or self.latency_ms > max_latency_ms * 2
        ):
            return PipelineStatus.CRITICAL
        if (
            self.error_rate > max_error_rate
            or self.latency_ms > max_latency_ms
            or self.throughput_per_sec < min_throughput
        ):
            return PipelineStatus.WARNING
        return PipelineStatus.OK

    def to_dict(self) -> dict:
        return {
            "pipeline_id": self.pipeline_id,
            "records_processed": self.records_processed,
            "records_failed": self.records_failed,
            "error_rate": round(self.error_rate, 4),
            "throughput_per_sec": self.throughput_per_sec,
            "latency_ms": self.latency_ms,
            "timestamp": self.timestamp.isoformat(),
            "error_message": self.error_message,
        }
