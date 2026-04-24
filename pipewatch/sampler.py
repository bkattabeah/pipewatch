"""Metric sampling — periodically sample pipeline metrics and store them for analysis."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class SampleEntry:
    pipeline_name: str
    sampled_at: datetime
    error_rate: float
    total_records: int
    failed_records: int

    def to_dict(self) -> dict:
        return {
            "pipeline_name": self.pipeline_name,
            "sampled_at": self.sampled_at.isoformat(),
            "error_rate": self.error_rate,
            "total_records": self.total_records,
            "failed_records": self.failed_records,
        }


@dataclass
class SamplerConfig:
    max_samples: int = 100
    pipelines: Optional[List[str]] = None  # None means sample all

    def validate(self) -> None:
        if self.max_samples < 1:
            raise ValueError("max_samples must be at least 1")


class MetricSampler:
    """Collects periodic samples of pipeline metrics."""

    def __init__(self, config: Optional[SamplerConfig] = None) -> None:
        self._config = config or SamplerConfig()
        self._config.validate()
        self._samples: List[SampleEntry] = []

    def sample(self, metric: PipelineMetric) -> SampleEntry:
        """Record a sample from a PipelineMetric and return the entry."""
        if (
            self._config.pipelines is not None
            and metric.pipeline_name not in self._config.pipelines
        ):
            raise ValueError(
                f"Pipeline '{metric.pipeline_name}' is not in the sampler's watch list."
            )

        entry = SampleEntry(
            pipeline_name=metric.pipeline_name,
            sampled_at=metric.timestamp,
            error_rate=metric.error_rate,
            total_records=metric.total_records,
            failed_records=metric.failed_records,
        )
        self._samples.append(entry)
        # Trim to max_samples (keep most recent)
        if len(self._samples) > self._config.max_samples:
            self._samples = self._samples[-self._config.max_samples :]
        return entry

    def get_samples(self, pipeline_name: Optional[str] = None) -> List[SampleEntry]:
        """Return all samples, optionally filtered by pipeline name."""
        if pipeline_name is None:
            return list(self._samples)
        return [s for s in self._samples if s.pipeline_name == pipeline_name]

    def clear(self) -> None:
        """Remove all stored samples."""
        self._samples.clear()

    def __len__(self) -> int:
        return len(self._samples)
