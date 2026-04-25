"""Compare two sets of pipeline metrics and report differences in health."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric, PipelineStatus


@dataclass
class MetricComparison:
    pipeline_id: str
    left_status: Optional[PipelineStatus]
    right_status: Optional[PipelineStatus]
    left_error_rate: Optional[float]
    right_error_rate: Optional[float]

    @property
    def status_changed(self) -> bool:
        return self.left_status != self.right_status

    @property
    def error_rate_delta(self) -> Optional[float]:
        if self.left_error_rate is None or self.right_error_rate is None:
            return None
        return self.right_error_rate - self.left_error_rate

    @property
    def only_in_left(self) -> bool:
        return self.right_status is None

    @property
    def only_in_right(self) -> bool:
        return self.left_status is None

    def to_dict(self) -> dict:
        return {
            "pipeline_id": self.pipeline_id,
            "left_status": self.left_status.value if self.left_status else None,
            "right_status": self.right_status.value if self.right_status else None,
            "left_error_rate": self.left_error_rate,
            "right_error_rate": self.right_error_rate,
            "status_changed": self.status_changed,
            "error_rate_delta": self.error_rate_delta,
        }


@dataclass
class CompareResult:
    comparisons: List[MetricComparison] = field(default_factory=list)

    @property
    def changed(self) -> List[MetricComparison]:
        return [c for c in self.comparisons if c.status_changed]

    @property
    def added(self) -> List[MetricComparison]:
        return [c for c in self.comparisons if c.only_in_right]

    @property
    def removed(self) -> List[MetricComparison]:
        return [c for c in self.comparisons if c.only_in_left]

    def to_dict(self) -> dict:
        return {
            "total": len(self.comparisons),
            "changed": len(self.changed),
            "added": len(self.added),
            "removed": len(self.removed),
            "comparisons": [c.to_dict() for c in self.comparisons],
        }


def compare_metrics(
    left: List[PipelineMetric],
    right: List[PipelineMetric],
) -> CompareResult:
    """Compare two lists of PipelineMetric by pipeline_id."""
    left_map: Dict[str, PipelineMetric] = {m.pipeline_id: m for m in left}
    right_map: Dict[str, PipelineMetric] = {m.pipeline_id: m for m in right}
    all_ids = sorted(set(left_map) | set(right_map))

    comparisons = []
    for pid in all_ids:
        lm = left_map.get(pid)
        rm = right_map.get(pid)
        comparisons.append(
            MetricComparison(
                pipeline_id=pid,
                left_status=lm.status if lm else None,
                right_status=rm.status if rm else None,
                left_error_rate=lm.error_rate if lm else None,
                right_error_rate=rm.error_rate if rm else None,
            )
        )
    return CompareResult(comparisons=comparisons)
