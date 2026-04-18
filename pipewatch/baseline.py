"""Baseline comparison: compare current metrics against a saved baseline snapshot."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric, error_rate
from pipewatch.snapshot import Snapshot


@dataclass
class BaselineEntry:
    pipeline: str
    baseline_error_rate: float
    current_error_rate: float
    delta: float
    regressed: bool

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "baseline_error_rate": round(self.baseline_error_rate, 4),
            "current_error_rate": round(self.current_error_rate, 4),
            "delta": round(self.delta, 4),
            "regressed": self.regressed,
        }


@dataclass
class BaselineReport:
    entries: List[BaselineEntry] = field(default_factory=list)
    missing_in_baseline: List[str] = field(default_factory=list)
    missing_in_current: List[str] = field(default_factory=list)

    def any_regressions(self) -> bool:
        return any(e.regressed for e in self.entries)

    def to_dict(self) -> dict:
        return {
            "entries": [e.to_dict() for e in self.entries],
            "missing_in_baseline": self.missing_in_baseline,
            "missing_in_current": self.missing_in_current,
            "any_regressions": self.any_regressions(),
        }


def compare_to_baseline(
    baseline: Snapshot,
    current: Snapshot,
    regression_threshold: float = 0.05,
) -> BaselineReport:
    """Compare current snapshot metrics against a baseline snapshot."""
    base_map: Dict[str, PipelineMetric] = {m.pipeline: m for m in baseline.metrics}
    curr_map: Dict[str, PipelineMetric] = {m.pipeline: m for m in current.metrics}

    report = BaselineReport()
    report.missing_in_baseline = [p for p in curr_map if p not in base_map]
    report.missing_in_current = [p for p in base_map if p not in curr_map]

    for pipeline, curr_metric in curr_map.items():
        if pipeline not in base_map:
            continue
        base_metric = base_map[pipeline]
        base_er = error_rate(base_metric)
        curr_er = error_rate(curr_metric)
        delta = curr_er - base_er
        report.entries.append(
            BaselineEntry(
                pipeline=pipeline,
                baseline_error_rate=base_er,
                current_error_rate=curr_er,
                delta=delta,
                regressed=delta > regression_threshold,
            )
        )

    return report
