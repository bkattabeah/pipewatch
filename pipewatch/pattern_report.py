"""Aggregate pattern detection results across all pipelines."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from pipewatch.metrics import PipelineMetric
from pipewatch.pattern import PatternConfig, PatternResult, detect_pattern


@dataclass
class PatternReport:
    results: List[PatternResult] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def recurring_count(self) -> int:
        return sum(1 for r in self.results if r.has_pattern)

    @property
    def clean_count(self) -> int:
        return self.total - self.recurring_count

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "recurring_count": self.recurring_count,
            "clean_count": self.clean_count,
            "results": [r.to_dict() for r in self.results],
        }


def build_pattern_report(
    metrics_by_pipeline: Dict[str, List[PipelineMetric]],
    config: PatternConfig | None = None,
) -> PatternReport:
    """Run pattern detection across all pipelines and return a combined report."""
    cfg = config or PatternConfig()
    cfg.validate()
    results: List[PatternResult] = []
    for pipeline, metrics in metrics_by_pipeline.items():
        result = detect_pattern(pipeline, metrics, cfg)
        if result is not None:
            results.append(result)
    return PatternReport(results=results)


def format_pattern_report(report: PatternReport) -> str:
    """Return a human-readable summary of the pattern report."""
    lines = [
        f"Pattern Report  total={report.total}  "
        f"recurring={report.recurring_count}  clean={report.clean_count}",
        "-" * 60,
    ]
    for r in sorted(report.results, key=lambda x: x.has_pattern, reverse=True):
        flag = "[PATTERN]" if r.has_pattern else "[ok]     "
        for m in r.matches:
            lines.append(
                f"{flag} {r.pipeline:<30} "
                f"occ={m.occurrences}  err={m.avg_error_rate:.2%}"
            )
    return "\n".join(lines)
