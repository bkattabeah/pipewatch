"""Stale pipeline report: summarize watchdog stale entries into a structured report."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.watchdog import StaleEntry


@dataclass
class StaleReport:
    total_stale: int
    pipelines: List[StaleEntry]
    most_overdue: Optional[StaleEntry]

    def has_stale(self) -> bool:
        return self.total_stale > 0

    def to_dict(self) -> dict:
        return {
            "total_stale": self.total_stale,
            "has_stale": self.has_stale(),
            "most_overdue": self.most_overdue.to_dict() if self.most_overdue else None,
            "pipelines": [e.to_dict() for e in self.pipelines],
        }


def _most_overdue(entries: List[StaleEntry]) -> Optional[StaleEntry]:
    """Return the entry with the greatest staleness (largest age_seconds)."""
    if not entries:
        return None
    return max(entries, key=lambda e: e.age_seconds)


def build_stale_report(entries: List[StaleEntry]) -> StaleReport:
    """Build a StaleReport from a list of StaleEntry objects."""
    return StaleReport(
        total_stale=len(entries),
        pipelines=list(entries),
        most_overdue=_most_overdue(entries),
    )


def format_stale_report(report: StaleReport) -> str:
    """Return a human-readable summary of the stale report."""
    if not report.has_stale():
        return "No stale pipelines detected."

    lines = [f"Stale pipelines: {report.total_stale}"]
    if report.most_overdue:
        mo = report.most_overdue
        lines.append(
            f"  Most overdue : {mo.pipeline_name} ({mo.age_seconds:.1f}s since last update)"
        )
    lines.append("")
    lines.append(f"  {'Pipeline':<30} {'Age (s)':>10}  Reason")
    lines.append("  " + "-" * 55)
    for entry in sorted(report.pipelines, key=lambda e: -e.age_seconds):
        lines.append(
            f"  {entry.pipeline_name:<30} {entry.age_seconds:>10.1f}  {entry.reason}"
        )
    return "\n".join(lines)
