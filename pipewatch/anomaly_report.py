"""Summarise anomaly detection results into a structured report."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipewatch.anomaly import AnomalyResult


@dataclass
class AnomalyReport:
    total_pipelines: int
    anomaly_count: int
    entries: List[AnomalyResult] = field(default_factory=list)

    @property
    def has_anomalies(self) -> bool:
        return self.anomaly_count > 0

    def to_dict(self) -> dict:
        return {
            "total_pipelines": self.total_pipelines,
            "anomaly_count": self.anomaly_count,
            "has_anomalies": self.has_anomalies,
            "entries": [e.to_dict() for e in self.entries],
        }


def build_anomaly_report(results: List[AnomalyResult]) -> AnomalyReport:
    """Build an AnomalyReport from a list of AnomalyResult objects."""
    anomalies = [r for r in results if r.is_anomaly]
    return AnomalyReport(
        total_pipelines=len(results),
        anomaly_count=len(anomalies),
        entries=results,
    )


def format_anomaly_report(report: AnomalyReport) -> str:
    """Return a human-readable summary of the anomaly report."""
    lines = [
        f"Anomaly Report — {report.anomaly_count}/{report.total_pipelines} pipelines anomalous",
        "-" * 60,
    ]
    if not report.entries:
        lines.append("  (no data)")
        return "\n".join(lines)

    for entry in report.entries:
        flag = "[!]" if entry.is_anomaly else "[ ]"
        lines.append(
            f"  {flag} {entry.pipeline:<30} "
            f"z={entry.z_score:+.2f}  rate={entry.current_error_rate:.4f}"
        )
    return "\n".join(lines)
