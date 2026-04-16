"""Export pipeline reports to various formats."""

from __future__ import annotations

import csv
import json
import io
from typing import List

from pipewatch.reporter import PipelineReport


def export_json(reports: List[PipelineReport], indent: int = 2) -> str:
    """Serialize a list of pipeline reports to a JSON string."""
    data = [r.to_dict() for r in reports]
    return json.dumps(data, indent=indent, default=str)


def export_csv(reports: List[PipelineReport]) -> str:
    """Serialize a list of pipeline reports to CSV."""
    fieldnames = [
        "pipeline",
        "status",
        "error_rate",
        "total_records",
        "failed_records",
        "avg_latency_ms",
        "sample_count",
    ]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for report in reports:
        row = report.to_dict()
        row["pipeline"] = report.pipeline_name
        writer.writerow(row)
    return output.getvalue()


def export_text(reports: List[PipelineReport]) -> str:
    """Serialize a list of pipeline reports to a human-readable text table."""
    if not reports:
        return "No pipeline reports available.\n"

    lines = [
        f"{'Pipeline':<25} {'Status':<10} {'Error Rate':>12} {'Records':>10} {'Avg Latency':>13}",
        "-" * 75,
    ]
    for r in reports:
        d = r.to_dict()
        lines.append(
            f"{r.pipeline_name:<25} {d.get('status', 'unknown'):<10}"
            f" {d.get('error_rate', 0.0):>11.2%}"
            f" {d.get('total_records', 0):>10}"
            f" {d.get('avg_latency_ms', 0.0):>12.1f}ms"
        )
    return "\n".join(lines) + "\n"
