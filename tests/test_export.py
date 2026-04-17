"""Tests for pipewatch.export module."""

import json
import csv
import io
from unittest.mock import MagicMock

import pytest

from pipewatch.export import export_json, export_csv, export_text
from pipewatch.metrics import PipelineMetric, PipelineStatus


def make_report(name: str, status: str = "healthy", error_rate: float = 0.0,
                total: int = 100, failed: int = 0, avg_latency: float = 50.0,
                sample_count: int = 5):
    """Create a mock pipeline report with sensible defaults."""
    report = MagicMock()
    report.pipeline_name = name
    report.to_dict.return_value = {
        "pipeline": name,
        "status": status,
        "error_rate": error_rate,
        "total_records": total,
        "failed_records": failed,
        "avg_latency_ms": avg_latency,
        "sample_count": sample_count,
    }
    return report


class TestExportJson:
    def test_returns_valid_json(self):
        reports = [make_report("pipe_a"), make_report("pipe_b")]
        result = export_json(reports)
        parsed = json.loads(result)
        assert len(parsed) == 2
        assert parsed[0]["pipeline"] == "pipe_a"

    def test_empty_list(self):
        result = export_json([])
        assert json.loads(result) == []

    def test_indent_respected(self):
        reports = [make_report("pipe_a")]
        result = export_json(reports, indent=4)
        assert "    " in result

    def test_all_fields_present(self):
        """Ensure every field from to_dict appears in the JSON output."""
        reports = [make_report("pipe_a", status="warning", error_rate=0.1,
                               total=200, failed=20, avg_latency=75.0, sample_count=10)]
        result = export_json(reports)
        parsed = json.loads(result)
        entry = parsed[0]
        assert entry["status"] == "warning"
        assert entry["error_rate"] == 0.1
        assert entry["total_records"] == 200
        assert entry["failed_records"] == 20
        assert entry["avg_latency_ms"] == 75.0
        assert entry["sample_count"] == 10


class TestExportCsv:
    def test_returns_csv_with_header(self):
        reports = [make_report("pipe_a", error_rate=0.05)]
        result = export_csv(reports)
        reader = csv.DictReader(io.StringIO(result))
        rows = list(reader)
        assert len(rows) == 1
        assert rows[0]["pipeline"] == "pipe_a"

    def test_empty_list_has_only_header(self):
        result = export_csv([])
        lines = [l for l in result.strip().splitlines() if l]
        assert len(lines) == 1  # header only

    def test_multiple_rows(self):
        reports = [make_report(f"pipe_{i}") for i in range(3)]
        result = export_csv(reports)
        reader = csv.DictReader(io.StringIO(result))
        assert len(list(reader)) == 3


class TestExportText:
    def test_empty_returns_message(self):
        result = export_text([])
        assert "No pipeline reports" in result

    def test_contains_pipeline_name(self):
        reports = [make_report("my_pipeline")]
        result = export_text(reports)
        assert "my_pipeline" in result

    def test_contains_status(self):
        reports = [make_report("pipe_a", status="warning")]
        result = export_text(reports)
        assert "warning" in result

    def test_multiple_pipelines(self):
        reports = [make_report(f"pipe_{i}") for i in range(4)]
        result = export_text(reports)
        for i in range(4):
            assert f"pipe_{i}" in result
