"""Tests for pipewatch.anomaly_report module."""
from __future__ import annotations

from pipewatch.anomaly import AnomalyResult
from pipewatch.anomaly_report import (
    AnomalyReport,
    build_anomaly_report,
    format_anomaly_report,
)


def make_result(pipeline: str, is_anomaly: bool, z: float = 1.0) -> AnomalyResult:
    return AnomalyResult(
        pipeline=pipeline,
        current_error_rate=0.1,
        mean=0.05,
        std_dev=0.02,
        z_score=z,
        is_anomaly=is_anomaly,
    )


class TestAnomalyReport:
    def test_has_anomalies_false_when_empty(self):
        report = AnomalyReport(total_pipelines=0, anomaly_count=0)
        assert report.has_anomalies is False

    def test_has_anomalies_true_when_count_positive(self):
        report = AnomalyReport(total_pipelines=3, anomaly_count=1)
        assert report.has_anomalies is True

    def test_to_dict_keys(self):
        report = AnomalyReport(total_pipelines=2, anomaly_count=1)
        d = report.to_dict()
        assert set(d.keys()) == {"total_pipelines", "anomaly_count", "has_anomalies", "entries"}

    def test_to_dict_entries_serialised(self):
        result = make_result("pipe_a", True, z=3.1)
        report = AnomalyReport(total_pipelines=1, anomaly_count=1, entries=[result])
        d = report.to_dict()
        assert len(d["entries"]) == 1
        assert d["entries"][0]["pipeline"] == "pipe_a"


class TestBuildAnomalyReport:
    def test_empty_list(self):
        report = build_anomaly_report([])
        assert report.total_pipelines == 0
        assert report.anomaly_count == 0
        assert report.has_anomalies is False

    def test_counts_anomalies_correctly(self):
        results = [
            make_result("a", True),
            make_result("b", False),
            make_result("c", True),
        ]
        report = build_anomaly_report(results)
        assert report.total_pipelines == 3
        assert report.anomaly_count == 2

    def test_all_entries_included(self):
        results = [make_result("a", True), make_result("b", False)]
        report = build_anomaly_report(results)
        assert len(report.entries) == 2


class TestFormatAnomalyReport:
    def test_header_present(self):
        report = build_anomaly_report([])
        text = format_anomaly_report(report)
        assert "Anomaly Report" in text

    def test_no_data_message_when_empty(self):
        report = build_anomaly_report([])
        text = format_anomaly_report(report)
        assert "(no data)" in text

    def test_anomaly_flag_shown(self):
        results = [make_result("pipe_x", True, z=3.5)]
        report = build_anomaly_report(results)
        text = format_anomaly_report(report)
        assert "[!]" in text
        assert "pipe_x" in text

    def test_non_anomaly_flag_shown(self):
        results = [make_result("pipe_y", False, z=0.8)]
        report = build_anomaly_report(results)
        text = format_anomaly_report(report)
        assert "[ ]" in text
