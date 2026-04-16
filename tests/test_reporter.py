"""Tests for pipewatch.reporter."""

from datetime import datetime

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.collector import MetricCollector
from pipewatch.alerts import AlertEngine, AlertRule
from pipewatch.reporter import Reporter, PipelineReport


def make_metric(pipeline_id="pipe-1", total=100, failed=0, latency=0.5):
    return PipelineMetric(
        pipeline_id=pipeline_id,
        timestamp=datetime.utcnow(),
        total_records=total,
        failed_records=failed,
        avg_latency_ms=latency,
    )


def make_reporter(rules=None):
    collector = MetricCollector()
    if rules is None:
        rules = [
            AlertRule(name="warn", threshold=0.1, status=PipelineStatus.WARNING),
            AlertRule(name="crit", threshold=0.25, status=PipelineStatus.CRITICAL),
        ]
    engine = AlertEngine(rules=rules)
    return Reporter(collector=collector, alert_engine=engine), collector


class TestReporter:
    def test_report_unknown_for_missing_pipeline(self):
        reporter, _ = make_reporter()
        report = reporter.generate("nonexistent")
        assert report.status == PipelineStatus.UNKNOWN
        assert report.total_records == 0
        assert report.alert_count == 0

    def test_report_healthy_pipeline(self):
        reporter, collector = make_reporter()
        metric = make_metric(total=200, failed=0)
        collector.record(metric)
        report = reporter.generate("pipe-1")
        assert report.status == PipelineStatus.HEALTHY
        assert report.error_rate == 0.0
        assert report.alert_count == 0
        assert report.total_records == 200

    def test_report_warning_on_high_error_rate(self):
        reporter, collector = make_reporter()
        metric = make_metric(total=100, failed=15)
        collector.record(metric)
        report = reporter.generate("pipe-1")
        assert report.alert_count >= 1
        assert any("warn" in str(a).lower() or "error" in str(a).lower() for a in report.alerts)

    def test_report_to_dict_has_required_keys(self):
        reporter, collector = make_reporter()
        collector.record(make_metric())
        report = reporter.generate("pipe-1")
        d = report.to_dict()
        for key in ("pipeline_id", "generated_at", "status", "total_records", "error_rate", "alert_count"):
            assert key in d

    def test_report_summary_is_string(self):
        reporter, collector = make_reporter()
        collector.record(make_metric(total=50, failed=5))
        report = reporter.generate("pipe-1")
        summary = report.summary()
        assert isinstance(summary, str)
        assert "pipe-1" in summary
        assert "error" in summary.lower()
