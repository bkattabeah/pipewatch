"""Tests for PipelineMetric and MetricCollector."""

from datetime import datetime, timedelta

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.collector import MetricCollector


def make_metric(pipeline_id="pipe-1", processed=100, failed=0, throughput=10.0, latency=200.0):
    return PipelineMetric(
        pipeline_id=pipeline_id,
        records_processed=processed,
        records_failed=failed,
        throughput_per_sec=throughput,
        latency_ms=latency,
    )


class TestPipelineMetric:
    def test_error_rate_zero_when_no_failures(self):
        m = make_metric(processed=100, failed=0)
        assert m.error_rate == 0.0

    def test_error_rate_calculation(self):
        m = make_metric(processed=90, failed=10)
        assert m.error_rate == pytest.approx(0.1)

    def test_error_rate_all_failed(self):
        m = make_metric(processed=0, failed=5)
        assert m.error_rate == 1.0

    def test_status_ok(self):
        m = make_metric(processed=100, failed=1, throughput=10.0, latency=200.0)
        assert m.evaluate_status() == PipelineStatus.OK

    def test_status_warning_high_latency(self):
        m = make_metric(latency=1500.0)
        assert m.evaluate_status() == PipelineStatus.WARNING

    def test_status_critical_high_error_rate(self):
        m = make_metric(processed=80, failed=20, latency=2500.0)
        assert m.evaluate_status() == PipelineStatus.CRITICAL

    def test_status_warning_low_throughput(self):
        m = make_metric(throughput=0.5)
        assert m.evaluate_status() == PipelineStatus.WARNING

    def test_to_dict_keys(self):
        m = make_metric()
        d = m.to_dict()
        assert "pipeline_id" in d
        assert "error_rate" in d
        assert "timestamp" in d


class TestMetricCollector:
    def test_record_and_latest(self):
        col = MetricCollector()
        m = make_metric()
        col.record(m)
        assert col.latest("pipe-1") is m

    def test_latest_returns_none_for_unknown(self):
        col = MetricCollector()
        assert col.latest("unknown") is None

    def test_window_size_respected(self):
        col = MetricCollector(window_size=3)
        for _ in range(5):
            col.record(make_metric())
        assert len(col.history("pipe-1")) == 3

    def test_history_since_filter(self):
        col = MetricCollector()
        old = make_metric()
        old.timestamp = datetime.utcnow() - timedelta(minutes=10)
        recent = make_metric()
        col.record(old)
        col.record(recent)
        cutoff = datetime.utcnow() - timedelta(minutes=1)
        result = col.history("pipe-1", since=cutoff)
        assert len(result) == 1

    def test_summary_contains_status(self):
        col = MetricCollector()
        col.record(make_metric())
        summary = col.summary("pipe-1")
        assert summary is not None
        assert "status" in summary
        assert summary["status"] == PipelineStatus.OK.value

    def test_pipeline_ids(self):
        col = MetricCollector()
        col.record(make_metric(pipeline_id="a"))
        col.record(make_metric(pipeline_id="b"))
        assert set(col.pipeline_ids()) == {"a", "b"}
