"""Tests for pipewatch.aggregator."""
import pytest
from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.aggregator import aggregate, group_by_status


def make_metric(pipeline_id: str, processed: int, failed: int) -> PipelineMetric:
    return PipelineMetric(
        pipeline_id=pipeline_id,
        processed=processed,
        failed=failed,
        latency_ms=100.0,
    )


class TestAggregate:
    def test_empty_returns_zero_stats(self):
        stats = aggregate([])
        assert stats.total == 0
        assert stats.avg_error_rate == 0.0

    def test_total_count(self):
        metrics = [make_metric(f"p{i}", 100, 0) for i in range(5)]
        stats = aggregate(metrics)
        assert stats.total == 5

    def test_all_healthy(self):
        metrics = [make_metric(f"p{i}", 100, 0) for i in range(3)]
        stats = aggregate(metrics)
        assert stats.healthy == 3
        assert stats.warning == 0
        assert stats.critical == 0

    def test_avg_error_rate(self):
        m1 = make_metric("a", 100, 10)  # 0.10
        m2 = make_metric("b", 100, 20)  # 0.20
        stats = aggregate([m1, m2])
        assert abs(stats.avg_error_rate - 0.15) < 1e-6

    def test_max_error_rate(self):
        m1 = make_metric("a", 100, 5)
        m2 = make_metric("b", 100, 50)
        stats = aggregate([m1, m2])
        assert abs(stats.max_error_rate - 0.50) < 1e-6

    def test_pipeline_names_captured(self):
        metrics = [make_metric("alpha", 10, 0), make_metric("beta", 10, 0)]
        stats = aggregate(metrics)
        assert "alpha" in stats.pipeline_names
        assert "beta" in stats.pipeline_names

    def test_to_dict_keys(self):
        stats = aggregate([make_metric("x", 100, 1)])
        d = stats.to_dict()
        for key in ("total", "healthy", "warning", "critical", "unknown", "avg_error_rate", "max_error_rate"):
            assert key in d


class TestGroupByStatus:
    def test_groups_by_status(self):
        healthy = make_metric("ok", 100, 0)
        groups = group_by_status([healthy])
        assert "healthy" in groups
        assert groups["healthy"][0].pipeline_id == "ok"

    def test_multiple_statuses(self):
        m_healthy = make_metric("h", 100, 0)
        m_critical = make_metric("c", 100, 90)
        groups = group_by_status([m_healthy, m_critical])
        assert "healthy" in groups
        assert "critical" in groups

    def test_empty_input(self):
        groups = group_by_status([])
        assert groups == {}
