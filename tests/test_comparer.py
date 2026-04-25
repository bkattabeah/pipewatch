"""Tests for pipewatch.comparer."""

import pytest
from datetime import datetime

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.comparer import compare_metrics, CompareResult, MetricComparison


def make_metric(
    pipeline_id: str,
    status: PipelineStatus = PipelineStatus.HEALTHY,
    error_rate: float = 0.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_id=pipeline_id,
        status=status,
        total_records=100,
        failed_records=int(error_rate * 100),
        error_rate=error_rate,
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
    )


class TestCompareMetrics:
    def test_identical_lists_no_changes(self):
        left = [make_metric("p1"), make_metric("p2")]
        right = [make_metric("p1"), make_metric("p2")]
        result = compare_metrics(left, right)
        assert len(result.changed) == 0
        assert len(result.added) == 0
        assert len(result.removed) == 0

    def test_detects_status_change(self):
        left = [make_metric("p1", PipelineStatus.HEALTHY, 0.0)]
        right = [make_metric("p1", PipelineStatus.CRITICAL, 0.9)]
        result = compare_metrics(left, right)
        assert len(result.changed) == 1
        assert result.changed[0].pipeline_id == "p1"

    def test_detects_added_pipeline(self):
        left = [make_metric("p1")]
        right = [make_metric("p1"), make_metric("p2")]
        result = compare_metrics(left, right)
        assert len(result.added) == 1
        assert result.added[0].pipeline_id == "p2"

    def test_detects_removed_pipeline(self):
        left = [make_metric("p1"), make_metric("p2")]
        right = [make_metric("p1")]
        result = compare_metrics(left, right)
        assert len(result.removed) == 1
        assert result.removed[0].pipeline_id == "p2"

    def test_error_rate_delta_computed(self):
        left = [make_metric("p1", PipelineStatus.WARNING, 0.1)]
        right = [make_metric("p1", PipelineStatus.CRITICAL, 0.4)]
        result = compare_metrics(left, right)
        cmp = result.comparisons[0]
        assert cmp.error_rate_delta == pytest.approx(0.3)

    def test_error_rate_delta_none_for_added(self):
        left: list = []
        right = [make_metric("p1", PipelineStatus.HEALTHY, 0.0)]
        result = compare_metrics(left, right)
        assert result.comparisons[0].error_rate_delta is None

    def test_to_dict_keys(self):
        result = compare_metrics(
            [make_metric("p1")],
            [make_metric("p1", PipelineStatus.WARNING, 0.2)],
        )
        d = result.to_dict()
        assert "total" in d
        assert "changed" in d
        assert "added" in d
        assert "removed" in d
        assert "comparisons" in d

    def test_empty_both_sides(self):
        result = compare_metrics([], [])
        assert result.to_dict()["total"] == 0

    def test_comparison_only_in_left(self):
        left = [make_metric("p1")]
        right: list = []
        result = compare_metrics(left, right)
        assert result.comparisons[0].only_in_left
        assert not result.comparisons[0].only_in_right

    def test_comparison_only_in_right(self):
        left: list = []
        right = [make_metric("p1")]
        result = compare_metrics(left, right)
        assert result.comparisons[0].only_in_right
        assert not result.comparisons[0].only_in_left
