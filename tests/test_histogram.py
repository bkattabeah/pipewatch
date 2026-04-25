"""Tests for pipewatch.histogram."""
from __future__ import annotations

from datetime import datetime
from typing import List

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.histogram import HistogramBucket, HistogramResult, build_histogram


def make_metric(
    name: str,
    total: int,
    failed: int,
    status: PipelineStatus = PipelineStatus.HEALTHY,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        total_runs=total,
        failed_runs=failed,
        status=status,
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
    )


class TestBuildHistogram:
    def test_empty_metrics_returns_empty_result(self):
        result = build_histogram([])
        assert result.total == 0
        assert result.buckets == []
        assert result.min_rate is None
        assert result.max_rate is None
        assert result.mean_rate is None

    def test_single_metric_fills_one_bucket(self):
        m = make_metric("p", total=10, failed=2)
        result = build_histogram([m], num_buckets=5)
        assert result.total == 1
        total_counts = sum(b.count for b in result.buckets)
        assert total_counts == 1

    def test_bucket_count_equals_metric_count(self):
        metrics = [
            make_metric("a", 10, 0),
            make_metric("b", 10, 5),
            make_metric("c", 10, 10),
        ]
        result = build_histogram(metrics, num_buckets=5)
        assert result.total == 3
        assert sum(b.count for b in result.buckets) == 3

    def test_min_max_mean_computed(self):
        metrics = [
            make_metric("a", 10, 0),   # 0.0
            make_metric("b", 10, 10),  # 1.0
        ]
        result = build_histogram(metrics, num_buckets=4)
        assert result.min_rate == pytest.approx(0.0)
        assert result.max_rate == pytest.approx(1.0)
        assert result.mean_rate == pytest.approx(0.5)

    def test_correct_number_of_buckets(self):
        metrics = [make_metric(f"p{i}", 10, i) for i in range(10)]
        result = build_histogram(metrics, num_buckets=5)
        assert len(result.buckets) == 5

    def test_pipeline_names_tracked_in_buckets(self):
        metrics = [
            make_metric("alpha", 10, 0),
            make_metric("beta", 10, 0),
        ]
        result = build_histogram(metrics, num_buckets=3)
        all_names = [n for b in result.buckets for n in b.pipelines]
        assert "alpha" in all_names
        assert "beta" in all_names

    def test_peak_bucket_has_highest_count(self):
        metrics = [
            make_metric("a", 10, 0),
            make_metric("b", 10, 0),
            make_metric("c", 10, 0),
            make_metric("d", 10, 10),
        ]
        result = build_histogram(metrics, num_buckets=4)
        peak = result.peak_bucket()
        assert peak is not None
        assert peak.count == max(b.count for b in result.buckets)

    def test_to_dict_contains_expected_keys(self):
        metrics = [make_metric("x", 10, 3)]
        result = build_histogram(metrics, num_buckets=2)
        d = result.to_dict()
        assert "buckets" in d
        assert "total" in d
        assert "min_rate" in d
        assert "max_rate" in d
        assert "mean_rate" in d

    def test_bucket_to_dict_range_string(self):
        b = HistogramBucket(low=0.0, high=0.25, count=3, pipelines=["p"])
        d = b.to_dict()
        assert d["range"] == "[0.00, 0.25)"
        assert d["count"] == 3
        assert d["pipelines"] == ["p"]

    def test_peak_bucket_none_for_empty_result(self):
        result = build_histogram([])
        assert result.peak_bucket() is None
