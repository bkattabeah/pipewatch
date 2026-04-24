"""Tests for pipewatch.heatmap."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.heatmap import build_heatmap, HeatmapBucket, HeatmapResult
from pipewatch.metrics import PipelineMetric, PipelineStatus


def make_metric(
    pipeline: str = "pipe_a",
    hour: int = 10,
    error_rate: float = 0.1,
    total: int = 100,
    failed: int = 10,
) -> PipelineMetric:
    ts = datetime(2024, 1, 15, hour, 0, 0, tzinfo=timezone.utc)
    return PipelineMetric(
        pipeline=pipeline,
        timestamp=ts,
        total_records=total,
        failed_records=failed,
        error_rate=error_rate,
        status=PipelineStatus.HEALTHY,
    )


class TestBuildHeatmap:
    def test_empty_metrics_returns_empty_buckets(self):
        result = build_heatmap("pipe_a", [])
        assert result.pipeline == "pipe_a"
        assert result.buckets == []

    def test_peak_hour_none_for_empty(self):
        result = build_heatmap("pipe_a", [])
        assert result.peak_hour() is None

    def test_single_metric_creates_one_bucket(self):
        metrics = [make_metric(hour=9, error_rate=0.05)]
        result = build_heatmap("pipe_a", metrics)
        assert len(result.buckets) == 1
        assert result.buckets[0].hour == 9
        assert result.buckets[0].sample_count == 1
        assert abs(result.buckets[0].avg_error_rate - 0.05) < 1e-6

    def test_multiple_metrics_same_hour_averaged(self):
        metrics = [
            make_metric(hour=14, error_rate=0.2),
            make_metric(hour=14, error_rate=0.4),
        ]
        result = build_heatmap("pipe_a", metrics)
        assert len(result.buckets) == 1
        assert result.buckets[0].sample_count == 2
        assert abs(result.buckets[0].avg_error_rate - 0.3) < 1e-6

    def test_max_error_rate_tracked(self):
        metrics = [
            make_metric(hour=6, error_rate=0.1),
            make_metric(hour=6, error_rate=0.9),
            make_metric(hour=6, error_rate=0.3),
        ]
        result = build_heatmap("pipe_a", metrics)
        assert abs(result.buckets[0].max_error_rate - 0.9) < 1e-6

    def test_peak_hour_identified_correctly(self):
        metrics = [
            make_metric(hour=2, error_rate=0.05),
            make_metric(hour=14, error_rate=0.8),
            make_metric(hour=22, error_rate=0.2),
        ]
        result = build_heatmap("pipe_a", metrics)
        assert result.peak_hour() == 14

    def test_filters_other_pipelines(self):
        metrics = [
            make_metric(pipeline="pipe_a", hour=10, error_rate=0.5),
            make_metric(pipeline="pipe_b", hour=10, error_rate=0.9),
        ]
        result = build_heatmap("pipe_a", metrics)
        assert len(result.buckets) == 1
        assert abs(result.buckets[0].avg_error_rate - 0.5) < 1e-6

    def test_to_dict_structure(self):
        metrics = [make_metric(hour=8, error_rate=0.15)]
        result = build_heatmap("pipe_a", metrics)
        d = result.to_dict()
        assert d["pipeline"] == "pipe_a"
        assert d["peak_hour"] == 8
        assert len(d["buckets"]) == 1
        assert set(d["buckets"][0].keys()) == {"hour", "sample_count", "avg_error_rate", "max_error_rate"}

    def test_bucket_to_dict_rounds_values(self):
        b = HeatmapBucket(hour=3, sample_count=5, avg_error_rate=0.123456789, max_error_rate=0.987654321)
        d = b.to_dict()
        assert d["avg_error_rate"] == 0.1235
        assert d["max_error_rate"] == 0.9877
