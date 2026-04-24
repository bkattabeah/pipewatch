"""Tests for pipewatch.window sliding window aggregation."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.window import WindowConfig, WindowStats, compute_window


def make_metric(
    pipeline: str = "pipe1",
    processed: int = 100,
    failed: int = 0,
    offset_seconds: int = 0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        processed=processed,
        failed=failed,
        timestamp=datetime.utcnow() - timedelta(seconds=offset_seconds),
        status=PipelineStatus.HEALTHY,
    )


class TestWindowConfig:
    def test_defaults(self):
        cfg = WindowConfig()
        assert cfg.size_seconds == 300
        assert cfg.min_samples == 1

    def test_validate_passes(self):
        WindowConfig(size_seconds=60, min_samples=2).validate()

    def test_validate_rejects_zero_size(self):
        with pytest.raises(ValueError, match="size_seconds"):
            WindowConfig(size_seconds=0).validate()

    def test_validate_rejects_negative_size(self):
        with pytest.raises(ValueError):
            WindowConfig(size_seconds=-10).validate()

    def test_validate_rejects_zero_min_samples(self):
        with pytest.raises(ValueError, match="min_samples"):
            WindowConfig(min_samples=0).validate()


class TestComputeWindow:
    def test_returns_none_when_no_metrics(self):
        result = compute_window("pipe1", [])
        assert result is None

    def test_returns_none_below_min_samples(self):
        metrics = [make_metric()]
        result = compute_window("pipe1", metrics, WindowConfig(min_samples=3))
        assert result is None

    def test_excludes_old_metrics(self):
        recent = make_metric(offset_seconds=10)
        old = make_metric(offset_seconds=400)
        result = compute_window("pipe1", [recent, old], WindowConfig(size_seconds=300))
        assert result is not None
        assert result.sample_count == 1

    def test_filters_by_pipeline(self):
        m1 = make_metric(pipeline="pipe1")
        m2 = make_metric(pipeline="pipe2")
        result = compute_window("pipe1", [m1, m2])
        assert result is not None
        assert result.sample_count == 1
        assert result.pipeline == "pipe1"

    def test_avg_error_rate(self):
        m1 = make_metric(processed=100, failed=10)
        m2 = make_metric(processed=100, failed=20)
        result = compute_window("pipe1", [m1, m2])
        assert result is not None
        assert abs(result.avg_error_rate - 0.15) < 1e-6

    def test_max_and_min_error_rate(self):
        m1 = make_metric(processed=100, failed=5)
        m2 = make_metric(processed=100, failed=30)
        result = compute_window("pipe1", [m1, m2])
        assert result is not None
        assert result.max_error_rate == pytest.approx(0.30)
        assert result.min_error_rate == pytest.approx(0.05)

    def test_totals(self):
        m1 = make_metric(processed=100, failed=10)
        m2 = make_metric(processed=200, failed=5)
        result = compute_window("pipe1", [m1, m2])
        assert result is not None
        assert result.total_processed == 300
        assert result.total_failed == 15

    def test_to_dict_keys(self):
        result = compute_window("pipe1", [make_metric()])
        assert result is not None
        d = result.to_dict()
        for key in ("pipeline", "window_seconds", "sample_count",
                    "avg_error_rate", "max_error_rate", "min_error_rate",
                    "total_processed", "total_failed", "start", "end"):
            assert key in d
