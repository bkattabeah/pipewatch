"""Tests for pipewatch.trend module."""
import pytest
from datetime import datetime, timezone
from pipewatch.metrics import PipelineMetric
from pipewatch.trend import analyze_trend, TrendResult, _linear_slope


def make_metric(pipeline_id="pipe1", processed=100, failed=0) -> PipelineMetric:
    return PipelineMetric(
        pipeline_id=pipeline_id,
        timestamp=datetime.now(timezone.utc),
        processed=processed,
        failed=failed,
        latency_ms=50.0,
    )


class TestLinearSlope:
    def test_flat_series(self):
        assert _linear_slope([0.1, 0.1, 0.1]) == pytest.approx(0.0)

    def test_increasing_series(self):
        slope = _linear_slope([0.0, 0.1, 0.2])
        assert slope > 0

    def test_decreasing_series(self):
        slope = _linear_slope([0.2, 0.1, 0.0])
        assert slope < 0

    def test_single_value(self):
        assert _linear_slope([0.5]) == 0.0

    def test_empty(self):
        assert _linear_slope([]) == 0.0


class TestAnalyzeTrend:
    def test_returns_none_for_empty(self):
        assert analyze_trend("pipe1", []) is None

    def test_stable_trend(self):
        metrics = [make_metric(failed=5) for _ in range(5)]
        result = analyze_trend("pipe1", metrics)
        assert result is not None
        assert result.trend_direction == "stable"
        assert result.sample_count == 5

    def test_degrading_trend(self):
        metrics = [
            make_metric(processed=100, failed=i * 5) for i in range(6)
        ]
        result = analyze_trend("pipe1", metrics)
        assert result is not None
        assert result.trend_direction == "degrading"
        assert result.slope > 0

    def test_improving_trend(self):
        metrics = [
            make_metric(processed=100, failed=(5 - i) * 5) for i in range(6)
        ]
        result = analyze_trend("pipe1", metrics)
        assert result is not None
        assert result.trend_direction == "improving"
        assert result.slope < 0

    def test_to_dict_keys(self):
        metrics = [make_metric() for _ in range(3)]
        result = analyze_trend("pipe1", metrics)
        d = result.to_dict()
        expected_keys = {
            "pipeline_id", "sample_count", "avg_error_rate",
            "min_error_rate", "max_error_rate", "trend_direction", "slope",
        }
        assert expected_keys == set(d.keys())

    def test_single_metric_is_stable(self):
        result = analyze_trend("pipe1", [make_metric(failed=10)])
        assert result.trend_direction == "stable"
        assert result.sample_count == 1
