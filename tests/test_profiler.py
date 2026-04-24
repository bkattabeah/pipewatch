"""Tests for pipewatch.profiler."""

import pytest
from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.profiler import profile_metrics, _percentile, ProfileStats


def make_metric(pipeline: str, total: int, failed: int) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        total_records=total,
        failed_records=failed,
        status=PipelineStatus.HEALTHY,
    )


def test_profile_returns_none_for_empty():
    result = profile_metrics("pipe", [])
    assert result is None


def test_profile_single_metric():
    m = make_metric("pipe", 100, 10)
    result = profile_metrics("pipe", [m])
    assert result is not None
    assert result.pipeline == "pipe"
    assert result.count == 1
    assert result.mean_error_rate == pytest.approx(0.1)
    assert result.min_error_rate == pytest.approx(0.1)
    assert result.max_error_rate == pytest.approx(0.1)
    assert result.stddev_error_rate == pytest.approx(0.0)


def test_profile_mean_error_rate():
    metrics = [
        make_metric("p", 100, 0),
        make_metric("p", 100, 50),
        make_metric("p", 100, 100),
    ]
    result = profile_metrics("p", metrics)
    assert result.mean_error_rate == pytest.approx(0.5)


def test_profile_min_max():
    metrics = [
        make_metric("p", 100, 5),
        make_metric("p", 100, 20),
        make_metric("p", 100, 50),
    ]
    result = profile_metrics("p", metrics)
    assert result.min_error_rate == pytest.approx(0.05)
    assert result.max_error_rate == pytest.approx(0.50)


def test_profile_stddev_nonzero_for_varied_data():
    metrics = [
        make_metric("p", 100, 10),
        make_metric("p", 100, 90),
    ]
    result = profile_metrics("p", metrics)
    assert result.stddev_error_rate > 0.0


def test_profile_mean_throughput():
    metrics = [
        make_metric("p", 200, 0),
        make_metric("p", 400, 0),
    ]
    result = profile_metrics("p", metrics)
    assert result.mean_throughput == pytest.approx(300.0)


def test_percentile_p50():
    values = [0.1, 0.2, 0.3, 0.4, 0.5]
    assert _percentile(values, 0.5) == pytest.approx(0.3)


def test_percentile_p95_single():
    assert _percentile([0.5], 0.95) == pytest.approx(0.5)


def test_percentile_empty():
    assert _percentile([], 0.5) == 0.0


def test_to_dict_keys():
    m = make_metric("pipe", 100, 10)
    result = profile_metrics("pipe", [m])
    d = result.to_dict()
    expected_keys = {
        "pipeline", "count", "mean_error_rate", "min_error_rate",
        "max_error_rate", "stddev_error_rate", "mean_throughput",
        "p50_error_rate", "p95_error_rate",
    }
    assert set(d.keys()) == expected_keys
