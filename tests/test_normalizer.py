"""Tests for pipewatch.normalizer."""

import pytest
from pipewatch.normalizer import normalize, normalize_many, NormalizationError
from pipewatch.metrics import PipelineStatus


def valid_raw(**overrides):
    base = {
        "pipeline_id": "etl_orders",
        "total_runs": 100,
        "failed_runs": 5,
        "duration_seconds": 12.4,
    }
    base.update(overrides)
    return base


def test_normalize_returns_pipeline_metric():
    metric = normalize(valid_raw())
    assert metric.pipeline_id == "etl_orders"
    assert metric.total_runs == 100
    assert metric.failed_runs == 5
    assert metric.duration_seconds == 12.4


def test_normalize_coerces_string_numbers():
    metric = normalize(valid_raw(total_runs="50", failed_runs="2", duration_seconds="9.1"))
    assert metric.total_runs == 50
    assert metric.failed_runs == 2
    assert pytest.approx(metric.duration_seconds) == 9.1


def test_normalize_sets_status():
    metric = normalize(valid_raw(total_runs=10, failed_runs=0))
    assert metric.status == PipelineStatus.HEALTHY


def test_normalize_raises_on_missing_field():
    raw = valid_raw()
    del raw["failed_runs"]
    with pytest.raises(NormalizationError, match="Missing required fields"):
        normalize(raw)


def test_normalize_raises_on_non_dict():
    with pytest.raises(NormalizationError, match="Expected a dict"):
        normalize(["not", "a", "dict"])


def test_normalize_raises_on_empty_pipeline_id():
    with pytest.raises(NormalizationError, match="must not be empty"):
        normalize(valid_raw(pipeline_id="   "))


def test_normalize_raises_when_failed_exceeds_total():
    with pytest.raises(NormalizationError, match="cannot exceed"):
        normalize(valid_raw(total_runs=10, failed_runs=15))


def test_normalize_raises_on_negative_runs():
    with pytest.raises(NormalizationError, match=">= 0"):
        normalize(valid_raw(total_runs=-1))


def test_normalize_raises_on_bad_float():
    with pytest.raises(NormalizationError, match="duration_seconds"):
        normalize(valid_raw(duration_seconds="not_a_number"))


def test_normalize_many_splits_valid_and_errors():
    raws = [
        valid_raw(pipeline_id="p1"),
        {"bad": "data"},
        valid_raw(pipeline_id="p2"),
    ]
    metrics, errors = normalize_many(raws)
    assert len(metrics) == 2
    assert len(errors) == 1
    assert errors[0]["index"] == 1


def test_normalize_many_empty_list():
    metrics, errors = normalize_many([])
    assert metrics == []
    assert errors == []
