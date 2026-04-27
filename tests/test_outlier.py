"""Tests for pipewatch.outlier module."""
from __future__ import annotations

import pytest
from datetime import datetime

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.outlier import OutlierConfig, OutlierResult, detect_outliers, _quartiles


def make_metric(pipeline: str, error_rate: float) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        total=100,
        failed=int(error_rate * 100),
        error_rate=error_rate,
        status=PipelineStatus.HEALTHY,
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
    )


class TestOutlierConfig:
    def test_defaults(self):
        cfg = OutlierConfig()
        assert cfg.min_samples == 4
        assert cfg.iqr_multiplier == 1.5

    def test_validate_passes(self):
        OutlierConfig(min_samples=2, iqr_multiplier=1.0).validate()

    def test_validate_rejects_low_min_samples(self):
        with pytest.raises(ValueError, match="min_samples"):
            OutlierConfig(min_samples=1).validate()

    def test_validate_rejects_zero_multiplier(self):
        with pytest.raises(ValueError, match="iqr_multiplier"):
            OutlierConfig(iqr_multiplier=0.0).validate()


class TestQuartiles:
    def test_even_list(self):
        q1, q3 = _quartiles([1, 2, 3, 4])
        assert q1 == 1.5
        assert q3 == 3.5

    def test_odd_list(self):
        q1, q3 = _quartiles([1, 2, 3, 4, 5])
        assert q1 == 1.5
        assert q3 == 4.5


class TestDetectOutliers:
    def test_returns_empty_when_below_min_samples(self):
        metrics = [make_metric(f"p{i}", 0.1) for i in range(3)]
        results = detect_outliers(metrics, OutlierConfig(min_samples=4))
        assert results == []

    def test_no_outliers_for_uniform_rates(self):
        metrics = [make_metric(f"p{i}", 0.1) for i in range(6)]
        results = detect_outliers(metrics)
        assert all(not r.is_outlier for r in results)

    def test_detects_high_outlier(self):
        metrics = [
            make_metric("p1", 0.05),
            make_metric("p2", 0.06),
            make_metric("p3", 0.05),
            make_metric("p4", 0.07),
            make_metric("spike", 0.95),
        ]
        results = detect_outliers(metrics)
        outliers = [r for r in results if r.is_outlier]
        assert len(outliers) == 1
        assert outliers[0].pipeline == "spike"
        assert outliers[0].reason == "above upper fence"

    def test_detects_low_outlier(self):
        metrics = [
            make_metric("p1", 0.50),
            make_metric("p2", 0.55),
            make_metric("p3", 0.52),
            make_metric("p4", 0.53),
            make_metric("low", 0.0),
        ]
        results = detect_outliers(metrics)
        outliers = [r for r in results if r.is_outlier]
        assert any(r.pipeline == "low" for r in outliers)

    def test_result_count_equals_input_count(self):
        metrics = [make_metric(f"p{i}", 0.1 * i) for i in range(5)]
        results = detect_outliers(metrics)
        assert len(results) == len(metrics)

    def test_to_dict_has_expected_keys(self):
        metrics = [make_metric(f"p{i}", 0.1) for i in range(5)]
        results = detect_outliers(metrics)
        d = results[0].to_dict()
        assert set(d.keys()) == {"pipeline", "error_rate", "is_outlier", "lower_fence", "upper_fence", "reason"}

    def test_fences_are_symmetric_for_uniform(self):
        metrics = [make_metric(f"p{i}", 0.2) for i in range(6)]
        results = detect_outliers(metrics)
        r = results[0]
        assert r.lower_fence == r.upper_fence
