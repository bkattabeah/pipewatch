"""Tests for pipewatch.anomaly module."""
from __future__ import annotations

import pytest
from datetime import datetime, timezone

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.anomaly import (
    AnomalyConfig,
    AnomalyResult,
    detect_anomaly,
    detect_anomalies,
    _mean,
    _std_dev,
)


def make_metric(pipeline: str, processed: int, failed: int) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        processed=processed,
        failed=failed,
        latency_ms=100.0,
        timestamp=datetime.now(timezone.utc),
        status=PipelineStatus.HEALTHY,
    )


class TestAnomalyConfig:
    def test_defaults(self):
        cfg = AnomalyConfig()
        assert cfg.z_score_threshold == 2.5
        assert cfg.min_samples == 5

    def test_validate_passes(self):
        AnomalyConfig(z_score_threshold=3.0, min_samples=10).validate()

    def test_validate_rejects_non_positive_threshold(self):
        with pytest.raises(ValueError, match="z_score_threshold"):
            AnomalyConfig(z_score_threshold=0.0).validate()

    def test_validate_rejects_min_samples_below_2(self):
        with pytest.raises(ValueError, match="min_samples"):
            AnomalyConfig(min_samples=1).validate()


class TestHelpers:
    def test_mean(self):
        assert _mean([1.0, 2.0, 3.0]) == pytest.approx(2.0)

    def test_std_dev_flat(self):
        assert _std_dev([2.0, 2.0, 2.0], 2.0) == pytest.approx(0.0)

    def test_std_dev_known(self):
        values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
        mu = _mean(values)
        sigma = _std_dev(values, mu)
        assert sigma == pytest.approx(2.0)


class TestDetectAnomaly:
    def test_returns_none_when_insufficient_samples(self):
        metrics = [make_metric("p", 100, 1)] * 3
        result = detect_anomaly("p", metrics, AnomalyConfig(min_samples=5))
        assert result is None

    def test_no_anomaly_for_stable_series(self):
        metrics = [make_metric("p", 100, 5)] * 10
        result = detect_anomaly("p", metrics)
        assert result is not None
        assert result.is_anomaly is False

    def test_detects_spike_as_anomaly(self):
        stable = [make_metric("p", 100, 1)] * 9
        spike = make_metric("p", 100, 90)
        history = stable + [spike]
        result = detect_anomaly("p", history, AnomalyConfig(z_score_threshold=2.0))
        assert result is not None
        assert result.is_anomaly is True
        assert result.z_score > 2.0

    def test_to_dict_keys(self):
        metrics = [make_metric("p", 100, 5)] * 10
        result = detect_anomaly("p", metrics)
        d = result.to_dict()
        assert set(d.keys()) == {"pipeline", "current_error_rate", "mean", "std_dev", "z_score", "is_anomaly"}

    def test_zero_std_dev_gives_zero_z_score(self):
        metrics = [make_metric("p", 100, 0)] * 10
        result = detect_anomaly("p", metrics)
        assert result is not None
        assert result.z_score == pytest.approx(0.0)
        assert result.is_anomaly is False


class TestDetectAnomalies:
    def test_returns_results_for_each_pipeline(self):
        history_map = {
            "pipe_a": [make_metric("pipe_a", 100, 5)] * 10,
            "pipe_b": [make_metric("pipe_b", 100, 10)] * 10,
        }
        results = detect_anomalies(history_map)
        assert len(results) == 2
        pipelines = {r.pipeline for r in results}
        assert pipelines == {"pipe_a", "pipe_b"}

    def test_skips_pipeline_with_insufficient_history(self):
        history_map = {
            "pipe_a": [make_metric("pipe_a", 100, 5)] * 10,
            "pipe_b": [make_metric("pipe_b", 100, 5)] * 2,
        }
        results = detect_anomalies(history_map)
        assert len(results) == 1
        assert results[0].pipeline == "pipe_a"
