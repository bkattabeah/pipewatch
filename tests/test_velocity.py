"""Tests for pipewatch.velocity."""
from __future__ import annotations

import pytest
from datetime import datetime, timezone

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.velocity import VelocityConfig, VelocityResult, compute_velocity


def make_metric(pipeline: str, error_rate: float) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        total=100,
        failed=int(error_rate * 100),
        error_rate=error_rate,
        status=PipelineStatus.HEALTHY,
        timestamp=datetime.now(tz=timezone.utc),
    )


class TestVelocityConfig:
    def test_defaults(self):
        cfg = VelocityConfig()
        assert cfg.window == 10
        assert cfg.spike_threshold == 0.10

    def test_validate_passes(self):
        VelocityConfig(window=5, spike_threshold=0.05).validate()

    def test_validate_rejects_small_window(self):
        with pytest.raises(ValueError, match="window"):
            VelocityConfig(window=1).validate()

    def test_validate_rejects_zero_threshold(self):
        with pytest.raises(ValueError, match="spike_threshold"):
            VelocityConfig(spike_threshold=0.0).validate()


class TestComputeVelocity:
    def test_returns_none_for_empty(self):
        assert compute_velocity([]) is None

    def test_returns_none_for_single_metric(self):
        metrics = [make_metric("p", 0.1)]
        assert compute_velocity(metrics) is None

    def test_stable_series(self):
        metrics = [make_metric("p", 0.05)] * 5
        result = compute_velocity(metrics)
        assert result is not None
        assert result.direction == "stable"
        assert result.mean_delta == pytest.approx(0.0)
        assert result.is_spike is False

    def test_rising_series(self):
        rates = [0.01, 0.03, 0.06, 0.10, 0.15]
        metrics = [make_metric("p", r) for r in rates]
        result = compute_velocity(metrics)
        assert result is not None
        assert result.direction == "rising"
        assert result.mean_delta > 0

    def test_falling_series(self):
        rates = [0.20, 0.15, 0.10, 0.05, 0.01]
        metrics = [make_metric("p", r) for r in rates]
        result = compute_velocity(metrics)
        assert result is not None
        assert result.direction == "falling"
        assert result.mean_delta < 0

    def test_spike_detected(self):
        rates = [0.01, 0.02, 0.03, 0.04, 0.25]  # big jump at the end
        metrics = [make_metric("p", r) for r in rates]
        result = compute_velocity(metrics, VelocityConfig(spike_threshold=0.10))
        assert result is not None
        assert result.is_spike is True
        assert result.max_delta >= 0.10

    def test_window_limits_samples(self):
        rates = [0.01 * i for i in range(20)]
        metrics = [make_metric("p", r) for r in rates]
        result = compute_velocity(metrics, VelocityConfig(window=5))
        assert result is not None
        assert result.samples == 5

    def test_to_dict_keys(self):
        metrics = [make_metric("pipe", 0.1 * i) for i in range(5)]
        result = compute_velocity(metrics)
        d = result.to_dict()
        assert set(d.keys()) == {"pipeline", "samples", "mean_delta", "max_delta", "is_spike", "direction"}
        assert d["pipeline"] == "pipe"
