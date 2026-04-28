"""Tests for pipewatch.momentum."""

from __future__ import annotations

import pytest
from datetime import datetime, timezone

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.momentum import (
    MomentumConfig,
    MomentumResult,
    _slope,
    analyze_momentum,
)


def make_metric(error_rate: float, pipeline: str = "pipe-a") -> PipelineMetric:
    total = 100
    failed = int(error_rate * total)
    return PipelineMetric(
        pipeline=pipeline,
        total=total,
        failed=failed,
        error_rate=error_rate,
        status=PipelineStatus.HEALTHY if error_rate < 0.1 else PipelineStatus.CRITICAL,
        recorded_at=datetime.now(timezone.utc),
    )


class TestMomentumConfig:
    def test_defaults(self):
        cfg = MomentumConfig()
        assert cfg.window == 10
        assert cfg.min_samples == 4
        assert cfg.accel_threshold == 0.05

    def test_validate_passes(self):
        MomentumConfig(window=6, min_samples=4, accel_threshold=0.01).validate()

    def test_validate_rejects_small_window(self):
        with pytest.raises(ValueError, match="window"):
            MomentumConfig(window=1).validate()

    def test_validate_rejects_small_min_samples(self):
        with pytest.raises(ValueError, match="min_samples"):
            MomentumConfig(min_samples=1).validate()

    def test_validate_rejects_min_samples_exceeding_window(self):
        with pytest.raises(ValueError, match="min_samples must not exceed window"):
            MomentumConfig(window=4, min_samples=6).validate()

    def test_validate_rejects_negative_threshold(self):
        with pytest.raises(ValueError, match="accel_threshold"):
            MomentumConfig(accel_threshold=-0.1).validate()


class TestSlope:
    def test_flat_series_returns_zero(self):
        assert _slope([0.1, 0.1, 0.1, 0.1]) == pytest.approx(0.0)

    def test_increasing_series_positive_slope(self):
        assert _slope([0.0, 0.1, 0.2, 0.3]) > 0

    def test_decreasing_series_negative_slope(self):
        assert _slope([0.3, 0.2, 0.1, 0.0]) < 0

    def test_single_value_returns_zero(self):
        assert _slope([0.5]) == 0.0


class TestAnalyzeMomentum:
    def test_returns_none_when_insufficient_data(self):
        metrics = [make_metric(0.1)] * 3
        result = analyze_momentum("pipe", metrics, MomentumConfig(min_samples=4))
        assert result is None

    def test_stable_flat_series(self):
        metrics = [make_metric(0.1)] * 10
        result = analyze_momentum("pipe", metrics)
        assert result is not None
        assert result.direction == "stable"
        assert not result.is_accelerating

    def test_worsening_when_acceleration_positive(self):
        # second half ramps up sharply
        rates = [0.05, 0.05, 0.05, 0.05, 0.10, 0.20, 0.35, 0.55]
        metrics = [make_metric(r) for r in rates]
        cfg = MomentumConfig(window=8, min_samples=4, accel_threshold=0.01)
        result = analyze_momentum("pipe", metrics, cfg)
        assert result is not None
        assert result.direction == "worsening"
        assert result.is_accelerating

    def test_improving_when_acceleration_negative(self):
        rates = [0.50, 0.40, 0.30, 0.20, 0.10, 0.08, 0.06, 0.05]
        metrics = [make_metric(r) for r in rates]
        cfg = MomentumConfig(window=8, min_samples=4, accel_threshold=0.01)
        result = analyze_momentum("pipe", metrics, cfg)
        assert result is not None
        assert result.direction == "improving"

    def test_to_dict_contains_expected_keys(self):
        metrics = [make_metric(0.1)] * 10
        result = analyze_momentum("pipe-x", metrics)
        assert result is not None
        d = result.to_dict()
        for key in ("pipeline", "sample_count", "first_slope", "second_slope", "acceleration", "direction"):
            assert key in d

    def test_sample_count_respects_window(self):
        metrics = [make_metric(0.1)] * 20
        cfg = MomentumConfig(window=8, min_samples=4)
        result = analyze_momentum("pipe", metrics, cfg)
        assert result is not None
        assert result.sample_count == 8
