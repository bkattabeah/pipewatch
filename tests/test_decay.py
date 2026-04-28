"""Tests for pipewatch.decay."""

from __future__ import annotations

import pytest
from pipewatch.decay import DecayConfig, DecayResult, analyze_decay, _slope
from pipewatch.metrics import PipelineMetric, PipelineStatus


def make_metric(pipeline: str, processed: int, failed: int) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        processed=processed,
        failed=failed,
        status=PipelineStatus.HEALTHY,
    )


class TestDecayConfig:
    def test_defaults(self):
        cfg = DecayConfig()
        assert cfg.min_samples == 5
        assert cfg.decay_threshold == 0.01
        assert cfg.window == 20

    def test_validate_passes(self):
        DecayConfig(min_samples=3, decay_threshold=0.005, window=10).validate()

    def test_validate_rejects_min_samples_below_two(self):
        with pytest.raises(ValueError, match="min_samples"):
            DecayConfig(min_samples=1).validate()

    def test_validate_rejects_zero_threshold(self):
        with pytest.raises(ValueError, match="decay_threshold"):
            DecayConfig(decay_threshold=0.0).validate()

    def test_validate_rejects_window_smaller_than_min_samples(self):
        with pytest.raises(ValueError, match="window"):
            DecayConfig(min_samples=10, window=5).validate()


class TestLinearSlope:
    def test_flat_series_returns_zero(self):
        assert _slope([0.1, 0.1, 0.1, 0.1]) == pytest.approx(0.0)

    def test_increasing_series_positive_slope(self):
        assert _slope([0.0, 0.1, 0.2, 0.3]) > 0

    def test_decreasing_series_negative_slope(self):
        assert _slope([0.3, 0.2, 0.1, 0.0]) < 0

    def test_single_value_returns_zero(self):
        assert _slope([0.5]) == pytest.approx(0.0)


class TestAnalyzeDecay:
    def _make_batch(self, pipeline: str, rates: list[float]) -> list[PipelineMetric]:
        metrics = []
        for r in rates:
            processed = 100
            failed = int(r * processed)
            metrics.append(make_metric(pipeline, processed, failed))
        return metrics

    def test_returns_none_for_insufficient_samples(self):
        metrics = self._make_batch("p1", [0.1, 0.2])
        result = analyze_decay(metrics, DecayConfig(min_samples=5))
        assert result is None

    def test_stable_pipeline_not_decaying(self):
        metrics = self._make_batch("p1", [0.05] * 10)
        result = analyze_decay(metrics, DecayConfig(min_samples=5, decay_threshold=0.01))
        assert result is not None
        assert result.is_decaying is False

    def test_rising_error_rate_detected_as_decaying(self):
        rates = [0.01 * i for i in range(1, 11)]
        metrics = self._make_batch("p2", rates)
        result = analyze_decay(metrics, DecayConfig(min_samples=5, decay_threshold=0.005))
        assert result is not None
        assert result.is_decaying is True

    def test_result_fields_populated(self):
        rates = [0.02 * i for i in range(1, 8)]
        metrics = self._make_batch("p3", rates)
        result = analyze_decay(metrics, DecayConfig(min_samples=5, window=10))
        assert result is not None
        assert result.pipeline == "p3"
        assert result.sample_count == len(rates)
        assert result.first_error_rate >= 0
        assert result.last_error_rate >= 0

    def test_to_dict_contains_expected_keys(self):
        rates = [0.01 * i for i in range(1, 8)]
        metrics = self._make_batch("p4", rates)
        result = analyze_decay(metrics, DecayConfig(min_samples=5))
        assert result is not None
        d = result.to_dict()
        for key in ("pipeline", "slope", "is_decaying", "sample_count", "first_error_rate", "last_error_rate"):
            assert key in d

    def test_window_limits_samples_used(self):
        # First 5 are flat, last 5 are rising — window=5 should see only rising
        flat = [0.05] * 5
        rising = [0.05 + 0.02 * i for i in range(1, 6)]
        metrics = self._make_batch("p5", flat + rising)
        result = analyze_decay(metrics, DecayConfig(min_samples=5, window=5, decay_threshold=0.005))
        assert result is not None
        assert result.is_decaying is True
