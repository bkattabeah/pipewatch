"""Tests for pipewatch.forecaster."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.forecaster import ForecastConfig, ForecastResult, forecast


def make_metric(error_rate: float, pipeline: str = "pipe-a") -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        total=100,
        failed=int(error_rate * 100),
        error_rate=error_rate,
        status=PipelineStatus.HEALTHY,
        recorded_at=datetime.now(timezone.utc),
    )


class TestForecastConfig:
    def test_defaults(self):
        cfg = ForecastConfig()
        assert cfg.horizon == 5
        assert cfg.min_samples == 3

    def test_validate_passes(self):
        ForecastConfig(horizon=3, min_samples=2).validate()

    def test_validate_rejects_zero_horizon(self):
        with pytest.raises(ValueError, match="horizon"):
            ForecastConfig(horizon=0).validate()

    def test_validate_rejects_low_min_samples(self):
        with pytest.raises(ValueError, match="min_samples"):
            ForecastConfig(min_samples=1).validate()


class TestForecast:
    def _history(self, rates: List[float]) -> List[PipelineMetric]:
        return [make_metric(r) for r in rates]

    def test_insufficient_data_when_too_few_samples(self):
        result = forecast("p", self._history([0.1, 0.2]), ForecastConfig(min_samples=3))
        assert result.insufficient_data is True
        assert result.points == []

    def test_returns_correct_number_of_points(self):
        result = forecast("p", self._history([0.1, 0.2, 0.3]), ForecastConfig(horizon=4))
        assert not result.insufficient_data
        assert len(result.points) == 4

    def test_flat_series_zero_slope(self):
        result = forecast("p", self._history([0.2, 0.2, 0.2, 0.2]))
        assert abs(result.slope) < 1e-9
        for pt in result.points:
            assert abs(pt.predicted_error_rate - 0.2) < 1e-6

    def test_increasing_series_positive_slope(self):
        result = forecast("p", self._history([0.1, 0.2, 0.3, 0.4]))
        assert result.slope > 0

    def test_decreasing_series_negative_slope(self):
        result = forecast("p", self._history([0.4, 0.3, 0.2, 0.1]))
        assert result.slope < 0

    def test_predicted_rate_clamped_to_zero(self):
        # strongly decreasing — predictions should not go below 0
        result = forecast("p", self._history([0.05, 0.03, 0.01]), ForecastConfig(horizon=10))
        for pt in result.points:
            assert pt.predicted_error_rate >= 0.0

    def test_predicted_rate_clamped_to_one(self):
        result = forecast("p", self._history([0.8, 0.9, 1.0]), ForecastConfig(horizon=10))
        for pt in result.points:
            assert pt.predicted_error_rate <= 1.0

    def test_tick_sequence_is_contiguous(self):
        history = self._history([0.1, 0.2, 0.3])
        result = forecast("p", history, ForecastConfig(horizon=3))
        ticks = [pt.tick for pt in result.points]
        assert ticks == [3, 4, 5]

    def test_to_dict_contains_expected_keys(self):
        result = forecast("pipe-x", self._history([0.1, 0.2, 0.3]))
        d = result.to_dict()
        assert d["pipeline"] == "pipe-x"
        assert "slope" in d
        assert "points" in d
        assert "insufficient_data" in d
