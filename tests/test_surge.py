"""Tests for pipewatch.surge."""

import pytest
from datetime import datetime, timezone

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.surge import SurgeConfig, SurgeResult, detect_surge


def make_metric(pipeline: str, processed: int, failed: int) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        processed=processed,
        failed=failed,
        status=PipelineStatus.HEALTHY,
        timestamp=datetime.now(timezone.utc),
    )


class TestSurgeConfig:
    def test_defaults(self):
        cfg = SurgeConfig()
        assert cfg.window == 10
        assert cfg.baseline_window == 30
        assert cfg.multiplier == 2.0

    def test_validate_passes(self):
        SurgeConfig(window=5, baseline_window=20, multiplier=1.5).validate()

    def test_validate_rejects_small_window(self):
        with pytest.raises(ValueError, match="window must be"):
            SurgeConfig(window=1).validate()

    def test_validate_rejects_baseline_smaller_than_window(self):
        with pytest.raises(ValueError, match="baseline_window"):
            SurgeConfig(window=10, baseline_window=5).validate()

    def test_validate_rejects_multiplier_lte_one(self):
        with pytest.raises(ValueError, match="multiplier"):
            SurgeConfig(multiplier=1.0).validate()


class TestDetectSurge:
    def _make_batch(self, pipeline: str, count: int, processed: int, failed: int):
        return [make_metric(pipeline, processed, failed) for _ in range(count)]

    def test_returns_none_when_insufficient_data(self):
        metrics = self._make_batch("p", 5, 100, 5)
        result = detect_surge("p", metrics, SurgeConfig(window=10, baseline_window=30))
        assert result is None

    def test_returns_none_when_no_baseline_slice(self):
        # exactly window metrics — no baseline slice available
        metrics = self._make_batch("p", 10, 100, 5)
        result = detect_surge("p", metrics, SurgeConfig(window=10, baseline_window=30))
        assert result is None

    def test_no_surge_when_rates_stable(self):
        baseline = self._make_batch("p", 25, 100, 5)   # 5% error rate
        recent = self._make_batch("p", 10, 100, 6)      # 6% — not a 2x surge
        result = detect_surge("p", baseline + recent, SurgeConfig(window=10, baseline_window=30))
        assert result is not None
        assert result.is_surge is False

    def test_surge_detected_when_rate_spikes(self):
        baseline = self._make_batch("p", 25, 100, 5)   # 5% error rate
        recent = self._make_batch("p", 10, 100, 15)    # 15% — 3x surge
        result = detect_surge("p", baseline + recent, SurgeConfig(window=10, baseline_window=30))
        assert result is not None
        assert result.is_surge is True
        assert result.multiplier_observed >= 2.0

    def test_no_surge_when_baseline_is_zero(self):
        baseline = self._make_batch("p", 25, 100, 0)   # 0% baseline
        recent = self._make_batch("p", 10, 100, 10)
        result = detect_surge("p", baseline + recent, SurgeConfig(window=10, baseline_window=30))
        assert result is not None
        assert result.is_surge is False  # baseline=0 → multiplier_observed=0

    def test_to_dict_keys(self):
        baseline = self._make_batch("p", 25, 100, 5)
        recent = self._make_batch("p", 10, 100, 15)
        result = detect_surge("p", baseline + recent)
        assert result is not None
        d = result.to_dict()
        assert set(d.keys()) == {"pipeline", "current_rate", "baseline_rate", "multiplier_observed", "is_surge"}

    def test_pipeline_name_preserved(self):
        baseline = self._make_batch("my_pipeline", 25, 100, 5)
        recent = self._make_batch("my_pipeline", 10, 100, 15)
        result = detect_surge("my_pipeline", baseline + recent)
        assert result is not None
        assert result.pipeline == "my_pipeline"
