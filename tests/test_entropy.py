"""Tests for pipewatch.entropy module."""
import math
from datetime import datetime

import pytest

from pipewatch.entropy import (
    EntropyConfig,
    EntropyResult,
    analyze_entropy,
    _shannon_entropy,
)
from pipewatch.metrics import PipelineMetric, PipelineStatus


def make_metric(processed: int, failed: int, name: str = "pipe") -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        processed=processed,
        failed=failed,
        timestamp=datetime.utcnow(),
        status=PipelineStatus.HEALTHY,
    )


class TestEntropyConfig:
    def test_defaults(self):
        cfg = EntropyConfig()
        assert cfg.min_samples == 5
        assert cfg.bucket_count == 10

    def test_validate_passes(self):
        EntropyConfig(min_samples=2, bucket_count=2).validate()

    def test_validate_rejects_small_min_samples(self):
        with pytest.raises(ValueError, match="min_samples"):
            EntropyConfig(min_samples=1).validate()

    def test_validate_rejects_small_bucket_count(self):
        with pytest.raises(ValueError, match="bucket_count"):
            EntropyConfig(bucket_count=1).validate()


class TestShannonEntropy:
    def test_empty_returns_zero(self):
        assert _shannon_entropy([], 10) == 0.0

    def test_uniform_distribution_max_entropy(self):
        # All values spread across every bucket equally
        values = [i / 10 for i in range(10)]
        e = _shannon_entropy(values, 10)
        assert e == pytest.approx(math.log2(10), rel=0.01)

    def test_constant_values_zero_entropy(self):
        values = [0.0] * 20
        e = _shannon_entropy(values, 10)
        assert e == 0.0


class TestAnalyzeEntropy:
    def _uniform_metrics(self, n: int = 10) -> list:
        """Metrics with error rates spread 0..1."""
        metrics = []
        for i in range(n):
            processed = 10
            failed = int((i / n) * processed)
            metrics.append(make_metric(processed, failed))
        return metrics

    def test_returns_none_below_min_samples(self):
        metrics = [make_metric(10, 1)] * 3
        result = analyze_entropy("pipe", metrics, EntropyConfig(min_samples=5))
        assert result is None

    def test_returns_result_at_min_samples(self):
        metrics = [make_metric(10, 1)] * 5
        result = analyze_entropy("pipe", metrics)
        assert result is not None
        assert result.pipeline == "pipe"
        assert result.sample_count == 5

    def test_stable_label_for_constant_metrics(self):
        metrics = [make_metric(100, 0)] * 10
        result = analyze_entropy("pipe", metrics)
        assert result.label == "stable"
        assert result.is_stable is True
        assert result.entropy == 0.0
        assert result.normalized_entropy == 0.0

    def test_volatile_label_for_spread_metrics(self):
        metrics = self._uniform_metrics(20)
        result = analyze_entropy("pipe", metrics)
        assert result is not None
        assert result.normalized_entropy > 0.0

    def test_to_dict_contains_expected_keys(self):
        metrics = [make_metric(10, 1)] * 8
        result = analyze_entropy("pipe", metrics)
        d = result.to_dict()
        assert set(d.keys()) == {
            "pipeline", "sample_count", "entropy",
            "normalized_entropy", "is_stable", "label",
        }

    def test_normalized_entropy_between_zero_and_one(self):
        metrics = self._uniform_metrics(15)
        result = analyze_entropy("pipe", metrics)
        assert 0.0 <= result.normalized_entropy <= 1.0
