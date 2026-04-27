"""Tests for pipewatch.jitter module."""
import pytest
from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.jitter import JitterConfig, JitterResult, analyze_jitter


def make_metric(pipeline: str, timestamp: float, processed: int = 100, failed: int = 0) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        processed=processed,
        failed=failed,
        timestamp=timestamp,
        status=PipelineStatus.HEALTHY,
    )


class TestJitterConfig:
    def test_defaults(self):
        cfg = JitterConfig()
        assert cfg.min_samples == 3
        assert cfg.high_jitter_threshold == 0.5
        assert cfg.critical_jitter_threshold == 1.0

    def test_validate_passes(self):
        JitterConfig(min_samples=2, high_jitter_threshold=0.3, critical_jitter_threshold=0.8).validate()

    def test_validate_rejects_min_samples_below_two(self):
        with pytest.raises(ValueError, match="min_samples"):
            JitterConfig(min_samples=1).validate()

    def test_validate_rejects_non_positive_high_threshold(self):
        with pytest.raises(ValueError, match="high_jitter_threshold"):
            JitterConfig(high_jitter_threshold=0.0).validate()

    def test_validate_rejects_critical_not_exceeding_high(self):
        with pytest.raises(ValueError, match="critical_jitter_threshold"):
            JitterConfig(high_jitter_threshold=0.5, critical_jitter_threshold=0.5).validate()


class TestAnalyzeJitter:
    def test_insufficient_data_when_below_min_samples(self):
        metrics = [make_metric("p", 1.0), make_metric("p", 2.0)]
        result = analyze_jitter("p", metrics, JitterConfig(min_samples=3))
        assert result.level == "insufficient_data"
        assert result.coefficient_of_variation is None

    def test_empty_metrics_returns_insufficient_data(self):
        result = analyze_jitter("p", [])
        assert result.level == "insufficient_data"
        assert result.sample_count == 0

    def test_regular_intervals_are_ok(self):
        # Perfectly regular: intervals all 10s -> stddev=0, cv=0
        metrics = [make_metric("p", float(t)) for t in range(0, 50, 10)]
        result = analyze_jitter("p", metrics)
        assert result.level == "ok"
        assert result.coefficient_of_variation == 0.0

    def test_high_jitter_detected(self):
        # Alternating short/long gaps -> high cv
        timestamps = [0.0, 1.0, 11.0, 12.0, 22.0, 23.0]
        metrics = [make_metric("p", t) for t in timestamps]
        cfg = JitterConfig(high_jitter_threshold=0.5, critical_jitter_threshold=1.5)
        result = analyze_jitter("p", metrics, cfg)
        assert result.level in ("high", "critical")
        assert result.coefficient_of_variation is not None
        assert result.coefficient_of_variation > 0

    def test_critical_jitter_detected(self):
        # Very irregular: one tiny gap among large ones
        timestamps = [0.0, 0.01, 100.0, 100.01, 200.0]
        metrics = [make_metric("p", t) for t in timestamps]
        cfg = JitterConfig(min_samples=3, high_jitter_threshold=0.3, critical_jitter_threshold=0.8)
        result = analyze_jitter("p", metrics, cfg)
        assert result.level == "critical"

    def test_to_dict_contains_expected_keys(self):
        metrics = [make_metric("pipe", float(t)) for t in range(0, 50, 10)]
        result = analyze_jitter("pipe", metrics)
        d = result.to_dict()
        for key in ("pipeline", "sample_count", "mean_interval", "stddev_interval",
                    "coefficient_of_variation", "level"):
            assert key in d

    def test_sample_count_matches_input(self):
        metrics = [make_metric("p", float(t)) for t in range(0, 60, 10)]
        result = analyze_jitter("p", metrics)
        assert result.sample_count == 6

    def test_metrics_sorted_by_timestamp(self):
        # Provide out-of-order timestamps; result should still be regular
        timestamps = [40.0, 0.0, 20.0, 60.0, 80.0]
        metrics = [make_metric("p", t) for t in timestamps]
        result = analyze_jitter("p", metrics)
        assert result.level == "ok"
        assert result.mean_interval == pytest.approx(20.0)
