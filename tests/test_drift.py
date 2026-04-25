"""Tests for pipewatch.drift."""
from __future__ import annotations

from datetime import datetime
from typing import List

import pytest

from pipewatch.drift import DriftConfig, DriftResult, detect_drift, detect_drift_many
from pipewatch.metrics import PipelineMetric, PipelineStatus


def make_metric(
    pipeline: str = "pipe_a",
    total: int = 100,
    failed: int = 0,
    status: PipelineStatus = PipelineStatus.HEALTHY,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        total=total,
        failed=failed,
        status=status,
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
    )


class TestDriftConfig:
    def test_defaults(self):
        cfg = DriftConfig()
        assert cfg.window == 10
        assert cfg.threshold == 0.15

    def test_validate_passes(self):
        DriftConfig(window=5, threshold=0.10).validate()

    def test_validate_rejects_small_window(self):
        with pytest.raises(ValueError, match="window"):
            DriftConfig(window=1).validate()

    def test_validate_rejects_zero_threshold(self):
        with pytest.raises(ValueError, match="threshold"):
            DriftConfig(threshold=0.0).validate()

    def test_validate_rejects_threshold_above_one(self):
        with pytest.raises(ValueError, match="threshold"):
            DriftConfig(threshold=1.1).validate()


class TestDetectDrift:
    def test_no_drift_when_rates_equal(self):
        history = [make_metric(failed=10) for _ in range(5)]
        current = make_metric(failed=10)
        result = detect_drift(history, current, DriftConfig(threshold=0.05))
        assert not result.drifted
        assert result.delta == pytest.approx(0.0)

    def test_drift_detected_above_threshold(self):
        history = [make_metric(failed=5) for _ in range(5)]  # 5% error rate
        current = make_metric(failed=30)  # 30% error rate
        result = detect_drift(history, current, DriftConfig(threshold=0.10))
        assert result.drifted
        assert result.delta == pytest.approx(0.25)

    def test_empty_history_uses_zero_baseline(self):
        current = make_metric(failed=20)
        result = detect_drift([], current)
        assert result.baseline_error_rate == pytest.approx(0.0)
        assert result.baseline_samples == 0

    def test_window_limits_baseline_samples(self):
        history = [make_metric(failed=i) for i in range(20)]
        current = make_metric(failed=0)
        result = detect_drift(history, current, DriftConfig(window=4))
        assert result.baseline_samples == 4

    def test_to_dict_contains_expected_keys(self):
        result = detect_drift([], make_metric())
        d = result.to_dict()
        for key in ("pipeline", "baseline_error_rate", "current_error_rate", "delta", "drifted", "baseline_samples"):
            assert key in d


class TestDetectDriftMany:
    def test_returns_result_per_pipeline(self):
        metrics = [make_metric("a"), make_metric("b"), make_metric("c")]
        results = detect_drift_many([], metrics)
        assert len(results) == 3
        assert {r.pipeline for r in results} == {"a", "b", "c"}

    def test_empty_current_returns_empty(self):
        assert detect_drift_many([], []) == []

    def test_history_scoped_per_pipeline(self):
        history = (
            [make_metric("a", failed=50) for _ in range(5)]
            + [make_metric("b", failed=0) for _ in range(5)]
        )
        current = [make_metric("a", failed=0), make_metric("b", failed=50)]
        results = detect_drift_many(history, current, DriftConfig(threshold=0.10))
        by_pipe = {r.pipeline: r for r in results}
        assert by_pipe["a"].drifted
        assert by_pipe["b"].drifted
