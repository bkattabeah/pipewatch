"""Tests for pipewatch.maturity."""

from __future__ import annotations

import pytest
from datetime import datetime

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.maturity import MaturityConfig, MaturityResult, compute_maturity, _grade


def make_metric(status: PipelineStatus, pipeline: str = "pipe") -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        processed=100,
        failed=0 if status == PipelineStatus.HEALTHY else 20,
        duration_seconds=1.0,
        timestamp=datetime.utcnow(),
        status=status,
    )


def healthy_batch(n: int, pipeline: str = "pipe") -> list:
    return [make_metric(PipelineStatus.HEALTHY, pipeline) for _ in range(n)]


# --- MaturityConfig ---

class TestMaturityConfig:
    def test_defaults(self):
        cfg = MaturityConfig()
        assert cfg.min_samples == 10
        assert cfg.stable_window == 20

    def test_validate_passes(self):
        MaturityConfig().validate()  # should not raise

    def test_validate_rejects_zero_min_samples(self):
        with pytest.raises(ValueError, match="min_samples"):
            MaturityConfig(min_samples=0).validate()

    def test_validate_rejects_window_smaller_than_min(self):
        with pytest.raises(ValueError, match="stable_window"):
            MaturityConfig(min_samples=15, stable_window=10).validate()

    def test_validate_rejects_critical_exceeds_warning(self):
        with pytest.raises(ValueError, match="max_critical_rate"):
            MaturityConfig(max_critical_rate=0.5, max_warning_rate=0.1).validate()


# --- compute_maturity ---

class TestComputeMaturity:
    def test_returns_none_when_too_few_samples(self):
        metrics = healthy_batch(5)
        result = compute_maturity("pipe", metrics, MaturityConfig(min_samples=10))
        assert result is None

    def test_returns_result_at_exact_min_samples(self):
        metrics = healthy_batch(10)
        result = compute_maturity("pipe", metrics)
        assert result is not None
        assert result.pipeline == "pipe"

    def test_all_healthy_gives_high_score(self):
        metrics = healthy_batch(20)
        result = compute_maturity("pipe", metrics)
        assert result is not None
        assert result.score >= 0.9
        assert result.grade == "A"
        assert result.stable is True

    def test_all_critical_gives_low_score(self):
        metrics = [make_metric(PipelineStatus.CRITICAL) for _ in range(20)]
        result = compute_maturity("pipe", metrics)
        assert result is not None
        assert result.score == 0.0
        assert result.grade == "F"
        assert result.stable is False

    def test_mixed_warning_reduces_score(self):
        healthy = healthy_batch(15)
        warnings = [make_metric(PipelineStatus.WARNING) for _ in range(5)]
        result = compute_maturity("pipe", healthy + warnings)
        assert result is not None
        assert result.score < 1.0

    def test_to_dict_contains_expected_keys(self):
        metrics = healthy_batch(20)
        result = compute_maturity("pipe", metrics)
        d = result.to_dict()
        for key in ("pipeline", "score", "grade", "sample_count", "critical_ratio", "warning_ratio", "stable"):
            assert key in d

    def test_uses_only_last_stable_window_metrics(self):
        # First 10 critical, last 20 healthy — should score well
        old = [make_metric(PipelineStatus.CRITICAL) for _ in range(10)]
        recent = healthy_batch(20)
        result = compute_maturity("pipe", old + recent, MaturityConfig(min_samples=10, stable_window=20))
        assert result is not None
        assert result.critical_ratio == 0.0


# --- _grade ---

def test_grade_a():
    assert _grade(0.95) == "A"

def test_grade_b():
    assert _grade(0.80) == "B"

def test_grade_c():
    assert _grade(0.60) == "C"

def test_grade_d():
    assert _grade(0.40) == "D"

def test_grade_f():
    assert _grade(0.20) == "F"
