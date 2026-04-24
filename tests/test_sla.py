"""Tests for pipewatch.sla module."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.sla import SLAConfig, SLAResult, check_sla, check_all_slas


def make_metric(processed: int, failed: int, pipeline: str = "pipe-a") -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        records_processed=processed,
        records_failed=failed,
        status=PipelineStatus.HEALTHY,
        timestamp=datetime.now(timezone.utc),
    )


class TestSLAConfig:
    def test_defaults(self):
        cfg = SLAConfig()
        assert cfg.max_error_rate == 0.05
        assert cfg.window_minutes == 60
        assert cfg.min_samples == 5

    def test_validate_passes(self):
        SLAConfig(max_error_rate=0.1, window_minutes=30, min_samples=3).validate()

    def test_validate_rejects_negative_error_rate(self):
        with pytest.raises(ValueError, match="max_error_rate"):
            SLAConfig(max_error_rate=-0.1).validate()

    def test_validate_rejects_error_rate_above_one(self):
        with pytest.raises(ValueError, match="max_error_rate"):
            SLAConfig(max_error_rate=1.5).validate()

    def test_validate_rejects_zero_window(self):
        with pytest.raises(ValueError, match="window_minutes"):
            SLAConfig(window_minutes=0).validate()

    def test_validate_rejects_zero_min_samples(self):
        with pytest.raises(ValueError, match="min_samples"):
            SLAConfig(min_samples=0).validate()


class TestCheckSLA:
    def test_insufficient_samples_returns_compliant(self):
        metrics = [make_metric(100, 10)]
        result = check_sla("pipe-a", metrics, SLAConfig(min_samples=5))
        assert result.compliant is True
        assert "Insufficient" in result.message

    def test_compliant_when_error_rate_below_threshold(self):
        metrics = [make_metric(1000, 10)] * 5  # 1% error rate
        result = check_sla("pipe-a", metrics, SLAConfig(max_error_rate=0.05))
        assert result.compliant is True
        assert result.error_rate == pytest.approx(0.01)
        assert "SLA met" in result.message

    def test_breached_when_error_rate_exceeds_threshold(self):
        metrics = [make_metric(100, 20)] * 5  # 20% error rate
        result = check_sla("pipe-a", metrics, SLAConfig(max_error_rate=0.05))
        assert result.compliant is False
        assert result.error_rate == pytest.approx(0.20)
        assert "breached" in result.message

    def test_zero_processed_treated_as_zero_error_rate(self):
        metrics = [make_metric(0, 0)] * 5
        result = check_sla("pipe-a", metrics, SLAConfig(max_error_rate=0.05))
        assert result.compliant is True
        assert result.error_rate == 0.0

    def test_to_dict_contains_expected_keys(self):
        metrics = [make_metric(100, 5)] * 5
        result = check_sla("pipe-a", metrics, SLAConfig())
        d = result.to_dict()
        assert set(d.keys()) == {"pipeline", "compliant", "error_rate", "max_error_rate", "sample_count", "checked_at", "message"}

    def test_sample_count_reflects_input(self):
        metrics = [make_metric(100, 1)] * 7
        result = check_sla("pipe-a", metrics, SLAConfig())
        assert result.sample_count == 7


class TestCheckAllSLAs:
    def test_returns_result_per_pipeline(self):
        history = {
            "pipe-a": [make_metric(100, 1, "pipe-a")] * 5,
            "pipe-b": [make_metric(100, 50, "pipe-b")] * 5,
        }
        results = check_all_slas(history, SLAConfig(max_error_rate=0.05))
        assert len(results) == 2
        names = {r.pipeline for r in results}
        assert names == {"pipe-a", "pipe-b"}

    def test_empty_history_returns_empty_list(self):
        results = check_all_slas({}, SLAConfig())
        assert results == []
