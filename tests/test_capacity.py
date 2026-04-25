"""Tests for pipewatch.capacity."""
from __future__ import annotations

import pytest
from datetime import datetime

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.capacity import (
    CapacityConfig,
    CapacityResult,
    compute_capacity,
)


def make_metric(pipeline: str, processed: int, failed: int) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        processed=processed,
        failed=failed,
        status=PipelineStatus.HEALTHY,
        timestamp=datetime.utcnow(),
    )


# ---------------------------------------------------------------------------
# CapacityConfig
# ---------------------------------------------------------------------------

class TestCapacityConfig:
    def test_defaults(self):
        cfg = CapacityConfig()
        assert cfg.window_size == 60
        assert cfg.headroom_warn_pct == 0.75
        assert cfg.headroom_crit_pct == 0.90

    def test_validate_passes(self):
        CapacityConfig().validate()  # should not raise

    def test_validate_rejects_zero_window(self):
        with pytest.raises(ValueError, match="window_size"):
            CapacityConfig(window_size=0).validate()

    def test_validate_rejects_warn_gte_crit(self):
        with pytest.raises(ValueError, match="less than"):
            CapacityConfig(headroom_warn_pct=0.9, headroom_crit_pct=0.9).validate()

    def test_validate_rejects_warn_out_of_range(self):
        with pytest.raises(ValueError, match="headroom_warn_pct"):
            CapacityConfig(headroom_warn_pct=0.0).validate()


# ---------------------------------------------------------------------------
# compute_capacity
# ---------------------------------------------------------------------------

class TestComputeCapacity:
    def test_returns_none_for_empty_list(self):
        assert compute_capacity("pipe", []) is None

    def test_returns_none_for_wrong_pipeline(self):
        m = make_metric("other", 100, 5)
        assert compute_capacity("pipe", [m]) is None

    def test_ok_status_low_error_rate(self):
        metrics = [make_metric("pipe", 1000, 10)]
        result = compute_capacity("pipe", metrics)
        assert result is not None
        assert result.status == "ok"
        assert result.utilisation == pytest.approx(0.01)
        assert result.headroom == pytest.approx(0.99)

    def test_warn_status(self):
        cfg = CapacityConfig(headroom_warn_pct=0.75, headroom_crit_pct=0.90)
        metrics = [make_metric("pipe", 100, 80)]
        result = compute_capacity("pipe", metrics, cfg)
        assert result is not None
        assert result.status == "warn"

    def test_critical_status(self):
        cfg = CapacityConfig(headroom_warn_pct=0.75, headroom_crit_pct=0.90)
        metrics = [make_metric("pipe", 100, 95)]
        result = compute_capacity("pipe", metrics, cfg)
        assert result is not None
        assert result.status == "critical"

    def test_window_limits_samples(self):
        metrics = [make_metric("pipe", 100, 5)] * 10
        cfg = CapacityConfig(window_size=3)
        result = compute_capacity("pipe", metrics, cfg)
        assert result is not None
        assert result.sample_count == 3

    def test_to_dict_keys(self):
        metrics = [make_metric("pipe", 200, 20)]
        result = compute_capacity("pipe", metrics)
        d = result.to_dict()
        for key in ("pipeline", "total_processed", "total_failed",
                    "utilisation", "headroom", "status", "sample_count"):
            assert key in d

    def test_zero_processed_gives_zero_utilisation(self):
        metrics = [make_metric("pipe", 0, 0)]
        result = compute_capacity("pipe", metrics)
        assert result is not None
        assert result.utilisation == 0.0
        assert result.status == "ok"
