"""Tests for pipewatch.spillover."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.spillover import SpilloverConfig, SpilloverResult, detect_spillover


def make_metric(
    pipeline: str = "pipe_a",
    total: int = 100,
    failed: int = 0,
    ts: float = 0.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        total=total,
        failed=failed,
        timestamp=ts,
        status=PipelineStatus.HEALTHY,
    )


class TestSpilloverConfig:
    def test_defaults(self) -> None:
        cfg = SpilloverConfig()
        assert cfg.window == 10
        assert cfg.threshold == 0.25
        assert cfg.min_samples == 3

    def test_validate_passes(self) -> None:
        SpilloverConfig(window=5, threshold=0.5, min_samples=2).validate()

    def test_validate_rejects_zero_window(self) -> None:
        with pytest.raises(ValueError, match="window"):
            SpilloverConfig(window=0).validate()

    def test_validate_rejects_bad_threshold(self) -> None:
        with pytest.raises(ValueError, match="threshold"):
            SpilloverConfig(threshold=1.5).validate()

    def test_validate_rejects_zero_min_samples(self) -> None:
        with pytest.raises(ValueError, match="min_samples"):
            SpilloverConfig(min_samples=0).validate()


class TestDetectSpillover:
    def test_empty_metrics_returns_empty(self) -> None:
        assert detect_spillover([]) == []

    def test_insufficient_samples_excluded(self) -> None:
        metrics = [make_metric(failed=50, ts=float(i)) for i in range(2)]
        results = detect_spillover(metrics, SpilloverConfig(min_samples=3))
        assert results == []

    def test_healthy_pipeline_not_spilling(self) -> None:
        metrics = [make_metric(failed=5, ts=float(i)) for i in range(5)]
        results = detect_spillover(metrics, SpilloverConfig(threshold=0.25, min_samples=3))
        assert len(results) == 1
        assert results[0].spilling is False

    def test_high_error_rate_flagged_as_spilling(self) -> None:
        metrics = [make_metric(failed=40, ts=float(i)) for i in range(5)]
        results = detect_spillover(metrics, SpilloverConfig(threshold=0.25, min_samples=3))
        assert len(results) == 1
        assert results[0].spilling is True
        assert results[0].avg_error_rate == pytest.approx(0.40)

    def test_window_limits_samples_used(self) -> None:
        # first 5 metrics are bad, last 5 are good
        bad = [make_metric(failed=80, ts=float(i)) for i in range(5)]
        good = [make_metric(failed=0, ts=float(i + 5)) for i in range(5)]
        results = detect_spillover(bad + good, SpilloverConfig(window=5, min_samples=3))
        assert results[0].spilling is False

    def test_multiple_pipelines_reported_separately(self) -> None:
        m_a = [make_metric("pipe_a", failed=50, ts=float(i)) for i in range(4)]
        m_b = [make_metric("pipe_b", failed=0, ts=float(i)) for i in range(4)]
        results = detect_spillover(m_a + m_b, SpilloverConfig(min_samples=3))
        names = {r.pipeline for r in results}
        assert names == {"pipe_a", "pipe_b"}
        spilling = {r.pipeline: r.spilling for r in results}
        assert spilling["pipe_a"] is True
        assert spilling["pipe_b"] is False

    def test_to_dict_keys(self) -> None:
        metrics = [make_metric(failed=30, ts=float(i)) for i in range(4)]
        result = detect_spillover(metrics, SpilloverConfig(min_samples=3))[0]
        d = result.to_dict()
        assert set(d.keys()) == {
            "pipeline",
            "avg_error_rate",
            "sample_count",
            "threshold",
            "spilling",
        }
