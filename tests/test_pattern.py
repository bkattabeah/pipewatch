"""Tests for pipewatch.pattern and pipewatch.pattern_report."""

from __future__ import annotations

from datetime import datetime
from typing import List

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.pattern import (
    PatternConfig,
    detect_pattern,
)
from pipewatch.pattern_report import (
    PatternReport,
    build_pattern_report,
    format_pattern_report,
)


def make_metric(
    pipeline: str = "pipe",
    processed: int = 100,
    failed: int = 0,
    status: PipelineStatus = PipelineStatus.HEALTHY,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        processed=processed,
        failed=failed,
        status=status,
        timestamp=datetime.utcnow(),
    )


class TestPatternConfig:
    def test_defaults(self):
        cfg = PatternConfig()
        assert cfg.min_occurrences == 3
        assert cfg.window_size == 20
        assert cfg.error_rate_threshold == 0.1

    def test_validate_passes(self):
        PatternConfig(min_occurrences=2, window_size=5, error_rate_threshold=0.05).validate()

    def test_validate_rejects_zero_occurrences(self):
        with pytest.raises(ValueError, match="min_occurrences"):
            PatternConfig(min_occurrences=0).validate()

    def test_validate_rejects_small_window(self):
        with pytest.raises(ValueError, match="window_size"):
            PatternConfig(window_size=1).validate()

    def test_validate_rejects_out_of_range_threshold(self):
        with pytest.raises(ValueError, match="error_rate_threshold"):
            PatternConfig(error_rate_threshold=1.5).validate()


class TestDetectPattern:
    def test_returns_none_for_empty_metrics(self):
        result = detect_pattern("pipe", [])
        assert result is None

    def test_no_pattern_for_healthy_metrics(self):
        metrics = [make_metric(failed=0) for _ in range(10)]
        result = detect_pattern("pipe", metrics)
        assert result is not None
        assert not result.has_pattern

    def test_pattern_detected_when_enough_failures(self):
        metrics = [
            make_metric(failed=20, status=PipelineStatus.CRITICAL) for _ in range(5)
        ]
        cfg = PatternConfig(min_occurrences=3, error_rate_threshold=0.1)
        result = detect_pattern("pipe", metrics, cfg)
        assert result is not None
        assert result.has_pattern

    def test_pattern_not_detected_below_min_occurrences(self):
        metrics = [
            make_metric(failed=20, status=PipelineStatus.CRITICAL) for _ in range(2)
        ] + [make_metric() for _ in range(8)]
        cfg = PatternConfig(min_occurrences=3)
        result = detect_pattern("pipe", metrics, cfg)
        assert result is not None
        assert not result.has_pattern

    def test_to_dict_contains_expected_keys(self):
        metrics = [make_metric(failed=15, status=PipelineStatus.WARNING) for _ in range(4)]
        result = detect_pattern("pipe", metrics)
        d = result.to_dict()
        assert "pipeline" in d
        assert "has_pattern" in d
        assert "matches" in d

    def test_window_limits_inspection(self):
        # first 5 are critical, last 20 are healthy — window=20 should see only healthy
        old = [make_metric(failed=50, status=PipelineStatus.CRITICAL) for _ in range(5)]
        recent = [make_metric(failed=0) for _ in range(20)]
        cfg = PatternConfig(min_occurrences=3, window_size=20)
        result = detect_pattern("pipe", old + recent, cfg)
        assert result is not None
        assert not result.has_pattern


class TestBuildPatternReport:
    def test_empty_input_returns_empty_report(self):
        report = build_pattern_report({})
        assert report.total == 0
        assert report.recurring_count == 0

    def test_counts_recurring_pipelines(self):
        critical = [make_metric(failed=20, status=PipelineStatus.CRITICAL) for _ in range(5)]
        healthy = [make_metric() for _ in range(10)]
        report = build_pattern_report({"bad": critical, "good": healthy})
        assert report.total == 2
        assert report.recurring_count == 1
        assert report.clean_count == 1

    def test_to_dict_structure(self):
        report = build_pattern_report({})
        d = report.to_dict()
        assert set(d.keys()) == {"total", "recurring_count", "clean_count", "results"}

    def test_format_returns_string(self):
        critical = [make_metric("pipe_a", failed=20, status=PipelineStatus.CRITICAL) for _ in range(4)]
        report = build_pattern_report({"pipe_a": critical})
        text = format_pattern_report(report)
        assert "Pattern Report" in text
        assert "pipe_a" in text
