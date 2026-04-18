"""Tests for pipewatch.baseline."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.snapshot import Snapshot
from pipewatch.baseline import compare_to_baseline, BaselineReport


def make_metric(pipeline: str, processed: int, failed: int) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        processed=processed,
        failed=failed,
        latency_ms=100.0,
        timestamp=datetime.now(timezone.utc),
        status=PipelineStatus.HEALTHY,
    )


def make_snapshot(metrics: List[PipelineMetric]) -> Snapshot:
    return Snapshot(taken_at=datetime.now(timezone.utc), metrics=metrics)


class TestCompareToBaseline:
    def test_no_regression_when_rates_equal(self):
        m = make_metric("pipe_a", 100, 5)
        base = make_snapshot([m])
        curr = make_snapshot([m])
        report = compare_to_baseline(base, curr)
        assert not report.any_regressions()
        assert len(report.entries) == 1
        assert report.entries[0].delta == pytest.approx(0.0)

    def test_regression_detected_above_threshold(self):
        base = make_snapshot([make_metric("pipe_a", 100, 5)])
        curr = make_snapshot([make_metric("pipe_a", 100, 20)])
        report = compare_to_baseline(base, curr, regression_threshold=0.05)
        assert report.any_regressions()
        entry = report.entries[0]
        assert entry.regressed
        assert entry.delta == pytest.approx(0.15)

    def test_no_regression_below_threshold(self):
        base = make_snapshot([make_metric("pipe_a", 100, 5)])
        curr = make_snapshot([make_metric("pipe_a", 100, 8)])
        report = compare_to_baseline(base, curr, regression_threshold=0.05)
        assert not report.any_regressions()

    def test_missing_in_baseline(self):
        base = make_snapshot([])
        curr = make_snapshot([make_metric("new_pipe", 50, 0)])
        report = compare_to_baseline(base, curr)
        assert "new_pipe" in report.missing_in_baseline
        assert report.entries == []

    def test_missing_in_current(self):
        base = make_snapshot([make_metric("old_pipe", 50, 0)])
        curr = make_snapshot([])
        report = compare_to_baseline(base, curr)
        assert "old_pipe" in report.missing_in_current

    def test_multiple_pipelines_partial_regression(self):
        base = make_snapshot([
            make_metric("a", 100, 5),
            make_metric("b", 100, 5),
        ])
        curr = make_snapshot([
            make_metric("a", 100, 5),
            make_metric("b", 100, 25),
        ])
        report = compare_to_baseline(base, curr)
        assert report.any_regressions()
        by_name = {e.pipeline: e for e in report.entries}
        assert not by_name["a"].regressed
        assert by_name["b"].regressed

    def test_to_dict_structure(self):
        base = make_snapshot([make_metric("pipe_a", 100, 10)])
        curr = make_snapshot([make_metric("pipe_a", 100, 20)])
        report = compare_to_baseline(base, curr)
        d = report.to_dict()
        assert "entries" in d
        assert "any_regressions" in d
        assert isinstance(d["entries"], list)
