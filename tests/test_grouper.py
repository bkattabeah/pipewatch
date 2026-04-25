"""Tests for pipewatch.grouper."""

from __future__ import annotations

from datetime import datetime
from typing import List

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.grouper import GroupStats, group_metrics, group_by_prefix


def make_metric(
    pipeline_id: str,
    status: PipelineStatus = PipelineStatus.HEALTHY,
    error_rate: float = 0.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_id=pipeline_id,
        status=status,
        error_rate=error_rate,
        total_records=100,
        failed_records=int(error_rate * 100),
        recorded_at=datetime(2024, 1, 1, 12, 0, 0),
    )


class TestGroupMetrics:
    def test_empty_returns_empty_report(self):
        report = group_metrics([], key_fn=lambda m: "all")
        assert report.groups == {}

    def test_single_metric_creates_group(self):
        m = make_metric("etl_sales")
        report = group_metrics([m], key_fn=lambda x: "all")
        assert "all" in report.groups
        assert report.groups["all"].total == 1

    def test_fallback_key_used_when_fn_returns_none(self):
        m = make_metric("etl_sales")
        report = group_metrics([m], key_fn=lambda x: None, fallback="misc")
        assert "misc" in report.groups

    def test_status_counts_correct(self):
        metrics = [
            make_metric("a", PipelineStatus.HEALTHY),
            make_metric("b", PipelineStatus.WARNING),
            make_metric("c", PipelineStatus.CRITICAL),
            make_metric("d", PipelineStatus.UNKNOWN),
        ]
        report = group_metrics(metrics, key_fn=lambda m: "all")
        g = report.groups["all"]
        assert g.healthy == 1
        assert g.warning == 1
        assert g.critical == 1
        assert g.unknown == 1
        assert g.total == 4

    def test_avg_error_rate_computed(self):
        metrics = [
            make_metric("etl_a", error_rate=0.1),
            make_metric("etl_b", error_rate=0.3),
        ]
        report = group_metrics(metrics, key_fn=lambda m: "grp")
        assert abs(report.groups["grp"].avg_error_rate - 0.2) < 1e-6

    def test_multiple_groups(self):
        metrics = [
            make_metric("etl_sales"),
            make_metric("etl_orders"),
            make_metric("ml_predict"),
        ]
        report = group_metrics(metrics, key_fn=lambda m: m.pipeline_id.split("_")[0])
        assert "etl" in report.groups
        assert "ml" in report.groups
        assert report.groups["etl"].total == 2
        assert report.groups["ml"].total == 1


class TestGroupByPrefix:
    def test_groups_by_first_segment(self):
        metrics = [
            make_metric("etl_sales"),
            make_metric("etl_orders"),
            make_metric("ml_forecast"),
        ]
        report = group_by_prefix(metrics)
        assert set(report.groups.keys()) == {"etl", "ml"}

    def test_no_separator_single_group_per_pipeline(self):
        metrics = [make_metric("etlsales"), make_metric("etlorders")]
        report = group_by_prefix(metrics, separator="_")
        # no underscore -> each id is its own prefix
        assert "etlsales" in report.groups
        assert "etlorders" in report.groups

    def test_to_dict_contains_all_groups(self):
        metrics = [make_metric("a_x"), make_metric("b_y")]
        report = group_by_prefix(metrics)
        d = report.to_dict()
        assert "a" in d and "b" in d

    def test_sorted_by_critical_descending(self):
        metrics = [
            make_metric("a_1", PipelineStatus.HEALTHY),
            make_metric("b_1", PipelineStatus.CRITICAL),
            make_metric("b_2", PipelineStatus.CRITICAL),
        ]
        report = group_by_prefix(metrics)
        rows = report.sorted_by("critical")
        assert rows[0].key == "b"
