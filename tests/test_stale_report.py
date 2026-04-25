"""Tests for pipewatch.stale_report."""

from __future__ import annotations

import pytest

from pipewatch.watchdog import StaleEntry
from pipewatch.stale_report import (
    StaleReport,
    build_stale_report,
    format_stale_report,
)


def make_entry(name: str, age: float, reason: str = "timeout") -> StaleEntry:
    return StaleEntry(pipeline_name=name, age_seconds=age, reason=reason)


class TestBuildStaleReport:
    def test_empty_entries_returns_zero_stats(self):
        report = build_stale_report([])
        assert report.total_stale == 0
        assert report.pipelines == []
        assert report.most_overdue is None

    def test_has_stale_false_when_empty(self):
        report = build_stale_report([])
        assert report.has_stale() is False

    def test_has_stale_true_when_entries_present(self):
        report = build_stale_report([make_entry("pipe-a", 120.0)])
        assert report.has_stale() is True

    def test_total_stale_matches_entry_count(self):
        entries = [make_entry(f"pipe-{i}", float(i * 10)) for i in range(1, 5)]
        report = build_stale_report(entries)
        assert report.total_stale == 4

    def test_most_overdue_is_largest_age(self):
        entries = [
            make_entry("pipe-a", 30.0),
            make_entry("pipe-b", 300.0),
            make_entry("pipe-c", 90.0),
        ]
        report = build_stale_report(entries)
        assert report.most_overdue is not None
        assert report.most_overdue.pipeline_name == "pipe-b"

    def test_single_entry_is_most_overdue(self):
        entry = make_entry("only-pipe", 60.0)
        report = build_stale_report([entry])
        assert report.most_overdue.pipeline_name == "only-pipe"

    def test_to_dict_keys_present(self):
        report = build_stale_report([make_entry("pipe-x", 45.0)])
        d = report.to_dict()
        assert "total_stale" in d
        assert "has_stale" in d
        assert "most_overdue" in d
        assert "pipelines" in d

    def test_to_dict_most_overdue_none_when_empty(self):
        report = build_stale_report([])
        assert report.to_dict()["most_overdue"] is None

    def test_to_dict_pipelines_serialized(self):
        entries = [make_entry("pipe-a", 50.0), make_entry("pipe-b", 25.0)]
        report = build_stale_report(entries)
        d = report.to_dict()
        assert len(d["pipelines"]) == 2
        names = {p["pipeline_name"] for p in d["pipelines"]}
        assert names == {"pipe-a", "pipe-b"}


class TestFormatStaleReport:
    def test_no_stale_message_when_empty(self):
        report = build_stale_report([])
        text = format_stale_report(report)
        assert "No stale" in text

    def test_shows_stale_count(self):
        entries = [make_entry("pipe-a", 100.0), make_entry("pipe-b", 200.0)]
        report = build_stale_report(entries)
        text = format_stale_report(report)
        assert "2" in text

    def test_shows_most_overdue_pipeline_name(self):
        entries = [make_entry("slow-pipe", 999.0), make_entry("fast-pipe", 10.0)]
        report = build_stale_report(entries)
        text = format_stale_report(report)
        assert "slow-pipe" in text

    def test_all_pipeline_names_in_output(self):
        entries = [make_entry(f"p{i}", float(i)) for i in range(1, 4)]
        report = build_stale_report(entries)
        text = format_stale_report(report)
        for i in range(1, 4):
            assert f"p{i}" in text
