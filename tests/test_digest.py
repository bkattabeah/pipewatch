"""Tests for pipewatch.digest."""
import pytest
from datetime import datetime

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.digest import DigestConfig, DigestEntry, build_digest


def make_metric(pipeline_id, status, total, failed):
    return PipelineMetric(
        pipeline_id=pipeline_id,
        status=status,
        total_records=total,
        failed_records=failed,
        timestamp=datetime.utcnow(),
    )


class TestBuildDigest:
    def test_empty_metrics_returns_zero_stats(self):
        d = build_digest([])
        assert d.stats.total == 0
        assert d.entries == []

    def test_healthy_excluded_by_default(self):
        metrics = [
            make_metric("p1", PipelineStatus.HEALTHY, 100, 0),
            make_metric("p2", PipelineStatus.CRITICAL, 100, 80),
        ]
        d = build_digest(metrics)
        ids = [e.pipeline_id for e in d.entries]
        assert "p1" not in ids
        assert "p2" in ids

    def test_include_healthy_flag(self):
        metrics = [
            make_metric("p1", PipelineStatus.HEALTHY, 100, 0),
        ]
        config = DigestConfig(include_healthy=True)
        d = build_digest(metrics, config)
        assert any(e.pipeline_id == "p1" for e in d.entries)

    def test_top_n_limits_entries(self):
        metrics = [
            make_metric(f"p{i}", PipelineStatus.WARNING, 100, 10 + i)
            for i in range(10)
        ]
        config = DigestConfig(top_n_worst=3)
        d = build_digest(metrics, config)
        assert len(d.entries) == 3

    def test_entries_sorted_by_error_rate_descending(self):
        metrics = [
            make_metric("low", PipelineStatus.WARNING, 100, 5),
            make_metric("high", PipelineStatus.CRITICAL, 100, 90),
            make_metric("mid", PipelineStatus.WARNING, 100, 40),
        ]
        d = build_digest(metrics)
        rates = [e.error_rate for e in d.entries]
        assert rates == sorted(rates, reverse=True)

    def test_to_dict_structure(self):
        metrics = [make_metric("p1", PipelineStatus.CRITICAL, 100, 50)]
        d = build_digest(metrics)
        result = d.to_dict()
        assert "title" in result
        assert "generated_at" in result
        assert "stats" in result
        assert "entries" in result
        assert isinstance(result["entries"], list)

    def test_stats_counts_correct(self):
        metrics = [
            make_metric("p1", PipelineStatus.HEALTHY, 100, 0),
            make_metric("p2", PipelineStatus.WARNING, 100, 20),
            make_metric("p3", PipelineStatus.CRITICAL, 100, 80),
        ]
        d = build_digest(metrics)
        assert d.stats.total == 3
        assert d.stats.healthy == 1
        assert d.stats.warning == 1
        assert d.stats.critical == 1

    def test_custom_title(self):
        config = DigestConfig(title="My Custom Digest")
        d = build_digest([], config)
        assert d.title == "My Custom Digest"
