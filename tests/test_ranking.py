"""Tests for pipewatch.ranking."""
from __future__ import annotations

from datetime import datetime

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.ranking import RankEntry, RankingResult, rank_pipelines, _score_metric


def make_metric(
    name: str,
    status: PipelineStatus,
    processed: int = 100,
    failed: int = 0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        status=status,
        processed=processed,
        failed=failed,
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
    )


class TestRankPipelines:
    def test_empty_metrics_returns_empty_result(self):
        result = rank_pipelines([])
        assert result.entries == []

    def test_single_metric_rank_is_one(self):
        m = make_metric("pipe_a", PipelineStatus.HEALTHY)
        result = rank_pipelines([m])
        assert len(result.entries) == 1
        assert result.entries[0].rank == 1

    def test_critical_ranks_above_healthy(self):
        healthy = make_metric("healthy", PipelineStatus.HEALTHY, failed=0)
        critical = make_metric("critical", PipelineStatus.CRITICAL, failed=50)
        result = rank_pipelines([healthy, critical])
        assert result.entries[0].pipeline_name == "critical"
        assert result.entries[1].pipeline_name == "healthy"

    def test_ranks_are_sequential(self):
        metrics = [
            make_metric("a", PipelineStatus.HEALTHY),
            make_metric("b", PipelineStatus.WARNING, failed=10),
            make_metric("c", PipelineStatus.CRITICAL, failed=50),
        ]
        result = rank_pipelines(metrics)
        ranks = [e.rank for e in result.entries]
        assert ranks == [1, 2, 3]

    def test_descending_default_order(self):
        metrics = [
            make_metric("low", PipelineStatus.HEALTHY, failed=0),
            make_metric("high", PipelineStatus.CRITICAL, failed=80),
        ]
        result = rank_pipelines(metrics)
        assert result.entries[0].score >= result.entries[1].score

    def test_ascending_order_when_flag_set(self):
        metrics = [
            make_metric("low", PipelineStatus.HEALTHY, failed=0),
            make_metric("high", PipelineStatus.CRITICAL, failed=80),
        ]
        result = rank_pipelines(metrics, descending=False)
        assert result.entries[0].score <= result.entries[1].score

    def test_top_returns_correct_slice(self):
        metrics = [
            make_metric(f"pipe_{i}", PipelineStatus.HEALTHY) for i in range(10)
        ]
        result = rank_pipelines(metrics)
        assert len(result.top(3)) == 3

    def test_top_larger_than_entries_returns_all(self):
        metrics = [make_metric("a", PipelineStatus.HEALTHY)]
        result = rank_pipelines(metrics)
        assert len(result.top(100)) == 1


class TestRankEntry:
    def test_to_dict_contains_expected_keys(self):
        entry = RankEntry(
            pipeline_name="pipe_x",
            score=0.75,
            status=PipelineStatus.WARNING,
            error_rate=0.25,
            rank=1,
        )
        d = entry.to_dict()
        assert set(d.keys()) == {"rank", "pipeline_name", "score", "status", "error_rate"}

    def test_to_dict_rounds_score(self):
        entry = RankEntry(
            pipeline_name="pipe_y",
            score=0.123456789,
            status=PipelineStatus.HEALTHY,
            error_rate=0.0,
            rank=2,
        )
        assert entry.to_dict()["score"] == round(0.123456789, 4)


class TestRankingResult:
    def test_to_dict_has_rankings_key(self):
        result = rank_pipelines([make_metric("p", PipelineStatus.HEALTHY)])
        d = result.to_dict()
        assert "rankings" in d
        assert isinstance(d["rankings"], list)
