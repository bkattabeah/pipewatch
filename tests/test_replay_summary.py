"""Tests for pipewatch.replay_summary."""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from pipewatch.replay import ReplayConfig
from pipewatch.replay_summary import summarize_replay, PipelineReplaySummary
from pipewatch.snapshot import Snapshot
from pipewatch.metrics import PipelineMetric, PipelineStatus


def make_metric(pipeline="pipe1", total=10, failed=1, status=PipelineStatus.HEALTHY):
    return PipelineMetric(
        pipeline=pipeline,
        total_runs=total,
        failed_runs=failed,
        avg_duration_seconds=3.0,
        status=status,
        timestamp=datetime(2024, 1, 1),
    )


def make_snapshot(metrics: dict):
    return Snapshot(taken_at=datetime(2024, 1, 1), metrics=metrics)


@patch("pipewatch.replay_summary.replay")
def test_summary_counts_frames(mock_replay):
    from pipewatch.replay import ReplayFrame
    snaps = [
        ReplayFrame(0, make_snapshot({"p": make_metric()}), False),
        ReplayFrame(1, make_snapshot({"p": make_metric()}), True),
    ]
    mock_replay.return_value = iter(snaps)
    config = ReplayConfig(store_dir=".")
    result = summarize_replay(config)
    assert len(result) == 1
    assert result[0].frames_seen == 2


@patch("pipewatch.replay_summary.replay")
def test_summary_detects_status_change(mock_replay):
    from pipewatch.replay import ReplayFrame
    snaps = [
        ReplayFrame(0, make_snapshot({"p": make_metric(status=PipelineStatus.HEALTHY)}), False),
        ReplayFrame(1, make_snapshot({"p": make_metric(status=PipelineStatus.CRITICAL)}), True),
    ]
    mock_replay.return_value = iter(snaps)
    config = ReplayConfig(store_dir=".")
    result = summarize_replay(config)
    assert result[0].status_changes == 1


@patch("pipewatch.replay_summary.replay")
def test_summary_error_rate_range(mock_replay):
    from pipewatch.replay import ReplayFrame
    snaps = [
        ReplayFrame(0, make_snapshot({"p": make_metric(total=10, failed=1)}), False),
        ReplayFrame(1, make_snapshot({"p": make_metric(total=10, failed=5)}), True),
    ]
    mock_replay.return_value = iter(snaps)
    config = ReplayConfig(store_dir=".")
    result = summarize_replay(config)
    assert result[0].min_error_rate == pytest.approx(0.1)
    assert result[0].max_error_rate == pytest.approx(0.5)


@patch("pipewatch.replay_summary.replay")
def test_summary_to_dict(mock_replay):
    from pipewatch.replay import ReplayFrame
    snaps = [ReplayFrame(0, make_snapshot({"p": make_metric()}), True)]
    mock_replay.return_value = iter(snaps)
    config = ReplayConfig(store_dir=".")
    result = summarize_replay(config)
    d = result[0].to_dict()
    assert "pipeline" in d
    assert "status_changes" in d
    assert "final_status" in d
