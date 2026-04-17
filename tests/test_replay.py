"""Tests for pipewatch.replay."""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from pipewatch.replay import ReplayConfig, ReplayFrame, load_snapshots, replay
from pipewatch.snapshot import Snapshot
from pipewatch.metrics import PipelineMetric, PipelineStatus


def make_snapshot(name="pipe1", ts=None):
    metric = PipelineMetric(
        pipeline=name,
        total_runs=10,
        failed_runs=1,
        avg_duration_seconds=5.0,
        status=PipelineStatus.HEALTHY,
        timestamp=datetime(2024, 1, 1),
    )
    return Snapshot(
        taken_at=ts or datetime(2024, 1, 1),
        metrics={name: metric},
    )


def _mock_store(snapshots):
    store = MagicMock()
    store.list.return_value = [f"snap{i}" for i in range(len(snapshots))]
    store.load.side_effect = snapshots
    return store


@patch("pipewatch.replay.SnapshotStore")
def test_load_snapshots_respects_limit(MockStore):
    snaps = [make_snapshot() for _ in range(5)]
    MockStore.return_value = _mock_store(snaps)
    config = ReplayConfig(store_dir=".", limit=3)
    result = load_snapshots(config)
    assert len(result) == 3


@patch("pipewatch.replay.SnapshotStore")
def test_load_snapshots_reverse(MockStore):
    snaps = [make_snapshot() for _ in range(3)]
    MockStore.return_value = _mock_store(snaps)
    config = ReplayConfig(store_dir=".", limit=3, reverse=True)
    result = load_snapshots(config)
    assert len(result) == 3


@patch("pipewatch.replay.SnapshotStore")
def test_replay_yields_frames(MockStore):
    snaps = [make_snapshot() for _ in range(3)]
    MockStore.return_value = _mock_store(snaps)
    config = ReplayConfig(store_dir=".")
    frames = list(replay(config))
    assert len(frames) == 3
    assert frames[-1].is_last is True
    assert frames[0].is_last is False


@patch("pipewatch.replay.SnapshotStore")
def test_replay_filters_by_pipeline(MockStore):
    s1 = make_snapshot(name="pipe1")
    s2 = make_snapshot(name="pipe2")
    MockStore.return_value = _mock_store([s1, s2])
    config = ReplayConfig(store_dir=".", pipeline="pipe1")
    frames = list(replay(config))
    assert all("pipe1" in f.snapshot.metrics for f in frames)


def test_replay_frame_to_dict():
    snap = make_snapshot()
    frame = ReplayFrame(index=0, snapshot=snap, is_last=True)
    d = frame.to_dict()
    assert d["index"] == 0
    assert d["is_last"] is True
    assert "snapshot" in d


@patch("pipewatch.replay.SnapshotStore")
def test_replay_empty_store(MockStore):
    store = MagicMock()
    store.list.return_value = []
    MockStore.return_value = store
    config = ReplayConfig(store_dir=".")
    frames = list(replay(config))
    assert frames == []
