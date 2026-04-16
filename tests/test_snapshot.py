"""Tests for snapshot capture, serialization, and store."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.snapshot import Snapshot, capture, save_snapshot, load_snapshot
from pipewatch.snapshot_store import SnapshotStore


def make_metric(pipeline_id: str = "pipe-1", processed: int = 100, failed: int = 5) -> PipelineMetric:
    return PipelineMetric(
        pipeline_id=pipeline_id,
        timestamp=time.time(),
        records_processed=processed,
        records_failed=failed,
        latency_seconds=1.2,
    )


class TestSnapshot:
    def test_capture_creates_snapshot(self):
        m = make_metric()
        snap = capture("pipe-1", [m])
        assert snap.pipeline_id == "pipe-1"
        assert len(snap.metrics) == 1
        assert snap.timestamp > 0

    def test_to_dict_roundtrip(self):
        snap = Snapshot(timestamp=1234.5, pipeline_id="p", metrics=[{"a": 1}])
        d = snap.to_dict()
        restored = Snapshot.from_dict(d)
        assert restored.pipeline_id == snap.pipeline_id
        assert restored.timestamp == snap.timestamp
        assert restored.metrics == snap.metrics

    def test_save_and_load(self, tmp_path):
        snap = capture("pipe-1", [make_metric()])
        p = tmp_path / "snap.json"
        save_snapshot(snap, p)
        loaded = load_snapshot(p)
        assert loaded is not None
        assert loaded.pipeline_id == "pipe-1"

    def test_load_missing_returns_none(self, tmp_path):
        result = load_snapshot(tmp_path / "nonexistent.json")
        assert result is None


class TestSnapshotStore:
    def setup_method(self):
        pass

    def test_save_and_latest(self, tmp_path):
        store = SnapshotStore(store_dir=tmp_path)
        snap = capture("pipe-a", [make_metric("pipe-a")])
        store.save(snap)
        latest = store.latest("pipe-a")
        assert latest is not None
        assert latest.pipeline_id == "pipe-a"

    def test_latest_returns_none_for_unknown(self, tmp_path):
        store = SnapshotStore(store_dir=tmp_path)
        assert store.latest("unknown") is None

    def test_history_capped_at_max(self, tmp_path):
        store = SnapshotStore(store_dir=tmp_path)
        for _ in range(15):
            store.save(capture("pipe-b", [make_metric("pipe-b")]), max_history=10)
        assert len(store.list("pipe-b")) == 10

    def test_clear_removes_snapshots(self, tmp_path):
        store = SnapshotStore(store_dir=tmp_path)
        store.save(capture("pipe-c", [make_metric("pipe-c")]))
        store.clear("pipe-c")
        assert store.list("pipe-c") == []
