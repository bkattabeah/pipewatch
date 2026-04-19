"""Tests for pipewatch.checkpoint."""
import pytest
from pipewatch.checkpoint import Checkpoint, CheckpointStore, CheckpointDiff


def make_checkpoint(pipeline="pipe_a", marker="offset:100") -> Checkpoint:
    return Checkpoint(pipeline=pipeline, marker=marker)


class TestCheckpoint:
    def test_to_dict_roundtrip(self):
        cp = make_checkpoint()
        d = cp.to_dict()
        restored = Checkpoint.from_dict(d)
        assert restored.pipeline == cp.pipeline
        assert restored.marker == cp.marker
        assert restored.recorded_at == cp.recorded_at

    def test_recorded_at_set_automatically(self):
        cp = make_checkpoint()
        assert cp.recorded_at != ""

    def test_metadata_defaults_empty(self):
        cp = make_checkpoint()
        assert cp.metadata == {}


class TestCheckpointStore:
    def setup_method(self):
        self.store = CheckpointStore()

    def test_get_returns_none_when_missing(self):
        assert self.store.get("unknown") is None

    def test_record_and_retrieve(self):
        cp = make_checkpoint()
        self.store.record(cp)
        result = self.store.get("pipe_a")
        assert result is not None
        assert result.marker == "offset:100"

    def test_record_overwrites_existing(self):
        self.store.record(make_checkpoint(marker="offset:100"))
        self.store.record(make_checkpoint(marker="offset:200"))
        assert self.store.get("pipe_a").marker == "offset:200"

    def test_all_returns_all_entries(self):
        self.store.record(make_checkpoint(pipeline="a", marker="m1"))
        self.store.record(make_checkpoint(pipeline="b", marker="m2"))
        all_entries = self.store.all()
        assert set(all_entries.keys()) == {"a", "b"}

    def test_clear_removes_entry(self):
        self.store.record(make_checkpoint())
        removed = self.store.clear("pipe_a")
        assert removed is True
        assert self.store.get("pipe_a") is None

    def test_clear_returns_false_when_missing(self):
        assert self.store.clear("nonexistent") is False


class TestCheckpointDiff:
    def setup_method(self):
        self.store = CheckpointStore()

    def test_diff_changed_when_marker_differs(self):
        self.store.record(make_checkpoint(marker="offset:100"))
        diff = self.store.compare("pipe_a", "offset:200")
        assert diff.changed is True
        assert diff.previous == "offset:100"
        assert diff.current == "offset:200"

    def test_diff_unchanged_when_same_marker(self):
        self.store.record(make_checkpoint(marker="offset:100"))
        diff = self.store.compare("pipe_a", "offset:100")
        assert diff.changed is False

    def test_diff_no_previous_when_new(self):
        diff = self.store.compare("new_pipe", "offset:1")
        assert diff.previous is None
        assert diff.changed is True

    def test_diff_to_dict(self):
        diff = CheckpointDiff(pipeline="p", previous="a", current="b")
        d = diff.to_dict()
        assert d["changed"] is True
        assert d["pipeline"] == "p"
