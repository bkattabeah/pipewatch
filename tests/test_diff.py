"""Tests for snapshot diffing."""
import pytest
from unittest.mock import MagicMock
from pipewatch.diff import diff_snapshots, SnapshotDiff, MetricDiff


def make_snapshot(reports: list):
    snap = MagicMock()
    snap.to_dict.return_value = {"reports": reports}
    return snap


REPORT_A_HEALTHY = {"pipeline": "etl_a", "status": "healthy", "error_rate": 0.0}
REPORT_A_WARN = {"pipeline": "etl_a", "status": "warning", "error_rate": 0.12}
REPORT_B = {"pipeline": "etl_b", "status": "healthy", "error_rate": 0.01}


class TestDiffSnapshots:
    def test_no_changes(self):
        old = make_snapshot([REPORT_A_HEALTHY])
        new = make_snapshot([REPORT_A_HEALTHY])
        result = diff_snapshots(old, new)
        assert not result.has_changes()

    def test_added_pipeline(self):
        old = make_snapshot([REPORT_A_HEALTHY])
        new = make_snapshot([REPORT_A_HEALTHY, REPORT_B])
        result = diff_snapshots(old, new)
        assert "etl_b" in result.added
        assert result.removed == []
        assert result.has_changes()

    def test_removed_pipeline(self):
        old = make_snapshot([REPORT_A_HEALTHY, REPORT_B])
        new = make_snapshot([REPORT_A_HEALTHY])
        result = diff_snapshots(old, new)
        assert "etl_b" in result.removed
        assert result.added == []

    def test_status_change_detected(self):
        old = make_snapshot([REPORT_A_HEALTHY])
        new = make_snapshot([REPORT_A_WARN])
        result = diff_snapshots(old, new)
        assert len(result.changed) == 1
        ch = result.changed[0]
        assert ch.pipeline == "etl_a"
        assert ch.old_status == "healthy"
        assert ch.new_status == "warning"
        assert ch.status_changed

    def test_error_rate_delta(self):
        old = make_snapshot([REPORT_A_HEALTHY])
        new = make_snapshot([REPORT_A_WARN])
        result = diff_snapshots(old, new)
        ch = result.changed[0]
        assert ch.error_rate_delta == pytest.approx(0.12, abs=1e-4)

    def test_to_dict_structure(self):
        old = make_snapshot([REPORT_A_HEALTHY])
        new = make_snapshot([REPORT_A_WARN, REPORT_B])
        result = diff_snapshots(old, new)
        d = result.to_dict()
        assert "added" in d
        assert "removed" in d
        assert "changed" in d
        assert d["added"] == ["etl_b"]

    def test_no_delta_when_missing_rates(self):
        diff = MetricDiff(
            pipeline="x",
            old_status="healthy",
            new_status="critical",
            old_error_rate=None,
            new_error_rate=0.5,
        )
        assert diff.error_rate_delta is None
