"""Tests for pipewatch.retention."""
from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from pipewatch.retention import RetentionConfig, RetentionResult, apply_retention


def make_snapshot(sid: str, days_ago: float = 0):
    s = MagicMock()
    s.snapshot_id = sid
    s.timestamp = datetime.utcnow() - timedelta(days=days_ago)
    return s


class TestRetentionConfig:
    def test_defaults(self):
        cfg = RetentionConfig()
        assert cfg.max_snapshots == 50
        assert cfg.max_age_days == 30

    def test_validate_passes(self):
        RetentionConfig(max_snapshots=10, max_age_days=7).validate()

    def test_validate_rejects_zero_snapshots(self):
        with pytest.raises(ValueError, match="max_snapshots"):
            RetentionConfig(max_snapshots=0).validate()

    def test_validate_rejects_zero_age(self):
        with pytest.raises(ValueError, match="max_age_days"):
            RetentionConfig(max_age_days=0).validate()


class TestApplyRetention:
    def test_empty_list(self):
        result = apply_retention([], RetentionConfig())
        assert result.kept == 0
        assert result.removed == []

    def test_recent_snapshots_kept(self):
        snaps = [make_snapshot(f"s{i}", days_ago=i) for i in range(5)]
        result = apply_retention(snaps, RetentionConfig(max_snapshots=10, max_age_days=30))
        assert result.kept == 5
        assert result.removed == []

    def test_old_snapshots_removed(self):
        snaps = [
            make_snapshot("old", days_ago=40),
            make_snapshot("new", days_ago=1),
        ]
        result = apply_retention(snaps, RetentionConfig(max_age_days=30))
        assert "old" in result.removed
        assert "new" not in result.removed
        assert result.kept == 1

    def test_excess_count_removed(self):
        snaps = [make_snapshot(f"s{i}", days_ago=i) for i in range(10)]
        result = apply_retention(snaps, RetentionConfig(max_snapshots=3, max_age_days=365))
        assert result.kept == 3
        assert len(result.removed) == 7

    def test_to_dict_has_expected_keys(self):
        result = RetentionResult(removed=["a", "b"], kept=5)
        d = result.to_dict()
        assert d["kept"] == 5
        assert d["total_removed"] == 2
        assert "removed" in d
