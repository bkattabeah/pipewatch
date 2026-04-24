"""Tests for pipewatch.reaper."""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from pipewatch.reaper import AlertReaper, ReaperConfig, ReapedEntry


def _make_incident(pipeline: str, rule: str, opened_at: datetime, open: bool = True) -> MagicMock:
    alert = MagicMock()
    alert.pipeline = pipeline
    alert.rule_name = rule

    inc = MagicMock()
    inc.is_open = open
    inc.alert = alert
    inc.opened_at = opened_at
    return inc


class TestReaperConfig:
    def test_defaults(self):
        cfg = ReaperConfig()
        assert cfg.ttl_seconds == 3600
        assert cfg.max_reaped_per_run == 100

    def test_validate_passes(self):
        ReaperConfig(ttl_seconds=60, max_reaped_per_run=10).validate()

    def test_validate_rejects_zero_ttl(self):
        with pytest.raises(ValueError, match="ttl_seconds"):
            ReaperConfig(ttl_seconds=0).validate()

    def test_validate_rejects_zero_max(self):
        with pytest.raises(ValueError, match="max_reaped_per_run"):
            ReaperConfig(max_reaped_per_run=0).validate()


class TestAlertReaper:
    def setup_method(self):
        self.now = datetime(2024, 6, 1, 12, 0, 0)
        self.reaper = AlertReaper(ReaperConfig(ttl_seconds=300, max_reaped_per_run=50))

    def _old(self, minutes: int = 10) -> datetime:
        return self.now - timedelta(minutes=minutes)

    def test_no_incidents_returns_empty_result(self):
        result = self.reaper.reap([], now=self.now)
        assert result.total_reaped == 0
        assert result.skipped == 0

    def test_fresh_incident_not_reaped(self):
        inc = _make_incident("pipe", "rule", opened_at=self.now - timedelta(seconds=10))
        result = self.reaper.reap([inc], now=self.now)
        assert result.total_reaped == 0
        inc.resolve.assert_not_called()

    def test_stale_incident_is_reaped(self):
        inc = _make_incident("pipe", "high_error", opened_at=self._old(minutes=10))
        result = self.reaper.reap([inc], now=self.now)
        assert result.total_reaped == 1
        inc.resolve.assert_called_once()
        assert result.reaped[0].pipeline == "pipe"
        assert result.reaped[0].alert_id == "high_error"

    def test_closed_incident_skipped(self):
        inc = _make_incident("pipe", "rule", opened_at=self._old(minutes=10), open=False)
        result = self.reaper.reap([inc], now=self.now)
        assert result.total_reaped == 0
        inc.resolve.assert_not_called()

    def test_max_reaped_per_run_respected(self):
        reaper = AlertReaper(ReaperConfig(ttl_seconds=300, max_reaped_per_run=2))
        incidents = [_make_incident(f"pipe{i}", "rule", opened_at=self._old(10)) for i in range(5)]
        result = reaper.reap(incidents, now=self.now)
        assert result.total_reaped == 2
        assert result.skipped == 3

    def test_log_accumulates_entries(self):
        inc = _make_incident("pipe", "rule", opened_at=self._old(10))
        self.reaper.reap([inc], now=self.now)
        assert len(self.reaper.log()) == 1

    def test_clear_log(self):
        inc = _make_incident("pipe", "rule", opened_at=self._old(10))
        self.reaper.reap([inc], now=self.now)
        self.reaper.clear_log()
        assert self.reaper.log() == []

    def test_to_dict_structure(self):
        inc = _make_incident("pipe", "rule", opened_at=self._old(10))
        result = self.reaper.reap([inc], now=self.now)
        d = result.to_dict()
        assert "total_reaped" in d
        assert "skipped" in d
        assert "reaped" in d
        assert d["total_reaped"] == 1
