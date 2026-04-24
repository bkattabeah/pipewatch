"""Tests for pipewatch.heartbeat."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from pipewatch.heartbeat import HeartbeatConfig, HeartbeatMonitor


def _now() -> datetime:
    return datetime.now(timezone.utc)


class TestHeartbeatConfig:
    def test_defaults(self):
        cfg = HeartbeatConfig()
        assert cfg.timeout_seconds == 60.0
        assert cfg.max_missed == 3

    def test_validate_passes(self):
        HeartbeatConfig(timeout_seconds=30.0, max_missed=2).validate()

    def test_validate_rejects_zero_timeout(self):
        with pytest.raises(ValueError, match="timeout_seconds"):
            HeartbeatConfig(timeout_seconds=0).validate()

    def test_validate_rejects_zero_max_missed(self):
        with pytest.raises(ValueError, match="max_missed"):
            HeartbeatConfig(max_missed=0).validate()


class TestHeartbeatMonitor:
    def setup_method(self):
        self.cfg = HeartbeatConfig(timeout_seconds=10.0, max_missed=3)
        self.mon = HeartbeatMonitor(config=self.cfg)

    def test_no_heartbeat_returns_dead(self):
        status = self.mon.check("pipe-a")
        assert not status.alive
        assert status.last_seen is None
        assert status.missed == self.cfg.max_missed

    def test_recent_ping_is_alive(self):
        self.mon.ping("pipe-a")
        status = self.mon.check("pipe-a")
        assert status.alive
        assert status.missed == 0

    def test_stale_ping_is_dead(self):
        stale_time = _now() - timedelta(seconds=35)
        with patch("pipewatch.heartbeat.datetime") as mock_dt:
            mock_dt.now.return_value = stale_time
            mock_dt.now.side_effect = None
            self.mon.ping("pipe-a")

        status = self.mon.check("pipe-a")
        assert not status.alive
        assert status.missed >= 3

    def test_ping_resets_missed(self):
        self.mon.ping("pipe-a")
        self.mon.ping("pipe-a")
        status = self.mon.check("pipe-a")
        assert status.missed == 0

    def test_check_all_returns_all_pipelines(self):
        self.mon.ping("pipe-a")
        self.mon.ping("pipe-b")
        statuses = self.mon.check_all()
        names = {s.pipeline for s in statuses}
        assert names == {"pipe-a", "pipe-b"}

    def test_pipelines_list(self):
        self.mon.ping("pipe-x")
        self.mon.ping("pipe-y")
        assert set(self.mon.pipelines()) == {"pipe-x", "pipe-y"}

    def test_to_dict_keys(self):
        self.mon.ping("pipe-a")
        status = self.mon.check("pipe-a")
        d = status.to_dict()
        assert set(d.keys()) == {"pipeline", "alive", "missed", "last_seen", "message"}

    def test_message_alive(self):
        self.mon.ping("pipe-a")
        status = self.mon.check("pipe-a")
        assert status.message == "alive"

    def test_message_no_heartbeat(self):
        status = self.mon.check("ghost")
        assert "No heartbeat" in status.message
