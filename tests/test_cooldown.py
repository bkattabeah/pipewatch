"""Tests for pipewatch.cooldown AlertCooldownManager."""

from datetime import datetime, timedelta

import pytest

from pipewatch.alerts import Alert, AlertRule
from pipewatch.cooldown import AlertCooldownManager, CooldownConfig
from pipewatch.metrics import PipelineStatus


def make_alert(pipeline: str = "pipe_a", rule_name: str = "high_error") -> Alert:
    rule = AlertRule(
        name=rule_name,
        status=PipelineStatus.CRITICAL,
        message="error rate too high",
    )
    return Alert(pipeline=pipeline, rule=rule, value=0.9)


class TestCooldownConfig:
    def test_defaults(self):
        cfg = CooldownConfig()
        assert cfg.window_seconds == 300
        assert cfg.max_suppressed == 100

    def test_validate_passes(self):
        CooldownConfig(window_seconds=60, max_suppressed=10).validate()

    def test_validate_rejects_zero_window(self):
        with pytest.raises(ValueError, match="window_seconds"):
            CooldownConfig(window_seconds=0).validate()

    def test_validate_rejects_zero_max_suppressed(self):
        with pytest.raises(ValueError, match="max_suppressed"):
            CooldownConfig(max_suppressed=0).validate()


class TestAlertCooldownManager:
    def setup_method(self):
        self.config = CooldownConfig(window_seconds=60)
        self.mgr = AlertCooldownManager(self.config)
        self.now = datetime(2024, 1, 1, 12, 0, 0)

    def test_first_alert_always_allowed(self):
        alert = make_alert()
        assert self.mgr.allow(alert, now=self.now) is True

    def test_second_alert_within_window_suppressed(self):
        alert = make_alert()
        self.mgr.allow(alert, now=self.now)
        later = self.now + timedelta(seconds=30)
        assert self.mgr.allow(alert, now=later) is False

    def test_alert_allowed_after_window_expires(self):
        alert = make_alert()
        self.mgr.allow(alert, now=self.now)
        after = self.now + timedelta(seconds=61)
        assert self.mgr.allow(alert, now=after) is True

    def test_suppressed_count_increments(self):
        alert = make_alert()
        self.mgr.allow(alert, now=self.now)
        for i in range(3):
            self.mgr.allow(alert, now=self.now + timedelta(seconds=i + 1))
        status = self.mgr.status()
        assert status[0]["suppressed_count"] == 3

    def test_suppressed_count_capped_at_max(self):
        cfg = CooldownConfig(window_seconds=3600, max_suppressed=3)
        mgr = AlertCooldownManager(cfg)
        alert = make_alert()
        for i in range(6):
            mgr.allow(alert, now=self.now + timedelta(seconds=i))
        status = mgr.status()
        assert status[0]["suppressed_count"] == 3

    def test_different_pipelines_tracked_independently(self):
        a = make_alert(pipeline="pipe_a")
        b = make_alert(pipeline="pipe_b")
        self.mgr.allow(a, now=self.now)
        assert self.mgr.allow(b, now=self.now) is True

    def test_clear_removes_all_entries(self):
        alert = make_alert()
        self.mgr.allow(alert, now=self.now)
        self.mgr.clear()
        assert self.mgr.status() == []

    def test_clear_by_pipeline_removes_only_that_pipeline(self):
        a = make_alert(pipeline="pipe_a")
        b = make_alert(pipeline="pipe_b")
        self.mgr.allow(a, now=self.now)
        self.mgr.allow(b, now=self.now)
        self.mgr.clear(pipeline="pipe_a")
        pipelines = [e["pipeline"] for e in self.mgr.status()]
        assert "pipe_a" not in pipelines
        assert "pipe_b" in pipelines
