"""Tests for pipewatch.throttler."""

from datetime import datetime, timedelta

import pytest

from pipewatch.alerts import Alert
from pipewatch.metrics import PipelineStatus
from pipewatch.throttler import AlertThrottler, ThrottlerConfig


def make_alert(pipeline: str = "pipe_a", rule_name: str = "high_error_rate") -> Alert:
    return Alert(
        pipeline=pipeline,
        rule_name=rule_name,
        status=PipelineStatus.CRITICAL,
        message="error rate too high",
    )


class TestThrottlerConfig:
    def test_defaults(self):
        cfg = ThrottlerConfig()
        assert cfg.window_seconds == 60
        assert cfg.max_firings == 3

    def test_validate_passes(self):
        ThrottlerConfig(window_seconds=30, max_firings=5).validate()

    def test_validate_rejects_zero_window(self):
        with pytest.raises(ValueError, match="window_seconds"):
            ThrottlerConfig(window_seconds=0).validate()

    def test_validate_rejects_zero_max_firings(self):
        with pytest.raises(ValueError, match="max_firings"):
            ThrottlerConfig(max_firings=0).validate()


class TestAlertThrottler:
    def setup_method(self):
        self.config = ThrottlerConfig(window_seconds=60, max_firings=2)
        self.throttler = AlertThrottler(self.config)
        self.now = datetime(2024, 1, 1, 12, 0, 0)

    def test_first_alert_not_throttled(self):
        alert = make_alert()
        assert self.throttler.is_throttled(alert, self.now) is False

    def test_below_max_not_throttled(self):
        alert = make_alert()
        self.throttler.record(alert, self.now)
        assert self.throttler.is_throttled(alert, self.now) is False

    def test_at_max_is_throttled(self):
        alert = make_alert()
        self.throttler.record(alert, self.now)
        self.throttler.record(alert, self.now)
        assert self.throttler.is_throttled(alert, self.now) is True

    def test_expired_firings_not_counted(self):
        alert = make_alert()
        old_time = self.now - timedelta(seconds=120)
        self.throttler.record(alert, old_time)
        self.throttler.record(alert, old_time)
        # Both firings are outside the 60s window — should not be throttled
        assert self.throttler.is_throttled(alert, self.now) is False

    def test_mixed_old_and_recent_firings(self):
        alert = make_alert()
        old_time = self.now - timedelta(seconds=90)
        self.throttler.record(alert, old_time)   # outside window
        self.throttler.record(alert, self.now)   # inside window
        # Only 1 recent firing; max is 2 — not throttled
        assert self.throttler.is_throttled(alert, self.now) is False

    def test_different_pipelines_tracked_independently(self):
        a1 = make_alert(pipeline="pipe_a")
        a2 = make_alert(pipeline="pipe_b")
        self.throttler.record(a1, self.now)
        self.throttler.record(a1, self.now)
        assert self.throttler.is_throttled(a1, self.now) is True
        assert self.throttler.is_throttled(a2, self.now) is False

    def test_clear_resets_state(self):
        alert = make_alert()
        self.throttler.record(alert, self.now)
        self.throttler.record(alert, self.now)
        self.throttler.clear()
        assert self.throttler.is_throttled(alert, self.now) is False

    def test_status_returns_entries(self):
        alert = make_alert()
        self.throttler.record(alert, self.now)
        result = self.throttler.status()
        assert len(result) == 1
        assert result[0]["pipeline"] == "pipe_a"
        assert result[0]["rule_name"] == "high_error_rate"
        assert len(result[0]["firings"]) == 1
