"""Tests for pipewatch.notifier."""

from __future__ import annotations

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from pipewatch.alerts import Alert, AlertRule
from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.notifier import Notifier, NotificationConfig, NotificationRecord


def make_alert(severity: str = "warning", pipeline: str = "pipe-A") -> Alert:
    rule = AlertRule(name="high_error", severity=severity, threshold=0.1)
    metric = PipelineMetric(
        pipeline=pipeline,
        processed=100,
        failed=20,
        status=PipelineStatus.WARNING,
        timestamp=datetime.now(timezone.utc),
    )
    return Alert(rule=rule, metric=metric, pipeline=pipeline)


class TestNotificationConfig:
    def test_validate_passes_for_valid_config(self):
        cfg = NotificationConfig(channel="slack", min_severity="critical")
        cfg.validate()  # should not raise

    def test_validate_rejects_empty_channel(self):
        cfg = NotificationConfig(channel="  ")
        with pytest.raises(ValueError, match="channel"):
            cfg.validate()

    def test_validate_rejects_invalid_severity(self):
        cfg = NotificationConfig(channel="email", min_severity="info")
        with pytest.raises(ValueError, match="min_severity"):
            cfg.validate()


class TestNotifier:
    def setup_method(self):
        self.notifier = Notifier()
        self.handler = MagicMock()
        cfg = NotificationConfig(channel="test", enabled=True, min_severity="warning")
        self.notifier.register(cfg, self.handler)

    def test_notify_calls_handler(self):
        alert = make_alert(severity="warning")
        records = self.notifier.notify(alert)
        self.handler.assert_called_once_with(alert)
        assert len(records) == 1
        assert records[0].success is True

    def test_disabled_channel_skipped(self):
        notifier = Notifier()
        handler = MagicMock()
        cfg = NotificationConfig(channel="muted", enabled=False)
        notifier.register(cfg, handler)
        notifier.notify(make_alert())
        handler.assert_not_called()

    def test_critical_only_channel_skips_warning(self):
        notifier = Notifier()
        handler = MagicMock()
        cfg = NotificationConfig(channel="pagerduty", min_severity="critical")
        notifier.register(cfg, handler)
        notifier.notify(make_alert(severity="warning"))
        handler.assert_not_called()

    def test_critical_only_channel_delivers_critical(self):
        notifier = Notifier()
        handler = MagicMock()
        cfg = NotificationConfig(channel="pagerduty", min_severity="critical")
        notifier.register(cfg, handler)
        notifier.notify(make_alert(severity="critical"))
        handler.assert_called_once()

    def test_handler_exception_recorded_as_failure(self):
        notifier = Notifier()
        bad_handler = MagicMock(side_effect=RuntimeError("network error"))
        cfg = NotificationConfig(channel="flaky")
        notifier.register(cfg, bad_handler)
        records = notifier.notify(make_alert())
        assert records[0].success is False
        assert "network error" in records[0].error

    def test_history_accumulates(self):
        self.notifier.notify(make_alert())
        self.notifier.notify(make_alert(pipeline="pipe-B"))
        assert len(self.notifier.history()) == 2

    def test_history_filtered_by_channel(self):
        self.notifier.notify(make_alert())
        records = self.notifier.history(channel="test")
        assert all(r.channel == "test" for r in records)

    def test_clear_history_empties_records(self):
        self.notifier.notify(make_alert())
        self.notifier.clear_history()
        assert self.notifier.history() == []

    def test_record_to_dict_has_expected_keys(self):
        records = self.notifier.notify(make_alert())
        d = records[0].to_dict()
        for key in ("channel", "alert_id", "pipeline", "severity", "sent_at", "success", "error"):
            assert key in d
