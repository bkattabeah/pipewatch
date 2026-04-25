"""Tests for pipewatch.notification_summary."""

from __future__ import annotations

from datetime import datetime, timezone

from pipewatch.notifier import NotificationRecord
from pipewatch.notification_summary import (
    build_notification_summary,
    format_notification_summary,
    ChannelSummary,
)


def make_record(
    channel: str = "slack",
    pipeline: str = "pipe-A",
    severity: str = "warning",
    success: bool = True,
    error: str | None = None,
) -> NotificationRecord:
    return NotificationRecord(
        channel=channel,
        alert_id=f"{pipeline}:high_error",
        pipeline=pipeline,
        severity=severity,
        sent_at=datetime.now(timezone.utc),
        success=success,
        error=error,
    )


class TestBuildNotificationSummary:
    def test_empty_records_returns_empty_summary(self):
        summary = build_notification_summary([])
        assert summary.total_sent == 0
        assert summary.total_failures == 0
        assert summary.channels == []

    def test_single_success_counted(self):
        summary = build_notification_summary([make_record()])
        assert summary.total_sent == 1
        assert summary.total_failures == 0
        assert summary.channels[0].successes == 1

    def test_failure_counted(self):
        summary = build_notification_summary([make_record(success=False, error="timeout")])
        assert summary.total_failures == 1
        assert summary.channels[0].failures == 1

    def test_multiple_channels_grouped(self):
        records = [
            make_record(channel="slack"),
            make_record(channel="email"),
            make_record(channel="slack"),
        ]
        summary = build_notification_summary(records)
        channels = {c.channel: c for c in summary.channels}
        assert channels["slack"].total == 2
        assert channels["email"].total == 1

    def test_failure_rate_calculated(self):
        records = [
            make_record(success=True),
            make_record(success=False),
            make_record(success=False),
        ]
        summary = build_notification_summary(records)
        ch = summary.channels[0]
        assert abs(ch.failure_rate - 2 / 3) < 1e-6

    def test_pipelines_deduplicated_in_dict(self):
        records = [
            make_record(pipeline="pipe-A"),
            make_record(pipeline="pipe-A"),
            make_record(pipeline="pipe-B"),
        ]
        summary = build_notification_summary(records)
        d = summary.channels[0].to_dict()
        assert d["pipelines"] == ["pipe-A", "pipe-B"]

    def test_to_dict_has_expected_keys(self):
        summary = build_notification_summary([make_record()])
        d = summary.to_dict()
        assert "total_sent" in d
        assert "total_failures" in d
        assert "channels" in d

    def test_format_includes_channel_name(self):
        records = [make_record(channel="pagerduty")]
        summary = build_notification_summary(records)
        text = format_notification_summary(summary)
        assert "pagerduty" in text
        assert "Notifications:" in text

    def test_channel_summary_failure_rate_zero_when_no_records(self):
        ch = ChannelSummary(channel="test")
        assert ch.failure_rate == 0.0
