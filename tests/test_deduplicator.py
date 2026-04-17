"""Tests for AlertDeduplicator."""
from datetime import datetime, timedelta
import pytest

from pipewatch.alerts import Alert
from pipewatch.deduplicator import AlertDeduplicator, DeduplicatorConfig, DedupeEntry


def make_alert(pipeline="etl", level="warning", message="high error rate") -> Alert:
    return Alert(pipeline=pipeline, level=level, message=message)


class TestAlertDeduplicator:
    def setup_method(self):
        self.config = DeduplicatorConfig(window_seconds=60)
        self.d = AlertDeduplicator(config=self.config)
        self.now = datetime(2024, 1, 1, 12, 0, 0)

    def test_first_alert_not_duplicate(self):
        alert = make_alert()
        assert not self.d.is_duplicate(alert, now=self.now)

    def test_second_alert_within_window_is_duplicate(self):
        alert = make_alert()
        self.d.record(alert, now=self.now)
        later = self.now + timedelta(seconds=30)
        assert self.d.is_duplicate(alert, now=later)

    def test_alert_after_window_not_duplicate(self):
        alert = make_alert()
        self.d.record(alert, now=self.now)
        later = self.now + timedelta(seconds=61)
        assert not self.d.is_duplicate(alert, now=later)

    def test_different_pipelines_not_duplicate(self):
        a1 = make_alert(pipeline="etl")
        a2 = make_alert(pipeline="ml")
        self.d.record(a1, now=self.now)
        assert not self.d.is_duplicate(a2, now=self.now)

    def test_record_increments_count(self):
        alert = make_alert()
        self.d.record(alert, now=self.now)
        self.d.record(alert, now=self.now + timedelta(seconds=5))
        entries = self.d.entries()
        assert len(entries) == 1
        assert entries[0].count == 2

    def test_filter_suppresses_duplicates(self):
        alert = make_alert()
        self.d.record(alert, now=self.now)
        later = self.now + timedelta(seconds=10)
        result = self.d.filter([alert, alert], now=later)
        assert result == []

    def test_filter_passes_new_alerts(self):
        alerts = [make_alert(pipeline="a"), make_alert(pipeline="b")]
        result = self.d.filter(alerts, now=self.now)
        assert len(result) == 2

    def test_clear_resets_state(self):
        alert = make_alert()
        self.d.record(alert, now=self.now)
        self.d.clear()
        assert self.d.entries() == []
        assert not self.d.is_duplicate(alert, now=self.now)

    def test_to_dict_contains_expected_keys(self):
        alert = make_alert()
        entry = self.d.record(alert, now=self.now)
        d = entry.to_dict()
        for key in ("pipeline", "level", "message", "first_seen", "last_seen", "count"):
            assert key in d
