"""Tests for pipewatch.escalator."""
import pytest
from datetime import datetime, timedelta
from pipewatch.alerts import Alert
from pipewatch.metrics import PipelineStatus
from pipewatch.escalator import AlertEscalator, EscalationConfig


def make_alert(pipeline="pipe1", rule_name="high_error_rate", status=PipelineStatus.CRITICAL):
    return Alert(
        pipeline=pipeline,
        rule_name=rule_name,
        status=status,
        message="error rate too high",
        value=0.9,
        threshold=0.5,
    )


class TestAlertEscalator:
    def setup_method(self):
        self.config = EscalationConfig(repeat_threshold=3, window_seconds=60.0)
        self.escalator = AlertEscalator(self.config)

    def test_first_alert_not_escalated(self):
        alert = make_alert()
        escalated = self.escalator.record(alert)
        assert not escalated
        assert not self.escalator.is_escalated(alert)

    def test_escalates_at_threshold(self):
        alert = make_alert()
        now = datetime.utcnow()
        for i in range(2):
            result = self.escalator.record(alert, now=now + timedelta(seconds=i))
            assert not result
        result = self.escalator.record(alert, now=now + timedelta(seconds=3))
        assert result
        assert self.escalator.is_escalated(alert)

    def test_old_occurrences_pruned(self):
        alert = make_alert()
        old = datetime.utcnow() - timedelta(seconds=120)
        self.escalator.record(alert, now=old)
        self.escalator.record(alert, now=old + timedelta(seconds=1))
        # these are outside the 60s window; new record should start fresh count
        now = datetime.utcnow()
        result = self.escalator.record(alert, now=now)
        assert not result

    def test_clear_removes_entry(self):
        alert = make_alert()
        now = datetime.utcnow()
        for i in range(3):
            self.escalator.record(alert, now=now + timedelta(seconds=i))
        assert self.escalator.is_escalated(alert)
        self.escalator.clear(alert)
        assert not self.escalator.is_escalated(alert)

    def test_different_pipelines_tracked_separately(self):
        a1 = make_alert(pipeline="pipe1")
        a2 = make_alert(pipeline="pipe2")
        now = datetime.utcnow()
        for i in range(3):
            self.escalator.record(a1, now=now + timedelta(seconds=i))
        assert self.escalator.is_escalated(a1)
        assert not self.escalator.is_escalated(a2)

    def test_all_entries_returns_list(self):
        a1 = make_alert(pipeline="pipe1")
        a2 = make_alert(pipeline="pipe2")
        self.escalator.record(a1)
        self.escalator.record(a2)
        entries = self.escalator.all_entries()
        assert len(entries) == 2

    def test_entry_to_dict(self):
        alert = make_alert()
        self.escalator.record(alert)
        entry = self.escalator.all_entries()[0]
        d = entry.to_dict()
        assert d["pipeline"] == "pipe1"
        assert d["rule_name"] == "high_error_rate"
        assert isinstance(d["occurrences"], list)
        assert d["escalated"] is False
