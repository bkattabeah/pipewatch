"""Tests for pipewatch.audit."""
import pytest
from datetime import datetime, timedelta
from pipewatch.audit import AuditEvent, AuditLog


def make_event(pipeline="pipe-a", event_type="alert", severity="warning", message="test"):
    return AuditEvent(
        pipeline=pipeline,
        event_type=event_type,
        severity=severity,
        message=message,
    )


class TestAuditLog:
    def setup_method(self):
        self.log = AuditLog(max_size=100)

    def test_empty_log_returns_no_events(self):
        assert self.log.all() == []

    def test_record_and_retrieve(self):
        e = make_event()
        self.log.record(e)
        assert len(self.log.all()) == 1
        assert self.log.all()[0].pipeline == "pipe-a"

    def test_for_pipeline_filters_correctly(self):
        self.log.record(make_event(pipeline="pipe-a"))
        self.log.record(make_event(pipeline="pipe-b"))
        result = self.log.for_pipeline("pipe-a")
        assert len(result) == 1
        assert result[0].pipeline == "pipe-a"

    def test_by_type_filters_correctly(self):
        self.log.record(make_event(event_type="alert"))
        self.log.record(make_event(event_type="status_change"))
        alerts = self.log.by_type("alert")
        assert len(alerts) == 1
        assert alerts[0].event_type == "alert"

    def test_since_filters_by_time(self):
        past = make_event()
        past.timestamp = datetime.utcnow() - timedelta(hours=2)
        recent = make_event(message="recent")
        self.log.record(past)
        self.log.record(recent)
        cutoff = datetime.utcnow() - timedelta(hours=1)
        result = self.log.since(cutoff)
        assert len(result) == 1
        assert result[0].message == "recent"

    def test_max_size_enforced(self):
        log = AuditLog(max_size=5)
        for i in range(10):
            log.record(make_event(message=str(i)))
        assert len(log.all()) == 5
        assert log.all()[-1].message == "9"

    def test_clear_removes_all_events(self):
        self.log.record(make_event())
        self.log.clear()
        assert self.log.all() == []

    def test_to_dict_list_structure(self):
        self.log.record(make_event())
        result = self.log.to_dict_list()
        assert isinstance(result, list)
        assert "pipeline" in result[0]
        assert "event_type" in result[0]
        assert "timestamp" in result[0]

    def test_to_dict_event(self):
        e = make_event(pipeline="p", event_type="recovery", severity="info", message="ok")
        d = e.to_dict()
        assert d["pipeline"] == "p"
        assert d["event_type"] == "recovery"
        assert d["severity"] == "info"
        assert d["message"] == "ok"
        assert "timestamp" in d
