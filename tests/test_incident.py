"""Tests for pipewatch.incident."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from unittest.mock import patch

import pytest

from pipewatch.alerts import Alert
from pipewatch.incident import Incident, IncidentManager, IncidentManagerConfig


def make_alert(pipeline: str = "etl_main", severity: str = "critical", message: str = "High error rate") -> Alert:
    return Alert(pipeline=pipeline, severity=severity, message=message)


class TestIncident:
    def test_new_incident_is_open(self):
        inc = Incident(pipeline="p1", severity="critical", message="err")
        assert inc.is_open is True
        assert inc.resolved_at is None

    def test_resolve_closes_incident(self):
        inc = Incident(pipeline="p1", severity="critical", message="err")
        inc.resolve()
        assert inc.is_open is False
        assert inc.resolved_at is not None

    def test_resolve_idempotent(self):
        inc = Incident(pipeline="p1", severity="critical", message="err")
        inc.resolve()
        first_resolved = inc.resolved_at
        inc.resolve()
        assert inc.resolved_at == first_resolved

    def test_increment_increases_count(self):
        inc = Incident(pipeline="p1", severity="warning", message="slow")
        assert inc.alert_count == 1
        inc.increment()
        inc.increment()
        assert inc.alert_count == 3

    def test_to_dict_contains_expected_keys(self):
        inc = Incident(pipeline="p1", severity="critical", message="err")
        d = inc.to_dict()
        for key in ("incident_id", "pipeline", "severity", "message", "opened_at", "resolved_at", "alert_count", "is_open"):
            assert key in d

    def test_to_dict_resolved_at_none_when_open(self):
        inc = Incident(pipeline="p1", severity="warning", message="slow")
        assert inc.to_dict()["resolved_at"] is None


class TestIncidentManager:
    def setup_method(self):
        self.manager = IncidentManager()

    def test_process_creates_new_incident(self):
        alert = make_alert()
        inc = self.manager.process(alert)
        assert inc.pipeline == "etl_main"
        assert inc.is_open

    def test_process_same_pipeline_increments(self):
        alert = make_alert()
        self.manager.process(alert)
        inc = self.manager.process(alert)
        assert inc.alert_count == 2

    def test_open_incidents_returns_only_open(self):
        self.manager.process(make_alert("p1"))
        self.manager.process(make_alert("p2"))
        self.manager.resolve("p1")
        open_list = self.manager.open_incidents()
        assert len(open_list) == 1
        assert open_list[0].pipeline == "p2"

    def test_resolve_returns_incident(self):
        self.manager.process(make_alert("p1"))
        resolved = self.manager.resolve("p1")
        assert resolved is not None
        assert not resolved.is_open

    def test_resolve_unknown_pipeline_returns_none(self):
        result = self.manager.resolve("nonexistent")
        assert result is None

    def test_reopen_on_new_alert(self):
        self.manager.process(make_alert("p1"))
        self.manager.resolve("p1")
        inc = self.manager.process(make_alert("p1"))
        assert inc.is_open
        assert inc.alert_count == 1

    def test_no_reopen_when_disabled(self):
        config = IncidentManagerConfig(reopen_on_new_alert=False)
        manager = IncidentManager(config=config)
        manager.process(make_alert("p1"))
        manager.resolve("p1")
        inc = manager.process(make_alert("p1"))
        assert inc.is_open

    def test_clear_removes_all(self):
        self.manager.process(make_alert("p1"))
        self.manager.process(make_alert("p2"))
        self.manager.clear()
        assert self.manager.open_incidents() == []
        assert self.manager.all_incidents() == []
