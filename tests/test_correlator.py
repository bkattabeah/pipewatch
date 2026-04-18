"""Tests for pipewatch.correlator."""
import pytest
from datetime import datetime, timedelta
from pipewatch.correlator import AlertCorrelator, CorrelatorConfig, CorrelationGroup
from pipewatch.alerts import Alert, AlertRule
from pipewatch.metrics import PipelineStatus


def make_alert(pipeline: str, severity: str = "critical", seconds_ago: int = 0) -> Alert:
    rule = AlertRule(name="test", severity=severity, threshold=0.5)
    alert = Alert(rule=rule, pipeline=pipeline, value=0.9)
    alert.triggered_at = datetime.utcnow() - timedelta(seconds=seconds_ago)
    return alert


class TestAlertCorrelator:
    def setup_method(self):
        self.corr = AlertCorrelator(CorrelatorConfig(window_seconds=60, min_group_size=2))

    def test_no_groups_when_empty(self):
        assert self.corr.correlate() == []

    def test_no_group_below_min_size(self):
        self.corr.record(make_alert("pipe_a"))
        groups = self.corr.correlate()
        assert groups == []

    def test_group_formed_for_same_severity(self):
        self.corr.record(make_alert("pipe_a", severity="critical"))
        self.corr.record(make_alert("pipe_b", severity="critical"))
        groups = self.corr.correlate()
        assert len(groups) == 1
        assert len(groups[0].alerts) == 2

    def test_separate_groups_for_different_severities(self):
        self.corr.record(make_alert("pipe_a", severity="critical"))
        self.corr.record(make_alert("pipe_b", severity="critical"))
        self.corr.record(make_alert("pipe_c", severity="warning"))
        self.corr.record(make_alert("pipe_d", severity="warning"))
        groups = self.corr.correlate()
        assert len(groups) == 2

    def test_stale_alerts_excluded(self):
        self.corr.record(make_alert("pipe_a", seconds_ago=120))
        self.corr.record(make_alert("pipe_b", seconds_ago=120))
        groups = self.corr.correlate()
        assert groups == []

    def test_to_dict_contains_expected_keys(self):
        self.corr.record(make_alert("pipe_a"))
        self.corr.record(make_alert("pipe_b"))
        groups = self.corr.correlate()
        d = groups[0].to_dict()
        assert "group_id" in d
        assert "alert_count" in d
        assert "pipelines" in d
        assert d["alert_count"] == 2

    def test_clear_removes_all_alerts(self):
        self.corr.record(make_alert("pipe_a"))
        self.corr.record(make_alert("pipe_b"))
        self.corr.clear()
        assert self.corr.correlate() == []

    def test_config_validation_rejects_bad_window(self):
        with pytest.raises(ValueError):
            CorrelatorConfig(window_seconds=0).validate()

    def test_config_validation_rejects_small_min_size(self):
        with pytest.raises(ValueError):
            CorrelatorConfig(min_group_size=1).validate()
