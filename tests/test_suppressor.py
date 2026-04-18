"""Tests for pipewatch.suppressor."""
import pytest
from datetime import datetime
from pipewatch.alerts import Alert, AlertRule
from pipewatch.metrics import PipelineStatus
from pipewatch.suppressor import AlertSuppressor, SuppressorConfig


def make_alert(status: PipelineStatus) -> Alert:
    rule = AlertRule(name="test", status=status, message="msg")
    return Alert(pipeline="pipe_a", rule=rule, status=status, fired_at=datetime.utcnow())


class TestSuppressorConfig:
    def test_default_min_severity_is_warning(self):
        cfg = SuppressorConfig()
        assert cfg.min_severity == PipelineStatus.WARNING

    def test_validate_passes_for_valid_status(self):
        cfg = SuppressorConfig(min_severity=PipelineStatus.CRITICAL)
        cfg.validate()  # should not raise


class TestAlertSuppressor:
    def setup_method(self):
        self.suppressor = AlertSuppressor()

    def test_healthy_alert_is_suppressed(self):
        alert = make_alert(PipelineStatus.HEALTHY)
        result = self.suppressor.check(alert)
        assert result.suppressed is True
        assert "healthy" in result.reason

    def test_warning_alert_is_not_suppressed_at_default(self):
        alert = make_alert(PipelineStatus.WARNING)
        result = self.suppressor.check(alert)
        assert result.suppressed is False
        assert result.reason is None

    def test_critical_alert_is_not_suppressed(self):
        alert = make_alert(PipelineStatus.CRITICAL)
        result = self.suppressor.check(alert)
        assert result.suppressed is False

    def test_suppressed_count_increments(self):
        alert = make_alert(PipelineStatus.HEALTHY)
        self.suppressor.check(alert)
        self.suppressor.check(alert)
        assert self.suppressor.suppressed_count == 2

    def test_reset_clears_count(self):
        alert = make_alert(PipelineStatus.HEALTHY)
        self.suppressor.check(alert)
        self.suppressor.reset()
        assert self.suppressor.suppressed_count == 0

    def test_filter_removes_suppressed(self):
        alerts = [
            make_alert(PipelineStatus.HEALTHY),
            make_alert(PipelineStatus.WARNING),
            make_alert(PipelineStatus.CRITICAL),
        ]
        result = self.suppressor.filter(alerts)
        assert len(result) == 2
        assert all(a.status != PipelineStatus.HEALTHY for a in result)

    def test_critical_min_severity_suppresses_warning(self):
        cfg = SuppressorConfig(min_severity=PipelineStatus.CRITICAL)
        suppressor = AlertSuppressor(config=cfg)
        alert = make_alert(PipelineStatus.WARNING)
        result = suppressor.check(alert)
        assert result.suppressed is True

    def test_to_dict_includes_suppressed_flag(self):
        alert = make_alert(PipelineStatus.HEALTHY)
        result = self.suppressor.check(alert)
        d = result.to_dict()
        assert d["suppressed"] is True
        assert d["pipeline"] == "pipe_a"
