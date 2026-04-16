import io
import json
import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.alerts import Alert, AlertEngine, AlertRule
from pipewatch.handlers import console_handler, json_handler


def make_metric(pipeline_id="pipe-1", total=100, failed=0) -> PipelineMetric:
    return PipelineMetric(pipeline_id=pipeline_id, total_records=total, failed_records=failed)


class TestAlertEngine:
    def setup_method(self):
        self.engine = AlertEngine()

    def test_no_alerts_for_healthy_metric(self):
        metric = make_metric(total=100, failed=2)
        alerts = self.engine.evaluate(metric)
        assert alerts == []

    def test_warning_alert_on_high_error_rate(self):
        metric = make_metric(total=100, failed=15)
        alerts = self.engine.evaluate(metric)
        names = [a.rule_name for a in alerts]
        assert "high_error_rate" in names
        assert all(a.severity in ("warning", "critical") for a in alerts)

    def test_critical_alert_on_very_high_error_rate(self):
        metric = make_metric(total=100, failed=30)
        alerts = self.engine.evaluate(metric)
        names = [a.rule_name for a in alerts]
        assert "critical_error_rate" in names
        assert "high_error_rate" in names

    def test_pipeline_down_alert(self):
        metric = make_metric(total=0, failed=0)
        alerts = self.engine.evaluate(metric)
        names = [a.rule_name for a in alerts]
        assert "pipeline_down" in names

    def test_custom_rule_is_evaluated(self):
        rule = AlertRule(
            name="low_throughput",
            condition=lambda m: m.total_records < 10,
            message="Throughput too low",
            severity="warning",
        )
        self.engine.add_rule(rule)
        metric = make_metric(total=5, failed=0)
        alerts = self.engine.evaluate(metric)
        assert any(a.rule_name == "low_throughput" for a in alerts)

    def test_handler_is_called_on_alert(self):
        received = []
        self.engine.add_handler(received.append)
        metric = make_metric(total=100, failed=50)
        self.engine.evaluate(metric)
        assert len(received) > 0


class TestHandlers:
    def _make_alert(self, severity="warning"):
        metric = make_metric(total=100, failed=15)
        return Alert(rule_name="high_error_rate", severity=severity, message="Test", metric=metric)

    def test_console_handler_writes_to_stream(self):
        buf = io.StringIO()
        alert = self._make_alert()
        console_handler(alert, stream=buf)
        output = buf.getvalue()
        assert "high_error_rate" in output
        assert "WARNING" in output

    def test_json_handler_emits_valid_json(self):
        buf = io.StringIO()
        alert = self._make_alert(severity="critical")
        json_handler(alert, stream=buf)
        data = json.loads(buf.getvalue())
        assert data["rule"] == "high_error_rate"
        assert data["severity"] == "critical"
        assert "error_rate" in data
        assert data["pipeline_id"] == "pipe-1"
