"""Tests for pipewatch.alert_router."""
from datetime import datetime, timedelta
from pipewatch.alerts import Alert
from pipewatch.silencer import Silencer, SilenceRule
from pipewatch.alert_router import AlertRouter
from pipewatch.metrics import PipelineStatus


def make_alert(pipeline="etl_main", level="warning"):
    return Alert(pipeline=pipeline, level=level, message="test alert", metric=None)


def make_active_rule(pipeline):
    now = datetime.utcnow()
    return SilenceRule(pipeline, "maintenance", now - timedelta(minutes=5), now + timedelta(hours=1))


class TestAlertRouter:
    def setup_method(self):
        self.silencer = Silencer()
        self.received: list = []
        self.router = AlertRouter(self.silencer, handlers=[self.received.append])

    def test_dispatches_when_not_silenced(self):
        alert = make_alert()
        result = self.router.route(alert)
        assert result is True
        assert len(self.received) == 1

    def test_suppresses_when_silenced(self):
        self.silencer.add(make_active_rule("etl_main"))
        alert = make_alert("etl_main")
        result = self.router.route(alert)
        assert result is False
        assert len(self.received) == 0

    def test_suppressed_stored(self):
        self.silencer.add(make_active_rule("etl_main"))
        self.router.route(make_alert("etl_main"))
        assert len(self.router.suppressed_alerts()) == 1

    def test_other_pipeline_not_suppressed(self):
        self.silencer.add(make_active_rule("etl_main"))
        alert = make_alert("etl_other")
        result = self.router.route(alert)
        assert result is True
        assert len(self.received) == 1

    def test_route_many_counts(self):
        self.silencer.add(make_active_rule("etl_main"))
        alerts = [make_alert("etl_main"), make_alert("etl_other"), make_alert("etl_other")]
        counts = self.router.route_many(alerts)
        assert counts["dispatched"] == 2
        assert counts["suppressed"] == 1

    def test_clear_suppressed(self):
        self.silencer.add(make_active_rule("etl_main"))
        self.router.route(make_alert("etl_main"))
        self.router.clear_suppressed()
        assert self.router.suppressed_alerts() == []
