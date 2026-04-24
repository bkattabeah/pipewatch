"""Tests for pipewatch.runbook."""
import pytest
from pipewatch.metrics import PipelineStatus
from pipewatch.alerts import Alert, AlertRule
from pipewatch.runbook import RunbookEntry, RunbookRegistry


def make_alert(pipeline: str = "etl_main", status: PipelineStatus = PipelineStatus.CRITICAL) -> Alert:
    rule = AlertRule(name="test_rule", status=status, message="test alert")
    return Alert(pipeline=pipeline, rule=rule, status=status)


class TestRunbookEntry:
    def test_to_dict_contains_expected_keys(self):
        entry = RunbookEntry(
            pipeline="orders",
            status=PipelineStatus.CRITICAL,
            title="Orders pipeline down",
            steps=["Check logs", "Restart service"],
            reference_url="https://wiki.example.com/orders",
        )
        d = entry.to_dict()
        assert d["pipeline"] == "orders"
        assert d["status"] == "critical"
        assert d["title"] == "Orders pipeline down"
        assert len(d["steps"]) == 2
        assert d["reference_url"] == "https://wiki.example.com/orders"

    def test_to_dict_reference_url_none_by_default(self):
        entry = RunbookEntry(pipeline="p", status=PipelineStatus.WARNING, title="t")
        assert entry.to_dict()["reference_url"] is None


class TestRunbookRegistry:
    def setup_method(self):
        self.registry = RunbookRegistry()

    def test_register_and_lookup_custom_entry(self):
        entry = RunbookEntry(
            pipeline="payments",
            status=PipelineStatus.CRITICAL,
            title="Payments critical runbook",
            steps=["Alert on-call"],
        )
        self.registry.register(entry)
        result = self.registry.lookup("payments", PipelineStatus.CRITICAL)
        assert result.title == "Payments critical runbook"
        assert result.steps == ["Alert on-call"]

    def test_lookup_returns_default_when_not_registered(self):
        result = self.registry.lookup("unknown_pipeline", PipelineStatus.CRITICAL)
        assert "unknown_pipeline" in result.title
        assert len(result.steps) > 0

    def test_default_critical_steps_are_non_empty(self):
        result = self.registry.lookup("any", PipelineStatus.CRITICAL)
        assert any("error_rate" in s or "logs" in s.lower() for s in result.steps)

    def test_default_warning_steps_are_non_empty(self):
        result = self.registry.lookup("any", PipelineStatus.WARNING)
        assert len(result.steps) >= 1

    def test_default_unknown_steps_are_non_empty(self):
        result = self.registry.lookup("any", PipelineStatus.UNKNOWN)
        assert len(result.steps) >= 1

    def test_suggest_uses_alert_pipeline_and_status(self):
        alert = make_alert(pipeline="inventory", status=PipelineStatus.WARNING)
        entry = RunbookEntry(
            pipeline="inventory",
            status=PipelineStatus.WARNING,
            title="Inventory warning",
            steps=["Check inventory source"],
        )
        self.registry.register(entry)
        result = self.registry.suggest(alert)
        assert result.title == "Inventory warning"

    def test_all_entries_returns_registered(self):
        e1 = RunbookEntry(pipeline="a", status=PipelineStatus.CRITICAL, title="A critical")
        e2 = RunbookEntry(pipeline="b", status=PipelineStatus.WARNING, title="B warning")
        self.registry.register(e1)
        self.registry.register(e2)
        entries = self.registry.all_entries()
        assert len(entries) == 2

    def test_register_overwrites_existing_entry(self):
        e1 = RunbookEntry(pipeline="p", status=PipelineStatus.CRITICAL, title="First")
        e2 = RunbookEntry(pipeline="p", status=PipelineStatus.CRITICAL, title="Second")
        self.registry.register(e1)
        self.registry.register(e2)
        result = self.registry.lookup("p", PipelineStatus.CRITICAL)
        assert result.title == "Second"
        assert len(self.registry.all_entries()) == 1
