import pytest
from datetime import datetime
from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.labeler import Label, LabelRule, apply_labels, Labeler


def make_metric(name: str, status: PipelineStatus = PipelineStatus.HEALTHY) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        status=status,
        total_records=100,
        failed_records=0,
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
    )


class TestLabelRule:
    def test_matches_by_status(self):
        rule = LabelRule(key="severity", value="ok", status=PipelineStatus.HEALTHY)
        metric = make_metric("pipe", PipelineStatus.HEALTHY)
        assert rule.matches(metric)

    def test_no_match_wrong_status(self):
        rule = LabelRule(key="severity", value="ok", status=PipelineStatus.CRITICAL)
        metric = make_metric("pipe", PipelineStatus.HEALTHY)
        assert not rule.matches(metric)

    def test_matches_by_name_prefix(self):
        rule = LabelRule(key="team", value="data", name_prefix="etl_")
        metric = make_metric("etl_orders")
        assert rule.matches(metric)

    def test_no_match_wrong_prefix(self):
        rule = LabelRule(key="team", value="data", name_prefix="etl_")
        metric = make_metric("ml_orders")
        assert not rule.matches(metric)

    def test_matches_combined_criteria(self):
        rule = LabelRule(key="env", value="prod", status=PipelineStatus.WARNING, name_prefix="prod_")
        metric = make_metric("prod_pipe", PipelineStatus.WARNING)
        assert rule.matches(metric)

    def test_no_match_combined_criteria_partial(self):
        rule = LabelRule(key="env", value="prod", status=PipelineStatus.WARNING, name_prefix="prod_")
        metric = make_metric("prod_pipe", PipelineStatus.HEALTHY)
        assert not rule.matches(metric)

    def test_no_criteria_matches_all(self):
        rule = LabelRule(key="tag", value="all")
        assert rule.matches(make_metric("any", PipelineStatus.CRITICAL))


class TestLabeler:
    def setup_method(self):
        self.labeler = Labeler()
        self.labeler.add_rule(LabelRule(key="status_label", value="healthy", status=PipelineStatus.HEALTHY))
        self.labeler.add_rule(LabelRule(key="status_label", value="critical", status=PipelineStatus.CRITICAL))
        self.labeler.add_rule(LabelRule(key="team", value="etl", name_prefix="etl_"))

    def test_healthy_metric_gets_healthy_label(self):
        metric = make_metric("pipe", PipelineStatus.HEALTHY)
        labels = self.labeler.label(metric)
        assert any(l.key == "status_label" and l.value == "healthy" for l in labels)

    def test_critical_metric_gets_critical_label(self):
        metric = make_metric("pipe", PipelineStatus.CRITICAL)
        labels = self.labeler.label(metric)
        assert any(l.key == "status_label" and l.value == "critical" for l in labels)

    def test_prefix_rule_applied(self):
        metric = make_metric("etl_sales")
        labels = self.labeler.label(metric)
        assert any(l.key == "team" and l.value == "etl" for l in labels)

    def test_label_many_returns_dict(self):
        metrics = [make_metric("etl_a"), make_metric("ml_b", PipelineStatus.CRITICAL)]
        result = self.labeler.label_many(metrics)
        assert "etl_a" in result
        assert "ml_b" in result

    def test_label_to_dict(self):
        label = Label(key="k", value="v")
        assert label.to_dict() == {"key": "k", "value": "v"}

    def test_label_str(self):
        label = Label(key="env", value="prod")
        assert str(label) == "env=prod"
