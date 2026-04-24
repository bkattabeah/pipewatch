"""Tests for pipewatch.tag_report module."""
from __future__ import annotations

from datetime import datetime, timezone

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.tag import TagRule
from pipewatch.tag_report import TagGroupSummary, build_tag_report, format_tag_report


def make_metric(
    name: str = "pipe",
    status: PipelineStatus = PipelineStatus.HEALTHY,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        processed=100,
        failed=0,
        status=status,
        timestamp=datetime.now(timezone.utc),
    )


class TestBuildTagReport:
    def test_empty_metrics_returns_untagged_group(self):
        rules = [TagRule(key="env", value="prod", status="critical")]
        result = build_tag_report([], rules, key="env")
        assert result == []

    def test_single_group_all_healthy(self):
        metrics = [
            make_metric(name="p1", status=PipelineStatus.HEALTHY),
            make_metric(name="p2", status=PipelineStatus.HEALTHY),
        ]
        rules = [TagRule(key="env", value="prod")]
        result = build_tag_report(metrics, rules, key="env")
        assert len(result) == 1
        assert result[0].tag_value == "prod"
        assert result[0].total == 2
        assert result[0].healthy == 2
        assert result[0].critical == 0

    def test_mixed_status_counted_correctly(self):
        metrics = [
            make_metric(name="p1", status=PipelineStatus.HEALTHY),
            make_metric(name="p2", status=PipelineStatus.WARNING),
            make_metric(name="p3", status=PipelineStatus.CRITICAL),
        ]
        rules = [TagRule(key="env", value="prod")]
        result = build_tag_report(metrics, rules, key="env")
        assert result[0].healthy == 1
        assert result[0].warning == 1
        assert result[0].critical == 1

    def test_untagged_bucket_present_when_no_rule_matches(self):
        metrics = [make_metric(name="p1", status=PipelineStatus.HEALTHY)]
        rules = [TagRule(key="env", value="prod", status="critical")]
        result = build_tag_report(metrics, rules, key="env")
        buckets = [r.tag_value for r in result]
        assert "__untagged__" in buckets

    def test_to_dict_has_expected_keys(self):
        s = TagGroupSummary(
            tag_value="prod", total=3, healthy=2, warning=1, critical=0, unknown=0
        )
        d = s.to_dict()
        assert set(d.keys()) == {"tag_value", "total", "healthy", "warning", "critical", "unknown"}

    def test_multiple_rules_two_groups(self):
        metrics = [
            make_metric(name="etl_a", status=PipelineStatus.CRITICAL),
            make_metric(name="batch_b", status=PipelineStatus.HEALTHY),
        ]
        rules = [
            TagRule(key="tier", value="etl", name_prefix="etl_"),
            TagRule(key="tier", value="batch", name_prefix="batch_"),
        ]
        result = build_tag_report(metrics, rules, key="tier")
        values = {r.tag_value: r for r in result}
        assert values["etl"].critical == 1
        assert values["batch"].healthy == 1


class TestFormatTagReport:
    def test_empty_summaries_returns_message(self):
        assert format_tag_report([]) == "No tag groups found."

    def test_format_contains_tag_value(self):
        s = TagGroupSummary(
            tag_value="production", total=5, healthy=4, warning=1, critical=0, unknown=0
        )
        output = format_tag_report([s])
        assert "production" in output
        assert "5" in output

    def test_format_has_header(self):
        s = TagGroupSummary(
            tag_value="x", total=1, healthy=1, warning=0, critical=0, unknown=0
        )
        output = format_tag_report([s])
        assert "Tag" in output
        assert "Total" in output
