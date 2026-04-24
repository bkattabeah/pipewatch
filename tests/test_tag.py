"""Tests for pipewatch.tag module."""
from __future__ import annotations

import pytest
from datetime import datetime, timezone

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.tag import Tag, TagRule, apply_tags, group_by_tag


def make_metric(
    name: str = "pipe",
    status: PipelineStatus = PipelineStatus.HEALTHY,
    processed: int = 100,
    failed: int = 0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        processed=processed,
        failed=failed,
        status=status,
        timestamp=datetime.now(timezone.utc),
    )


class TestTag:
    def test_str_representation(self):
        t = Tag(key="env", value="prod")
        assert str(t) == "env:prod"

    def test_to_dict(self):
        t = Tag(key="team", value="data")
        d = t.to_dict()
        assert d == {"key": "team", "value": "data"}


class TestTagRule:
    def test_matches_no_constraints(self):
        rule = TagRule(key="env", value="prod")
        metric = make_metric()
        assert rule.matches(metric) is True

    def test_matches_by_name_prefix(self):
        rule = TagRule(key="env", value="prod", name_prefix="etl_")
        assert rule.matches(make_metric(name="etl_sales")) is True
        assert rule.matches(make_metric(name="batch_sales")) is False

    def test_matches_by_status(self):
        rule = TagRule(key="sev", value="high", status="critical")
        assert rule.matches(make_metric(status=PipelineStatus.CRITICAL)) is True
        assert rule.matches(make_metric(status=PipelineStatus.HEALTHY)) is False

    def test_matches_both_constraints(self):
        rule = TagRule(key="x", value="y", name_prefix="etl_", status="warning")
        assert rule.matches(make_metric(name="etl_x", status=PipelineStatus.WARNING)) is True
        assert rule.matches(make_metric(name="etl_x", status=PipelineStatus.HEALTHY)) is False
        assert rule.matches(make_metric(name="batch", status=PipelineStatus.WARNING)) is False

    def test_to_tag(self):
        rule = TagRule(key="env", value="staging")
        tag = rule.to_tag()
        assert tag.key == "env"
        assert tag.value == "staging"


class TestApplyTags:
    def test_no_rules_returns_empty(self):
        metric = make_metric()
        assert apply_tags(metric, []) == []

    def test_matching_rule_returns_tag(self):
        rule = TagRule(key="env", value="prod", status="healthy")
        metric = make_metric(status=PipelineStatus.HEALTHY)
        tags = apply_tags(metric, [rule])
        assert len(tags) == 1
        assert tags[0].key == "env"

    def test_non_matching_rule_excluded(self):
        rule = TagRule(key="env", value="prod", status="critical")
        metric = make_metric(status=PipelineStatus.HEALTHY)
        assert apply_tags(metric, [rule]) == []

    def test_multiple_rules_multiple_tags(self):
        rules = [
            TagRule(key="env", value="prod"),
            TagRule(key="tier", value="gold", name_prefix="etl_"),
        ]
        metric = make_metric(name="etl_orders")
        tags = apply_tags(metric, rules)
        assert len(tags) == 2


class TestGroupByTag:
    def test_untagged_bucket_when_no_match(self):
        metrics = [make_metric(name="pipe1", status=PipelineStatus.HEALTHY)]
        rule = TagRule(key="env", value="prod", status="critical")
        groups = group_by_tag(metrics, [rule], key="env")
        assert "__untagged__" in groups

    def test_groups_by_first_matching_tag_value(self):
        metrics = [
            make_metric(name="p1", status=PipelineStatus.CRITICAL),
            make_metric(name="p2", status=PipelineStatus.HEALTHY),
        ]
        rules = [TagRule(key="sev", value="high", status="critical")]
        groups = group_by_tag(metrics, rules, key="sev")
        assert "high" in groups
        assert len(groups["high"]) == 1
        assert groups["high"][0].pipeline_name == "p1"
