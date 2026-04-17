"""Tests for pipewatch.annotator."""

import pytest
from pipewatch.annotator import annotate, annotate_many, Annotation, AnnotationRule
from pipewatch.metrics import PipelineMetric, PipelineStatus


def make_metric(pipeline="pipe1", processed=100, failed=0, status=PipelineStatus.HEALTHY):
    return PipelineMetric(
        pipeline=pipeline,
        processed=processed,
        failed=failed,
        error_rate=failed / processed if processed else 0.0,
        status=status,
    )


class TestAnnotate:
    def test_healthy_metric_gets_info_annotation(self):
        m = make_metric(processed=50, failed=0, status=PipelineStatus.HEALTHY)
        anns = annotate(m)
        levels = [a.level for a in anns]
        assert "info" in levels

    def test_critical_metric_gets_critical_annotation(self):
        m = make_metric(processed=100, failed=80, status=PipelineStatus.CRITICAL)
        anns = annotate(m)
        levels = [a.level for a in anns]
        assert "critical" in levels

    def test_warning_metric_gets_warning_annotation(self):
        m = make_metric(processed=100, failed=20, status=PipelineStatus.WARNING)
        anns = annotate(m)
        levels = [a.level for a in anns]
        assert "warning" in levels

    def test_zero_processed_gets_info_annotation(self):
        m = make_metric(processed=0, failed=0, status=PipelineStatus.HEALTHY)
        anns = annotate(m)
        messages = [a.message for a in anns]
        assert any("No records" in msg for msg in messages)

    def test_annotation_pipeline_name_set(self):
        m = make_metric(pipeline="etl_main")
        anns = annotate(m)
        assert all(a.pipeline == "etl_main" for a in anns)

    def test_to_dict_has_expected_keys(self):
        ann = Annotation(pipeline="p", level="info", message="ok")
        d = ann.to_dict()
        assert set(d.keys()) == {"pipeline", "level", "message"}

    def test_str_format(self):
        ann = Annotation(pipeline="p", level="warning", message="high errors")
        assert str(ann) == "[WARNING] p: high errors"

    def test_custom_rule_applied(self):
        rule = AnnotationRule(
            level="info",
            condition=lambda m: m.processed > 1000,
            message_fn=lambda m: "High volume pipeline",
        )
        m = make_metric(processed=2000)
        anns = annotate(m, rules=[rule])
        assert len(anns) == 1
        assert anns[0].message == "High volume pipeline"

    def test_annotate_many_aggregates(self):
        metrics = [
            make_metric(pipeline="a", status=PipelineStatus.HEALTHY),
            make_metric(pipeline="b", status=PipelineStatus.CRITICAL, processed=100, failed=90),
        ]
        anns = annotate_many(metrics)
        pipelines = {a.pipeline for a in anns}
        assert "a" in pipelines
        assert "b" in pipelines
