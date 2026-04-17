"""Tests for pipewatch.filter module."""
import pytest
from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.filter import FilterCriteria, filter_metrics, filter_by_status, filter_by_name


def make_metric(name: str, success: int, failure: int) -> PipelineMetric:
    return PipelineMetric(pipeline_name=name, success_count=success, failure_count=failure)


class TestFilterCriteria:
    def setup_method(self):
        self.healthy = make_metric("ingest", 100, 0)
        self.warning = make_metric("transform", 80, 15)
        self.critical = make_metric("export_pipe", 10, 90)
        self.all = [self.healthy, self.warning, self.critical]

    def test_no_criteria_matches_all(self):
        result = filter_metrics(self.all, FilterCriteria())
        assert len(result) == 3

    def test_filter_by_status_healthy(self):
        result = filter_by_status(self.all, PipelineStatus.HEALTHY)
        assert all(m.pipeline_name == "ingest" for m in result)

    def test_filter_by_status_critical(self):
        result = filter_by_status(self.all, PipelineStatus.CRITICAL)
        assert len(result) == 1
        assert result[0].pipeline_name == "export_pipe"

    def test_filter_by_min_error_rate(self):
        result = filter_metrics(self.all, FilterCriteria(min_error_rate=0.5))
        assert len(result) == 1
        assert result[0].pipeline_name == "export_pipe"

    def test_filter_by_max_error_rate(self):
        result = filter_metrics(self.all, FilterCriteria(max_error_rate=0.01))
        assert len(result) == 1
        assert result[0].pipeline_name == "ingest"

    def test_filter_by_name_substring(self):
        result = filter_by_name(self.all, "export")
        assert len(result) == 1
        assert result[0].pipeline_name == "export_pipe"

    def test_filter_by_name_case_insensitive(self):
        result = filter_by_name(self.all, "INGEST")
        assert len(result) == 1

    def test_combined_criteria(self):
        criteria = FilterCriteria(min_error_rate=0.1, max_error_rate=0.5)
        result = filter_metrics(self.all, criteria)
        assert len(result) == 1
        assert result[0].pipeline_name == "transform"

    def test_empty_input(self):
        result = filter_metrics([], FilterCriteria(status=PipelineStatus.HEALTHY))
        assert result == []
