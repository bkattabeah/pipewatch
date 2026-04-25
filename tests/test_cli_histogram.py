"""Tests for pipewatch.cli_histogram CLI commands."""
from __future__ import annotations

from datetime import datetime
from unittest.mock import patch, MagicMock

from click.testing import CliRunner

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.cli_histogram import cmd_histogram_show, cmd_histogram_json


def _make_metric(name: str, total: int, failed: int) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        total_runs=total,
        failed_runs=failed,
        status=PipelineStatus.HEALTHY,
        timestamp=datetime(2024, 6, 1, 10, 0, 0),
    )


def _mock_collector(metrics: list) -> MagicMock:
    collector = MagicMock()
    collector.latest.return_value = {m.pipeline_name: m for m in metrics}
    return collector


class TestCmdHistogramShow:
    def test_no_data_prints_message(self):
        runner = CliRunner()
        with patch("pipewatch.cli_histogram._get_collector",
                   return_value=_mock_collector([])):
            result = runner.invoke(cmd_histogram_show, [])
        assert result.exit_code == 0
        assert "No data available" in result.output

    def test_shows_histogram_header(self):
        metrics = [_make_metric("pipe_a", 100, 10), _make_metric("pipe_b", 100, 50)]
        runner = CliRunner()
        with patch("pipewatch.cli_histogram._get_collector",
                   return_value=_mock_collector(metrics)):
            result = runner.invoke(cmd_histogram_show, ["--buckets", "4"])
        assert result.exit_code == 0
        assert "histogram" in result.output.lower()
        assert "total=2" in result.output

    def test_pipeline_filter_applied(self):
        metrics = [_make_metric("alpha", 10, 1), _make_metric("beta", 10, 9)]
        runner = CliRunner()
        with patch("pipewatch.cli_histogram._get_collector",
                   return_value=_mock_collector(metrics)):
            result = runner.invoke(cmd_histogram_show, ["--pipeline", "alpha"])
        assert result.exit_code == 0
        assert "total=1" in result.output

    def test_pipeline_filter_no_match_prints_no_data(self):
        metrics = [_make_metric("alpha", 10, 1)]
        runner = CliRunner()
        with patch("pipewatch.cli_histogram._get_collector",
                   return_value=_mock_collector(metrics)):
            result = runner.invoke(cmd_histogram_show, ["--pipeline", "nonexistent"])
        assert result.exit_code == 0
        assert "No data" in result.output


class TestCmdHistogramJson:
    def test_outputs_valid_json(self):
        import json
        metrics = [_make_metric("p", 20, 4)]
        runner = CliRunner()
        with patch("pipewatch.cli_histogram._get_collector",
                   return_value=_mock_collector(metrics)):
            result = runner.invoke(cmd_histogram_json, ["--buckets", "3"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "buckets" in data
        assert "total" in data

    def test_empty_metrics_json_has_zero_total(self):
        import json
        runner = CliRunner()
        with patch("pipewatch.cli_histogram._get_collector",
                   return_value=_mock_collector([])):
            result = runner.invoke(cmd_histogram_json, [])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["total"] == 0
