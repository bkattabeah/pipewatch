"""Tests for pipewatch.cli_grouper CLI commands."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from pipewatch.cli_grouper import cmd_grouper_show, cmd_grouper_json
from pipewatch.metrics import PipelineMetric, PipelineStatus


def _make_metric(pipeline_id: str, status: PipelineStatus = PipelineStatus.HEALTHY) -> PipelineMetric:
    return PipelineMetric(
        pipeline_id=pipeline_id,
        status=status,
        error_rate=0.05,
        total_records=200,
        failed_records=10,
        recorded_at=datetime(2024, 6, 1, 9, 0, 0),
    )


def _mock_collector(metrics):
    collector = MagicMock()
    collector.latest.return_value = metrics
    return collector


class TestCmdGrouperShow:
    def test_no_data_prints_message(self):
        runner = CliRunner()
        with patch("pipewatch.cli_grouper._get_collector", return_value=_mock_collector([])):
            result = runner.invoke(cmd_grouper_show, [])
        assert result.exit_code == 0
        assert "No metrics available" in result.output

    def test_shows_header_row(self):
        metrics = [_make_metric("etl_sales"), _make_metric("etl_orders")]
        runner = CliRunner()
        with patch("pipewatch.cli_grouper._get_collector", return_value=_mock_collector(metrics)):
            result = runner.invoke(cmd_grouper_show, [])
        assert result.exit_code == 0
        assert "Group" in result.output
        assert "Total" in result.output

    def test_groups_appear_in_output(self):
        metrics = [
            _make_metric("etl_sales"),
            _make_metric("ml_forecast"),
        ]
        runner = CliRunner()
        with patch("pipewatch.cli_grouper._get_collector", return_value=_mock_collector(metrics)):
            result = runner.invoke(cmd_grouper_show, [])
        assert "etl" in result.output
        assert "ml" in result.output


class TestCmdGrouperJson:
    def test_returns_valid_json(self):
        import json
        metrics = [_make_metric("etl_sales"), _make_metric("etl_orders")]
        runner = CliRunner()
        with patch("pipewatch.cli_grouper._get_collector", return_value=_mock_collector(metrics)):
            result = runner.invoke(cmd_grouper_json, [])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "etl" in data

    def test_empty_metrics_returns_empty_json(self):
        import json
        runner = CliRunner()
        with patch("pipewatch.cli_grouper._get_collector", return_value=_mock_collector([])):
            result = runner.invoke(cmd_grouper_json, [])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data == {}

    def test_custom_separator(self):
        import json
        metrics = [
            _make_metric("etl.sales"),
            _make_metric("etl.orders"),
        ]
        runner = CliRunner()
        with patch("pipewatch.cli_grouper._get_collector", return_value=_mock_collector(metrics)):
            result = runner.invoke(cmd_grouper_json, ["--separator", "."])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "etl" in data
