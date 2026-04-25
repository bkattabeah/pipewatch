"""Tests for pipewatch.cli_capacity CLI commands."""
from __future__ import annotations

from datetime import datetime
from unittest.mock import patch, MagicMock

from click.testing import CliRunner

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.cli_capacity import cmd_capacity_show, cmd_capacity_json


def _make_metric(pipeline: str, processed: int, failed: int) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        processed=processed,
        failed=failed,
        status=PipelineStatus.HEALTHY,
        timestamp=datetime.utcnow(),
    )


def _mock_collector(metrics):
    mc = MagicMock()
    mc.history.return_value = metrics
    return mc


class TestCmdCapacityShow:
    def test_no_data_prints_message(self):
        runner = CliRunner()
        with patch("pipewatch.cli_capacity._get_collector", return_value=_mock_collector([])):
            result = runner.invoke(cmd_capacity_show, ["--pipeline", "pipe"])
        assert result.exit_code == 0
        assert "No data" in result.output

    def test_shows_ok_status(self):
        runner = CliRunner()
        metrics = [_make_metric("pipe", 1000, 5)]
        with patch("pipewatch.cli_capacity._get_collector", return_value=_mock_collector(metrics)):
            result = runner.invoke(cmd_capacity_show, ["--pipeline", "pipe"])
        assert result.exit_code == 0
        assert "OK" in result.output

    def test_shows_warn_status(self):
        runner = CliRunner()
        metrics = [_make_metric("pipe", 100, 80)]
        with patch("pipewatch.cli_capacity._get_collector", return_value=_mock_collector(metrics)):
            result = runner.invoke(
                cmd_capacity_show,
                ["--pipeline", "pipe", "--warn", "0.75", "--crit", "0.90"],
            )
        assert result.exit_code == 0
        assert "WARN" in result.output

    def test_invalid_threshold_exits_with_error(self):
        runner = CliRunner()
        with patch("pipewatch.cli_capacity._get_collector", return_value=_mock_collector([])):
            result = runner.invoke(
                cmd_capacity_show,
                ["--pipeline", "pipe", "--warn", "0.95", "--crit", "0.90"],
            )
        assert result.exit_code != 0


class TestCmdCapacityJson:
    def test_no_data_returns_error_json(self):
        import json
        runner = CliRunner()
        with patch("pipewatch.cli_capacity._get_collector", return_value=_mock_collector([])):
            result = runner.invoke(cmd_capacity_json, ["--pipeline", "pipe"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["error"] == "no data"

    def test_returns_valid_json_with_data(self):
        import json
        runner = CliRunner()
        metrics = [_make_metric("pipe", 500, 50)]
        with patch("pipewatch.cli_capacity._get_collector", return_value=_mock_collector(metrics)):
            result = runner.invoke(cmd_capacity_json, ["--pipeline", "pipe"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["pipeline"] == "pipe"
        assert "utilisation" in data
        assert "headroom" in data
