"""Tests for pipewatch.cli_sla CLI commands."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

from click.testing import CliRunner

from pipewatch.cli_sla import cmd_sla_show, cmd_sla_json
from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.sla import SLAResult


def _make_result(pipeline: str, compliant: bool, error_rate: float = 0.01) -> SLAResult:
    return SLAResult(
        pipeline=pipeline,
        compliant=compliant,
        error_rate=error_rate,
        max_error_rate=0.05,
        sample_count=10,
        checked_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        message="SLA met." if compliant else "SLA breached.",
    )


class TestCmdSlaShow:
    def test_no_data_prints_message(self):
        runner = CliRunner()
        with patch("pipewatch.cli_sla._get_collector") as mock_col:
            col = MagicMock()
            col.pipelines.return_value = []
            mock_col.return_value = col
            result = runner.invoke(cmd_sla_show, [])
        assert result.exit_code == 0
        assert "No pipeline data" in result.output

    def test_compliant_pipeline_shows_pass(self):
        runner = CliRunner()
        with patch("pipewatch.cli_sla._get_collector") as mock_col, \
             patch("pipewatch.cli_sla.check_all_slas") as mock_check:
            col = MagicMock()
            col.pipelines.return_value = ["pipe-a"]
            mock_col.return_value = col
            mock_check.return_value = [_make_result("pipe-a", compliant=True)]
            result = runner.invoke(cmd_sla_show, [])
        assert result.exit_code == 0
        assert "pipe-a" in result.output

    def test_invalid_max_error_rate_exits_with_error(self):
        runner = CliRunner()
        result = runner.invoke(cmd_sla_show, ["--max-error-rate", "2.0"])
        assert result.exit_code != 0


class TestCmdSlaJson:
    def test_outputs_valid_json(self):
        import json
        runner = CliRunner()
        with patch("pipewatch.cli_sla._get_collector") as mock_col, \
             patch("pipewatch.cli_sla.check_all_slas") as mock_check:
            col = MagicMock()
            col.pipelines.return_value = ["pipe-a"]
            mock_col.return_value = col
            mock_check.return_value = [_make_result("pipe-a", compliant=True)]
            result = runner.invoke(cmd_sla_json, [])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert isinstance(data, list)
        assert data[0]["pipeline"] == "pipe-a"

    def test_invalid_window_exits_with_error(self):
        runner = CliRunner()
        result = runner.invoke(cmd_sla_json, ["--window", "0"])
        assert result.exit_code != 0
