"""Tests for pipewatch.cli_maturity CLI commands."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.cli_maturity import cmd_maturity_show, cmd_maturity_json


def _make_metric(status: PipelineStatus = PipelineStatus.HEALTHY) -> PipelineMetric:
    return PipelineMetric(
        pipeline="alpha",
        processed=100,
        failed=0,
        duration_seconds=1.0,
        timestamp=datetime.utcnow(),
        status=status,
    )


def _mock_collector(pipelines: list, history_map: dict):
    collector = MagicMock()
    collector.list_pipelines.return_value = pipelines
    collector.history.side_effect = lambda name: history_map.get(name, [])
    return collector


class TestCmdMaturityShow:
    def test_no_data_prints_message(self):
        collector = _mock_collector([], {})
        runner = CliRunner()
        with patch("pipewatch.cli_maturity._get_collector", return_value=collector):
            result = runner.invoke(cmd_maturity_show, [])
        assert result.exit_code == 0
        assert "No pipeline data" in result.output

    def test_shows_header_row(self):
        metrics = [_make_metric() for _ in range(20)]
        collector = _mock_collector(["alpha"], {"alpha": metrics})
        runner = CliRunner()
        with patch("pipewatch.cli_maturity._get_collector", return_value=collector):
            result = runner.invoke(cmd_maturity_show, [])
        assert "Pipeline" in result.output
        assert "Grade" in result.output

    def test_shows_grade_for_healthy_pipeline(self):
        metrics = [_make_metric() for _ in range(20)]
        collector = _mock_collector(["alpha"], {"alpha": metrics})
        runner = CliRunner()
        with patch("pipewatch.cli_maturity._get_collector", return_value=collector):
            result = runner.invoke(cmd_maturity_show, [])
        assert "A" in result.output

    def test_na_when_insufficient_samples(self):
        metrics = [_make_metric() for _ in range(3)]
        collector = _mock_collector(["alpha"], {"alpha": metrics})
        runner = CliRunner()
        with patch("pipewatch.cli_maturity._get_collector", return_value=collector):
            result = runner.invoke(cmd_maturity_show, ["--min-samples", "10"])
        assert "N/A" in result.output


class TestCmdMaturityJson:
    def test_outputs_valid_json(self):
        import json
        metrics = [_make_metric() for _ in range(20)]
        collector = _mock_collector(["alpha"], {"alpha": metrics})
        runner = CliRunner()
        with patch("pipewatch.cli_maturity._get_collector", return_value=collector):
            result = runner.invoke(cmd_maturity_json, [])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert isinstance(data, list)
        assert data[0]["pipeline"] == "alpha"

    def test_empty_when_no_pipelines(self):
        import json
        collector = _mock_collector([], {})
        runner = CliRunner()
        with patch("pipewatch.cli_maturity._get_collector", return_value=collector):
            result = runner.invoke(cmd_maturity_json, [])
        assert json.loads(result.output) == []
