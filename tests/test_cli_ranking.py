"""Tests for pipewatch.cli_ranking CLI commands."""
from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.cli_ranking import cmd_ranking_show, cmd_ranking_json


def _make_metric(name: str, status: PipelineStatus, failed: int = 0) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        status=status,
        processed=100,
        failed=failed,
        timestamp=datetime(2024, 6, 1, 10, 0, 0),
    )


def _mock_collector(metrics: list[PipelineMetric]) -> MagicMock:
    collector = MagicMock()
    names = [m.pipeline_name for m in metrics]
    collector.list_pipelines.return_value = names
    metric_map = {m.pipeline_name: m for m in metrics}
    collector.latest.side_effect = lambda name: metric_map.get(name)
    return collector


class TestCmdRankingShow:
    def test_no_data_prints_message(self):
        with patch("pipewatch.cli_ranking._get_collector", return_value=_mock_collector([])):
            runner = CliRunner()
            result = runner.invoke(cmd_ranking_show, [])
        assert result.exit_code == 0
        assert "No pipeline data" in result.output

    def test_shows_header_row(self):
        metrics = [_make_metric("alpha", PipelineStatus.HEALTHY)]
        with patch("pipewatch.cli_ranking._get_collector", return_value=_mock_collector(metrics)):
            runner = CliRunner()
            result = runner.invoke(cmd_ranking_show, [])
        assert "Rank" in result.output
        assert "Pipeline" in result.output
        assert "Score" in result.output

    def test_critical_pipeline_appears_first(self):
        metrics = [
            _make_metric("healthy_pipe", PipelineStatus.HEALTHY),
            _make_metric("critical_pipe", PipelineStatus.CRITICAL, failed=60),
        ]
        with patch("pipewatch.cli_ranking._get_collector", return_value=_mock_collector(metrics)):
            runner = CliRunner()
            result = runner.invoke(cmd_ranking_show, [])
        lines = result.output.splitlines()
        data_lines = [l for l in lines if "critical_pipe" in l or "healthy_pipe" in l]
        assert data_lines[0].strip().startswith("1")
        assert "critical_pipe" in data_lines[0]

    def test_top_option_limits_output(self):
        metrics = [_make_metric(f"pipe_{i}", PipelineStatus.HEALTHY) for i in range(10)]
        with patch("pipewatch.cli_ranking._get_collector", return_value=_mock_collector(metrics)):
            runner = CliRunner()
            result = runner.invoke(cmd_ranking_show, ["--top", "3"])
        data_lines = [l for l in result.output.splitlines() if "pipe_" in l]
        assert len(data_lines) == 3


class TestCmdRankingJson:
    def test_returns_valid_json(self):
        import json
        metrics = [_make_metric("p", PipelineStatus.WARNING, failed=10)]
        with patch("pipewatch.cli_ranking._get_collector", return_value=_mock_collector(metrics)):
            runner = CliRunner()
            result = runner.invoke(cmd_ranking_json, [])
        data = json.loads(result.output)
        assert isinstance(data, list)
        assert data[0]["pipeline_name"] == "p"

    def test_top_zero_returns_all(self):
        import json
        metrics = [_make_metric(f"p{i}", PipelineStatus.HEALTHY) for i in range(5)]
        with patch("pipewatch.cli_ranking._get_collector", return_value=_mock_collector(metrics)):
            runner = CliRunner()
            result = runner.invoke(cmd_ranking_json, ["--top", "0"])
        data = json.loads(result.output)
        assert len(data) == 5
