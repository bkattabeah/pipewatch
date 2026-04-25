"""Tests for pipewatch.cli_drift."""
from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from pipewatch.cli_drift import cmd_drift_show, cmd_drift_json, drift
from pipewatch.metrics import PipelineMetric, PipelineStatus


def _make_metric(
    pipeline: str = "pipe_a",
    total: int = 100,
    failed: int = 5,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        total=total,
        failed=failed,
        status=PipelineStatus.HEALTHY,
        timestamp=datetime(2024, 6, 1, 10, 0, 0),
    )


def _mock_collector(pipelines=None, history=None, latest_map=None):
    collector = MagicMock()
    collector.pipelines.return_value = pipelines or []
    collector.history.return_value = history or []
    collector.latest.side_effect = lambda p: (latest_map or {}).get(p)
    return collector


class TestCmdDriftShow:
    def test_no_data_prints_message(self):
        runner = CliRunner()
        with patch("pipewatch.cli_drift._get_collector", return_value=_mock_collector()):
            result = runner.invoke(cmd_drift_show)
        assert result.exit_code == 0
        assert "No pipeline data" in result.output

    def test_shows_header_row(self):
        m = _make_metric()
        collector = _mock_collector(
            pipelines=["pipe_a"],
            history=[m],
            latest_map={"pipe_a": m},
        )
        runner = CliRunner()
        with patch("pipewatch.cli_drift._get_collector", return_value=collector):
            result = runner.invoke(cmd_drift_show)
        assert result.exit_code == 0
        assert "Pipeline" in result.output
        assert "Drifted" in result.output

    def test_invalid_threshold_exits_with_error(self):
        runner = CliRunner()
        with patch("pipewatch.cli_drift._get_collector", return_value=_mock_collector()):
            result = runner.invoke(cmd_drift_show, ["--threshold", "0.0"])
        assert result.exit_code == 1
        assert "Invalid config" in result.output


class TestCmdDriftJson:
    def test_returns_valid_json(self):
        import json

        m = _make_metric()
        collector = _mock_collector(
            pipelines=["pipe_a"],
            history=[m],
            latest_map={"pipe_a": m},
        )
        runner = CliRunner()
        with patch("pipewatch.cli_drift._get_collector", return_value=collector):
            result = runner.invoke(cmd_drift_json)
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert isinstance(data, list)
        assert data[0]["pipeline"] == "pipe_a"

    def test_empty_pipelines_returns_empty_array(self):
        import json

        runner = CliRunner()
        with patch("pipewatch.cli_drift._get_collector", return_value=_mock_collector()):
            result = runner.invoke(cmd_drift_json)
        assert result.exit_code == 0
        assert json.loads(result.output) == []
