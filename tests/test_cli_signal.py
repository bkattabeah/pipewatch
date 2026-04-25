"""Tests for pipewatch.cli_signal CLI commands."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from pipewatch.cli_signal import cmd_signal_show, cmd_signal_json
from pipewatch.metrics import PipelineMetric, PipelineStatus


def _make_metric(pipeline: str, failed: int, total: int) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        failed=failed,
        total=total,
        status=PipelineStatus.HEALTHY if failed == 0 else PipelineStatus.WARNING,
    )


def _mock_collector(histories: dict):
    """Return a patched MetricCollector whose history() returns preset lists."""
    collector = MagicMock()
    collector.pipelines.return_value = list(histories.keys())
    collector.history.side_effect = lambda p: histories.get(p, [])
    return collector


class TestCmdSignalShow:
    def test_no_data_prints_message(self):
        collector = _mock_collector({})
        with patch("pipewatch.cli_signal._get_collector", return_value=collector):
            runner = CliRunner()
            result = runner.invoke(cmd_signal_show, [])
        assert result.exit_code == 0
        assert "No data" in result.output

    def test_shows_header_row(self):
        hist = [
            _make_metric("pipe_a", 0, 10),
            _make_metric("pipe_a", 2, 10),
        ]
        collector = _mock_collector({"pipe_a": hist})
        with patch("pipewatch.cli_signal._get_collector", return_value=collector):
            runner = CliRunner()
            result = runner.invoke(cmd_signal_show, [])
        assert result.exit_code == 0
        assert "PIPELINE" in result.output
        assert "SIGNAL" in result.output

    def test_spike_shown_in_output(self):
        hist = [
            _make_metric("pipe_a", 0, 10),
            _make_metric("pipe_a", 5, 10),
        ]
        collector = _mock_collector({"pipe_a": hist})
        with patch("pipewatch.cli_signal._get_collector", return_value=collector):
            runner = CliRunner()
            result = runner.invoke(cmd_signal_show, [])
        assert "spike" in result.output

    def test_single_observation_skipped(self):
        hist = [_make_metric("pipe_a", 1, 10)]
        collector = _mock_collector({"pipe_a": hist})
        with patch("pipewatch.cli_signal._get_collector", return_value=collector):
            runner = CliRunner()
            result = runner.invoke(cmd_signal_show, [])
        assert "No data" in result.output


class TestCmdSignalJson:
    def test_returns_valid_json(self):
        import json
        hist = [
            _make_metric("pipe_x", 0, 20),
            _make_metric("pipe_x", 4, 20),
        ]
        collector = _mock_collector({"pipe_x": hist})
        with patch("pipewatch.cli_signal._get_collector", return_value=collector):
            runner = CliRunner()
            result = runner.invoke(cmd_signal_json, [])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert isinstance(data, list)
        assert data[0]["pipeline"] == "pipe_x"
        assert data[0]["signal"] == "spike"
