"""Tests for pipewatch.cli_heatmap CLI commands."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from pipewatch.cli_heatmap import cmd_heatmap_show, cmd_heatmap_json
from pipewatch.metrics import PipelineMetric, PipelineStatus


def _make_metric(hour: int = 10, error_rate: float = 0.1) -> PipelineMetric:
    return PipelineMetric(
        pipeline="pipe_x",
        timestamp=datetime(2024, 3, 1, hour, 0, 0, tzinfo=timezone.utc),
        total_records=100,
        failed_records=int(error_rate * 100),
        error_rate=error_rate,
        status=PipelineStatus.WARNING,
    )


def _mock_collector(metrics):
    collector = MagicMock()
    collector.history.return_value = metrics
    return collector


class TestCmdHeatmapShow:
    def test_no_data_prints_message(self):
        runner = CliRunner()
        with patch("pipewatch.cli_heatmap._get_collector", return_value=_mock_collector([])):
            result = runner.invoke(cmd_heatmap_show, ["pipe_x"])
        assert result.exit_code == 0
        assert "No data found" in result.output

    def test_shows_heatmap_header(self):
        metrics = [_make_metric(hour=h, error_rate=0.1 * (h % 3 + 1)) for h in range(5)]
        runner = CliRunner()
        with patch("pipewatch.cli_heatmap._get_collector", return_value=_mock_collector(metrics)):
            result = runner.invoke(cmd_heatmap_show, ["pipe_x"])
        assert result.exit_code == 0
        assert "Heatmap for pipeline: pipe_x" in result.output
        assert "Peak hour" in result.output

    def test_shows_hour_column(self):
        metrics = [_make_metric(hour=14, error_rate=0.25)]
        runner = CliRunner()
        with patch("pipewatch.cli_heatmap._get_collector", return_value=_mock_collector(metrics)):
            result = runner.invoke(cmd_heatmap_show, ["pipe_x"])
        assert "14h" in result.output


class TestCmdHeatmapJson:
    def test_returns_valid_json(self):
        metrics = [_make_metric(hour=8, error_rate=0.05)]
        runner = CliRunner()
        with patch("pipewatch.cli_heatmap._get_collector", return_value=_mock_collector(metrics)):
            result = runner.invoke(cmd_heatmap_json, ["pipe_x"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["pipeline"] == "pipe_x"
        assert "buckets" in data

    def test_empty_metrics_returns_empty_buckets(self):
        runner = CliRunner()
        with patch("pipewatch.cli_heatmap._get_collector", return_value=_mock_collector([])):
            result = runner.invoke(cmd_heatmap_json, ["pipe_x"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["buckets"] == []
        assert data["peak_hour"] is None
