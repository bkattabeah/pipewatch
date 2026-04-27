"""Tests for pipewatch.cli_pattern."""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from pipewatch.cli_pattern import cmd_pattern_show, cmd_pattern_json
from pipewatch.metrics import PipelineMetric, PipelineStatus


def _make_metric(
    pipeline: str = "pipe",
    failed: int = 0,
    status: PipelineStatus = PipelineStatus.HEALTHY,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline=pipeline,
        processed=100,
        failed=failed,
        status=status,
        timestamp=datetime.utcnow(),
    )


def _mock_collector(metrics):
    collector = MagicMock()
    collector.history.return_value = metrics
    return collector


class TestCmdPatternShow:
    def setup_method(self):
        self.runner = CliRunner()

    def test_no_data_prints_message(self):
        with patch("pipewatch.cli_pattern._get_collector", return_value=_mock_collector([])):
            result = self.runner.invoke(cmd_pattern_show, ["pipe"])
        assert result.exit_code == 0
        assert "No data" in result.output

    def test_shows_pipeline_name(self):
        metrics = [_make_metric(failed=0) for _ in range(5)]
        with patch("pipewatch.cli_pattern._get_collector", return_value=_mock_collector(metrics)):
            result = self.runner.invoke(cmd_pattern_show, ["pipe"])
        assert result.exit_code == 0
        assert "pipe" in result.output

    def test_shows_pattern_yes_for_recurring(self):
        metrics = [
            _make_metric(failed=20, status=PipelineStatus.CRITICAL) for _ in range(5)
        ]
        with patch("pipewatch.cli_pattern._get_collector", return_value=_mock_collector(metrics)):
            result = self.runner.invoke(
                cmd_pattern_show, ["pipe", "--min-occurrences", "3", "--threshold", "0.1"]
            )
        assert result.exit_code == 0
        assert "YES" in result.output

    def test_invalid_threshold_exits_with_error(self):
        metrics = [_make_metric() for _ in range(5)]
        with patch("pipewatch.cli_pattern._get_collector", return_value=_mock_collector(metrics)):
            result = self.runner.invoke(
                cmd_pattern_show, ["pipe", "--threshold", "2.5"]
            )
        assert result.exit_code != 0


class TestCmdPatternJson:
    def setup_method(self):
        self.runner = CliRunner()

    def test_returns_valid_json(self):
        metrics = [_make_metric() for _ in range(5)]
        with patch("pipewatch.cli_pattern._get_collector", return_value=_mock_collector(metrics)):
            result = self.runner.invoke(cmd_pattern_json, ["pipe"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "pipeline" in data
        assert "has_pattern" in data

    def test_empty_metrics_returns_no_pattern(self):
        with patch("pipewatch.cli_pattern._get_collector", return_value=_mock_collector([])):
            result = self.runner.invoke(cmd_pattern_json, ["pipe"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["has_pattern"] is False
