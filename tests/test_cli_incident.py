"""Tests for pipewatch.cli_incident."""
from __future__ import annotations

import json
from unittest.mock import patch, MagicMock

import pytest
from click.testing import CliRunner

from pipewatch.alerts import Alert
from pipewatch.incident import Incident, IncidentManager
from pipewatch.cli_incident import incident


def make_manager_with_incidents() -> IncidentManager:
    manager = IncidentManager()
    manager.process(Alert(pipeline="pipe_a", severity="critical", message="High error rate"))
    manager.process(Alert(pipeline="pipe_b", severity="warning", message="Slow throughput"))
    return manager


class TestCmdShow:
    def test_no_incidents_prints_message(self):
        runner = CliRunner()
        with patch("pipewatch.cli_incident._get_manager", return_value=IncidentManager()):
            result = runner.invoke(incident, ["show"])
        assert result.exit_code == 0
        assert "No incidents found" in result.output

    def test_shows_open_incidents(self):
        runner = CliRunner()
        manager = make_manager_with_incidents()
        with patch("pipewatch.cli_incident._get_manager", return_value=manager):
            result = runner.invoke(incident, ["show"])
        assert result.exit_code == 0
        assert "pipe_a" in result.output
        assert "pipe_b" in result.output
        assert "OPEN" in result.output

    def test_all_flag_includes_resolved(self):
        runner = CliRunner()
        manager = make_manager_with_incidents()
        manager.resolve("pipe_a")
        with patch("pipewatch.cli_incident._get_manager", return_value=manager):
            result = runner.invoke(incident, ["show", "--all"])
        assert "RESOLVED" in result.output or "pipe_a" in result.output


class TestCmdJson:
    def test_outputs_valid_json(self):
        runner = CliRunner()
        manager = make_manager_with_incidents()
        with patch("pipewatch.cli_incident._get_manager", return_value=manager):
            result = runner.invoke(incident, ["json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert isinstance(data, list)
        assert len(data) == 2

    def test_empty_returns_empty_list(self):
        runner = CliRunner()
        with patch("pipewatch.cli_incident._get_manager", return_value=IncidentManager()):
            result = runner.invoke(incident, ["json"])
        assert json.loads(result.output) == []


class TestCmdResolve:
    def test_resolve_known_pipeline(self):
        runner = CliRunner()
        manager = make_manager_with_incidents()
        with patch("pipewatch.cli_incident._get_manager", return_value=manager):
            result = runner.invoke(incident, ["resolve", "pipe_a"])
        assert result.exit_code == 0
        assert "Resolved" in result.output

    def test_resolve_unknown_pipeline(self):
        runner = CliRunner()
        with patch("pipewatch.cli_incident._get_manager", return_value=IncidentManager()):
            result = runner.invoke(incident, ["resolve", "ghost_pipe"])
        assert result.exit_code == 0
        assert "No open incident" in result.output


class TestCmdClear:
    def test_clear_empties_manager(self):
        runner = CliRunner()
        manager = make_manager_with_incidents()
        with patch("pipewatch.cli_incident._get_manager", return_value=manager):
            result = runner.invoke(incident, ["clear"])
        assert result.exit_code == 0
        assert "cleared" in result.output.lower()
