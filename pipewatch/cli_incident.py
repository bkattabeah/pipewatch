"""CLI commands for incident management."""
from __future__ import annotations

import json
import click

from pipewatch.incident import IncidentManager, IncidentManagerConfig
from pipewatch.collector import MetricCollector
from pipewatch.alerts import AlertEngine
from pipewatch.cli import build_default_engine

_manager: IncidentManager = IncidentManager()


def _get_manager() -> IncidentManager:
    return _manager


@click.group()
def incident() -> None:
    """Manage pipeline incidents."""


@incident.command("show")
@click.option("--all", "show_all", is_flag=True, default=False, help="Include resolved incidents.")
def cmd_show(show_all: bool) -> None:
    """Show current incidents."""
    manager = _get_manager()
    incidents = manager.all_incidents() if show_all else manager.open_incidents()
    if not incidents:
        click.echo("No incidents found.")
        return
    for inc in incidents:
        status = "OPEN" if inc.is_open else "RESOLVED"
        click.echo(
            f"[{status}] {inc.pipeline} | {inc.severity.upper()} | "
            f"alerts={inc.alert_count} | opened={inc.opened_at.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        )
        click.echo(f"  {inc.message}")


@incident.command("json")
@click.option("--all", "show_all", is_flag=True, default=False)
def cmd_json(show_all: bool) -> None:
    """Output incidents as JSON."""
    manager = _get_manager()
    incidents = manager.all_incidents() if show_all else manager.open_incidents()
    click.echo(json.dumps([i.to_dict() for i in incidents], indent=2))


@incident.command("resolve")
@click.argument("pipeline")
def cmd_resolve(pipeline: str) -> None:
    """Resolve an open incident for PIPELINE."""
    manager = _get_manager()
    resolved = manager.resolve(pipeline)
    if resolved:
        click.echo(f"Resolved incident for '{pipeline}' (id={resolved.incident_id}).")
    else:
        click.echo(f"No open incident found for '{pipeline}'.")


@incident.command("clear")
def cmd_clear() -> None:
    """Clear all incident history."""
    _get_manager().clear()
    click.echo("All incidents cleared.")
