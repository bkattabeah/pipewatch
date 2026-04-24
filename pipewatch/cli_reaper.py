"""CLI commands for the alert reaper."""

from __future__ import annotations

import json
from datetime import datetime

import click

from pipewatch.incident import IncidentManager
from pipewatch.reaper import AlertReaper, ReaperConfig


def _get_reaper(ttl: int, max_reap: int) -> AlertReaper:
    config = ReaperConfig(ttl_seconds=ttl, max_reaped_per_run=max_reap)
    return AlertReaper(config)


@click.group()
def reaper() -> None:
    """Manage stale alert reaping."""


@reaper.command("run")
@click.option("--ttl", default=3600, show_default=True, help="Staleness TTL in seconds.")
@click.option("--max-reap", default=100, show_default=True, help="Max alerts to reap per run.")
def cmd_run(ttl: int, max_reap: int) -> None:
    """Reap stale open incidents."""
    manager = IncidentManager()
    r = _get_reaper(ttl, max_reap)
    result = r.reap(list(manager.all()), now=datetime.utcnow())

    if result.total_reaped == 0:
        click.echo("No stale incidents to reap.")
        return

    click.echo(f"Reaped {result.total_reaped} incident(s):")
    for entry in result.reaped:
        click.echo(f"  [{entry.reaped_at.strftime('%H:%M:%S')}] {entry.pipeline} / {entry.alert_id} — {entry.reason}")
    if result.skipped:
        click.echo(f"  ({result.skipped} skipped — limit reached)")


@reaper.command("json")
@click.option("--ttl", default=3600, show_default=True, help="Staleness TTL in seconds.")
@click.option("--max-reap", default=100, show_default=True, help="Max alerts to reap per run.")
def cmd_json(ttl: int, max_reap: int) -> None:
    """Reap stale open incidents and output JSON."""
    manager = IncidentManager()
    r = _get_reaper(ttl, max_reap)
    result = r.reap(list(manager.all()), now=datetime.utcnow())
    click.echo(json.dumps(result.to_dict(), indent=2))
