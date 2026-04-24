"""CLI commands for heartbeat monitoring."""
from __future__ import annotations

import json
from datetime import timezone

import click

from pipewatch.heartbeat import HeartbeatConfig, HeartbeatMonitor

_monitor: HeartbeatMonitor = HeartbeatMonitor()


def _get_monitor() -> HeartbeatMonitor:
    return _monitor


@click.group()
def heartbeat() -> None:
    """Heartbeat liveness monitoring commands."""


@heartbeat.command("ping")
@click.argument("pipeline")
def cmd_ping(pipeline: str) -> None:
    """Record a heartbeat ping for PIPELINE."""
    mon = _get_monitor()
    mon.ping(pipeline)
    click.echo(f"Heartbeat recorded for '{pipeline}'.")


@heartbeat.command("check")
@click.argument("pipeline")
def cmd_check(pipeline: str) -> None:
    """Check liveness status of PIPELINE."""
    mon = _get_monitor()
    status = mon.check(pipeline)
    icon = "✓" if status.alive else "✗"
    click.echo(f"[{icon}] {status.pipeline}: {status.message}")
    if status.last_seen:
        click.echo(f"    Last seen: {status.last_seen.isoformat()}")


@heartbeat.command("check-all")
def cmd_check_all() -> None:
    """Check liveness status of all known pipelines."""
    mon = _get_monitor()
    statuses = mon.check_all()
    if not statuses:
        click.echo("No pipelines registered.")
        return
    for s in statuses:
        icon = "✓" if s.alive else "✗"
        click.echo(f"[{icon}] {s.pipeline}: {s.message}")


@heartbeat.command("json")
def cmd_json() -> None:
    """Output heartbeat statuses as JSON."""
    mon = _get_monitor()
    statuses = mon.check_all()
    click.echo(json.dumps([s.to_dict() for s in statuses], indent=2))
