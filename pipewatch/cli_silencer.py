"""CLI commands for managing alert silence rules."""
import click
from datetime import datetime, timedelta
from pipewatch.silencer import Silencer, SilenceRule

_silencer = Silencer()


def _get_silencer() -> Silencer:
    return _silencer


@click.group()
def silence():
    """Manage alert silence rules."""
    pass


@silence.command("add")
@click.argument("pipeline")
@click.option("--minutes", default=60, help="Duration in minutes")
@click.option("--reason", default="maintenance", help="Reason for silence")
@click.option("--by", default="user", help="Created by")
def cmd_add(pipeline: str, minutes: int, reason: str, by: str):
    """Silence alerts for a pipeline."""
    s = _get_silencer()
    now = datetime.utcnow()
    rule = SilenceRule(
        pipeline=pipeline,
        reason=reason,
        start=now,
        end=now + timedelta(minutes=minutes),
        created_by=by,
    )
    s.add(rule)
    click.echo(f"Silenced '{pipeline}' for {minutes} minutes. Reason: {reason}")


@silence.command("list")
def cmd_list():
    """List active silence rules."""
    s = _get_silencer()
    rules = s.active_rules()
    if not rules:
        click.echo("No active silence rules.")
        return
    for r in rules:
        click.echo(f"  {r.pipeline}: {r.reason} until {r.end.isoformat()} (by {r.created_by})")


@silence.command("remove")
@click.argument("pipeline")
def cmd_remove(pipeline: str):
    """Remove silence rules for a pipeline."""
    s = _get_silencer()
    removed = s.remove(pipeline)
    click.echo(f"Removed {removed} rule(s) for '{pipeline}'.")


@silence.command("clear-expired")
def cmd_clear_expired():
    """Remove expired silence rules."""
    s = _get_silencer()
    n = s.clear_expired()
    click.echo(f"Cleared {n} expired rule(s).")
