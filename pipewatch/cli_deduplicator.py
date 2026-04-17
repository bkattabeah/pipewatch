"""CLI commands for inspecting deduplication state."""
import json
import click

from pipewatch.deduplicator import AlertDeduplicator, DeduplicatorConfig

_deduplicator: AlertDeduplicator = AlertDeduplicator()


def get_deduplicator() -> AlertDeduplicator:
    return _deduplicator


@click.group()
def dedupe():
    """Manage alert deduplication."""


@dedupe.command("status")
def cmd_status():
    """Show current deduplication entries."""
    d = get_deduplicator()
    entries = d.entries()
    if not entries:
        click.echo("No deduplicated alerts recorded.")
        return
    for e in entries:
        click.echo(
            f"[{e.alert.level}] {e.alert.pipeline}: '{e.alert.message}' "
            f"— seen {e.count}x, first={e.first_seen.isoformat()}, last={e.last_seen.isoformat()}"
        )


@dedupe.command("json")
def cmd_json():
    """Dump deduplication entries as JSON."""
    d = get_deduplicator()
    click.echo(json.dumps([e.to_dict() for e in d.entries()], indent=2))


@dedupe.command("clear")
def cmd_clear():
    """Clear all deduplication history."""
    get_deduplicator().clear()
    click.echo("Deduplication history cleared.")
