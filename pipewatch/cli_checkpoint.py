"""CLI commands for checkpoint management."""
import click
import json
from pipewatch.checkpoint import Checkpoint, CheckpointStore

_store = CheckpointStore()


def _get_store() -> CheckpointStore:
    return _store


@click.group()
def checkpoint():
    """Manage pipeline checkpoints."""


@checkpoint.command("record")
@click.argument("pipeline")
@click.argument("marker")
def cmd_record(pipeline: str, marker: str):
    """Record a checkpoint marker for a pipeline."""
    store = _get_store()
    cp = Checkpoint(pipeline=pipeline, marker=marker)
    diff = store.compare(pipeline, marker)
    store.record(cp)
    if diff.changed:
        prev = diff.previous or "(none)"
        click.echo(f"[{pipeline}] checkpoint updated: {prev} -> {marker}")
    else:
        click.echo(f"[{pipeline}] checkpoint unchanged: {marker}")


@checkpoint.command("show")
@click.argument("pipeline")
def cmd_show(pipeline: str):
    """Show the latest checkpoint for a pipeline."""
    store = _get_store()
    cp = store.get(pipeline)
    if cp is None:
        click.echo(f"No checkpoint found for '{pipeline}'.")
    else:
        click.echo(f"[{pipeline}] marker={cp.marker}  recorded_at={cp.recorded_at}")


@checkpoint.command("list")
def cmd_list():
    """List all recorded checkpoints."""
    store = _get_store()
    entries = store.all()
    if not entries:
        click.echo("No checkpoints recorded.")
        return
    for pipeline, cp in entries.items():
        click.echo(f"{pipeline}: {cp.marker} @ {cp.recorded_at}")


@checkpoint.command("json")
def cmd_json():
    """Dump all checkpoints as JSON."""
    store = _get_store()
    data = [cp.to_dict() for cp in store.all().values()]
    click.echo(json.dumps(data, indent=2))


@checkpoint.command("clear")
@click.argument("pipeline")
def cmd_clear(pipeline: str):
    """Remove the checkpoint for a pipeline."""
    store = _get_store()
    removed = store.clear(pipeline)
    if removed:
        click.echo(f"Checkpoint cleared for '{pipeline}'.")
    else:
        click.echo(f"No checkpoint found for '{pipeline}'.")
