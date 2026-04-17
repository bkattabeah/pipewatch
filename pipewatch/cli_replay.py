"""CLI commands for replaying historical snapshots."""

import click
import json
from pipewatch.replay import ReplayConfig, replay
from pipewatch.snapshot_store import SnapshotStore


DEFAULT_STORE = ".pipewatch/snapshots"


@click.group()
def replay_cmd():
    """Replay historical pipeline snapshots."""
    pass


@replay_cmd.command("show")
@click.option("--store", default=DEFAULT_STORE, show_default=True)
@click.option("--pipeline", default=None, help="Filter to a specific pipeline")
@click.option("--limit", default=10, show_default=True)
@click.option("--reverse", is_flag=True, default=False)
def cmd_replay_show(store, pipeline, limit, reverse):
    """Print replayed frames as text."""
    config = ReplayConfig(store_dir=store, pipeline=pipeline, limit=limit, reverse=reverse)
    for frame in replay(config):
        snap = frame.snapshot
        click.echo(f"[{frame.index}] {snap.taken_at}  pipelines={list(snap.metrics.keys())}")
        if frame.is_last:
            click.echo("--- end of replay ---")


@replay_cmd.command("json")
@click.option("--store", default=DEFAULT_STORE, show_default=True)
@click.option("--pipeline", default=None)
@click.option("--limit", default=10, show_default=True)
@click.option("--reverse", is_flag=True, default=False)
def cmd_replay_json(store, pipeline, limit, reverse):
    """Output replayed frames as JSON."""
    config = ReplayConfig(store_dir=store, pipeline=pipeline, limit=limit, reverse=reverse)
    frames = [frame.to_dict() for frame in replay(config)]
    click.echo(json.dumps(frames, indent=2, default=str))
