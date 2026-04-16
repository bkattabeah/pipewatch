"""CLI commands for snapshot diffing."""
import json
import click
from pipewatch.snapshot_store import SnapshotStore
from pipewatch.diff import diff_snapshots
from pipewatch.config import load_config


def _get_store() -> SnapshotStore:
    cfg = load_config()
    return SnapshotStore(cfg.snapshot_dir)


@click.group("diff")
def diff():
    """Compare pipeline snapshots."""


@diff.command("latest")
@click.argument("pipeline", required=False)
def cmd_diff_latest(pipeline):
    """Diff the two most recent snapshots."""
    store = _get_store()
    names = store.list()
    if len(names) < 2:
        click.echo("Need at least two snapshots to diff.", err=True)
        raise SystemExit(1)

    old = store.load(names[-2])
    new = store.load(names[-1])
    result = diff_snapshots(old, new)

    if not result.has_changes():
        click.echo("No changes between snapshots.")
        return

    d = result.to_dict()
    if d["added"]:
        click.echo(f"Added pipelines: {', '.join(d['added'])}")
    if d["removed"]:
        click.echo(f"Removed pipelines: {', '.join(d['removed'])}")
    for ch in d["changed"]:
        if pipeline and ch["pipeline"] != pipeline:
            continue
        delta = ch["error_rate_delta"]
        delta_str = f"{delta:+.2%}" if delta is not None else "n/a"
        click.echo(
            f"  {ch['pipeline']}: status {ch['old_status']} -> {ch['new_status']}, "
            f"error_rate delta {delta_str}"
        )


@diff.command("json")
def cmd_diff_json():
    """Output diff of two most recent snapshots as JSON."""
    store = _get_store()
    names = store.list()
    if len(names) < 2:
        click.echo("Need at least two snapshots to diff.", err=True)
        raise SystemExit(1)

    old = store.load(names[-2])
    new = store.load(names[-1])
    result = diff_snapshots(old, new)
    click.echo(json.dumps(result.to_dict(), indent=2))
