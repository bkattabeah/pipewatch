"""CLI commands for snapshot retention management."""
import click
import json

from pipewatch.retention import RetentionConfig, apply_retention
from pipewatch.snapshot_store import SnapshotStore
from pipewatch.snapshot import load_snapshot


def _get_store(directory: str) -> SnapshotStore:
    return SnapshotStore(directory)


@click.group()
def retention():
    """Manage snapshot retention policies."""


@retention.command("run")
@click.option("--dir", "directory", default=".pipewatch/snapshots", show_default=True)
@click.option("--max-snapshots", default=50, show_default=True)
@click.option("--max-age-days", default=30, show_default=True)
@click.option("--dry-run", is_flag=True, default=False)
def cmd_run(directory: str, max_snapshots: int, max_age_days: int, dry_run: bool):
    """Apply retention policy and remove old snapshots."""
    store = _get_store(directory)
    ids = store.list()
    snapshots = [load_snapshot(store._path(sid)) for sid in ids]
    config = RetentionConfig(max_snapshots=max_snapshots, max_age_days=max_age_days)
    result = apply_retention(snapshots, config)

    if dry_run:
        click.echo(f"[dry-run] Would remove {len(result.removed)} snapshot(s), keep {result.kept}.")
        for sid in result.removed:
            click.echo(f"  - {sid}")
    else:
        for sid in result.removed:
            path = store._path(sid)
            if path.exists():
                path.unlink()
        click.echo(f"Removed {len(result.removed)} snapshot(s). Kept {result.kept}.")


@retention.command("json")
@click.option("--dir", "directory", default=".pipewatch/snapshots", show_default=True)
@click.option("--max-snapshots", default=50, show_default=True)
@click.option("--max-age-days", default=30, show_default=True)
def cmd_json(directory: str, max_snapshots: int, max_age_days: int):
    """Output retention analysis as JSON without deleting."""
    store = _get_store(directory)
    ids = store.list()
    snapshots = [load_snapshot(store._path(sid)) for sid in ids]
    config = RetentionConfig(max_snapshots=max_snapshots, max_age_days=max_age_days)
    result = apply_retention(snapshots, config)
    click.echo(json.dumps(result.to_dict(), indent=2))
