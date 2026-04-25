"""CLI commands for comparing two snapshots of pipeline metrics."""

import json
import click

from pipewatch.comparer import compare_metrics
from pipewatch.snapshot_store import SnapshotStore


def _get_store(directory: str) -> SnapshotStore:
    return SnapshotStore(directory=directory)


@click.group()
def comparer() -> None:
    """Compare pipeline metric snapshots."""


@comparer.command("show")
@click.argument("left_tag")
@click.argument("right_tag")
@click.option("--dir", "directory", default=".pipewatch/snapshots", show_default=True)
def cmd_compare_show(left_tag: str, right_tag: str, directory: str) -> None:
    """Print a human-readable comparison of two snapshots."""
    store = _get_store(directory)
    left_snap = store.load(left_tag)
    right_snap = store.load(right_tag)

    if left_snap is None:
        click.echo(f"Snapshot not found: {left_tag}", err=True)
        raise SystemExit(1)
    if right_snap is None:
        click.echo(f"Snapshot not found: {right_tag}", err=True)
        raise SystemExit(1)

    result = compare_metrics(
        list(left_snap.metrics.values()),
        list(right_snap.metrics.values()),
    )

    click.echo(f"Comparing {left_tag!r} → {right_tag!r}")
    click.echo(f"  Total pipelines : {result.to_dict()['total']}")
    click.echo(f"  Status changes  : {len(result.changed)}")
    click.echo(f"  Added           : {len(result.added)}")
    click.echo(f"  Removed         : {len(result.removed)}")

    if result.changed:
        click.echo("\nChanged pipelines:")
        for c in result.changed:
            delta = f"{c.error_rate_delta:+.3f}" if c.error_rate_delta is not None else "n/a"
            click.echo(
                f"  {c.pipeline_id}: {c.left_status.value if c.left_status else '-'}"
                f" → {c.right_status.value if c.right_status else '-'}"
                f"  (Δ error_rate {delta})"
            )


@comparer.command("json")
@click.argument("left_tag")
@click.argument("right_tag")
@click.option("--dir", "directory", default=".pipewatch/snapshots", show_default=True)
def cmd_compare_json(left_tag: str, right_tag: str, directory: str) -> None:
    """Output comparison result as JSON."""
    store = _get_store(directory)
    left_snap = store.load(left_tag)
    right_snap = store.load(right_tag)

    if left_snap is None or right_snap is None:
        missing = left_tag if left_snap is None else right_tag
        click.echo(json.dumps({"error": f"Snapshot not found: {missing}"}), err=True)
        raise SystemExit(1)

    result = compare_metrics(
        list(left_snap.metrics.values()),
        list(right_snap.metrics.values()),
    )
    click.echo(json.dumps(result.to_dict(), indent=2))
