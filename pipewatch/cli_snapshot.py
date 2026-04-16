"""CLI commands for snapshot management."""

from __future__ import annotations

import json
from pathlib import Path

import click

from pipewatch.snapshot import capture
from pipewatch.snapshot_store import SnapshotStore, DEFAULT_STORE_DIR
from pipewatch.collector import MetricCollector


def _get_store(store_dir: str) -> SnapshotStore:
    return SnapshotStore(store_dir=Path(store_dir))


@click.group("snapshot")
def snapshot():
    """Manage pipeline metric snapshots."""


@snapshot.command("take")
@click.argument("pipeline_id")
@click.option("--store-dir", default=str(DEFAULT_STORE_DIR), show_default=True)
def cmd_take(pipeline_id: str, store_dir: str) -> None:
    """Capture and save a snapshot for a pipeline."""
    collector = MetricCollector()
    metrics = collector.history(pipeline_id)
    if not metrics:
        click.echo(f"No metrics found for pipeline '{pipeline_id}'.")
        return
    snap = capture(pipeline_id, metrics)
    store = _get_store(store_dir)
    store.save(snap)
    click.echo(f"Snapshot saved for '{pipeline_id}' at t={snap.timestamp:.2f}")


@snapshot.command("latest")
@click.argument("pipeline_id")
@click.option("--store-dir", default=str(DEFAULT_STORE_DIR), show_default=True)
def cmd_latest(pipeline_id: str, store_dir: str) -> None:
    """Show the latest snapshot for a pipeline."""
    store = _get_store(store_dir)
    snap = store.latest(pipeline_id)
    if snap is None:
        click.echo(f"No snapshots found for '{pipeline_id}'.")
        return
    click.echo(json.dumps(snap.to_dict(), indent=2))


@snapshot.command("list")
@click.argument("pipeline_id")
@click.option("--store-dir", default=str(DEFAULT_STORE_DIR), show_default=True)
def cmd_list(pipeline_id: str, store_dir: str) -> None:
    """List all stored snapshots for a pipeline."""
    store = _get_store(store_dir)
    snaps = store.list(pipeline_id)
    if not snaps:
        click.echo(f"No snapshots for '{pipeline_id}'.")
        return
    for s in snaps:
        click.echo(f"  [{s.timestamp:.2f}] {len(s.metrics)} metric(s)")


@snapshot.command("clear")
@click.argument("pipeline_id")
@click.option("--store-dir", default=str(DEFAULT_STORE_DIR), show_default=True)
def cmd_clear(pipeline_id: str, store_dir: str) -> None:
    """Delete all snapshots for a pipeline."""
    store = _get_store(store_dir)
    store.clear(pipeline_id)
    click.echo(f"Cleared snapshots for '{pipeline_id}'.")
