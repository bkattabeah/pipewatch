"""CLI commands for baseline comparison."""

from __future__ import annotations

import json
import click

from pipewatch.baseline import compare_to_baseline
from pipewatch.snapshot_store import SnapshotStore
from pipewatch.snapshot import load_snapshot


def _get_store() -> SnapshotStore:
    return SnapshotStore()


@click.group()
def baseline():
    """Compare pipeline metrics against a saved baseline."""


@baseline.command("compare")
@click.argument("baseline_name")
@click.argument("current_name")
@click.option("--threshold", default=0.05, show_default=True, help="Regression delta threshold.")
def cmd_compare(baseline_name: str, current_name: str, threshold: float):
    """Compare CURRENT_NAME snapshot against BASELINE_NAME snapshot."""
    store = _get_store()
    base_snap = store.load(baseline_name)
    curr_snap = store.load(current_name)
    if base_snap is None:
        click.echo(f"Baseline snapshot '{baseline_name}' not found.", err=True)
        raise SystemExit(1)
    if curr_snap is None:
        click.echo(f"Current snapshot '{current_name}' not found.", err=True)
        raise SystemExit(1)

    report = compare_to_baseline(base_snap, curr_snap, regression_threshold=threshold)

    for entry in report.entries:
        flag = " [REGRESSED]" if entry.regressed else ""
        click.echo(
            f"{entry.pipeline}: baseline={entry.baseline_error_rate:.2%} "
            f"current={entry.current_error_rate:.2%} delta={entry.delta:+.2%}{flag}"
        )
    if report.missing_in_baseline:
        click.echo(f"New pipelines (not in baseline): {', '.join(report.missing_in_baseline)}")
    if report.missing_in_current:
        click.echo(f"Dropped pipelines (not in current): {', '.join(report.missing_in_current)}")
    if report.any_regressions():
        raise SystemExit(2)


@baseline.command("compare-json")
@click.argument("baseline_name")
@click.argument("current_name")
@click.option("--threshold", default=0.05, show_default=True)
def cmd_compare_json(baseline_name: str, current_name: str, threshold: float):
    """Output baseline comparison as JSON."""
    store = _get_store()
    base_snap = store.load(baseline_name)
    curr_snap = store.load(current_name)
    if base_snap is None or curr_snap is None:
        click.echo(json.dumps({"error": "snapshot not found"}), err=True)
        raise SystemExit(1)
    report = compare_to_baseline(base_snap, curr_snap, regression_threshold=threshold)
    click.echo(json.dumps(report.to_dict(), indent=2))
