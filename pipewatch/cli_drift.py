"""CLI commands for drift detection."""
from __future__ import annotations

import json
import sys

import click

from pipewatch.collector import MetricCollector
from pipewatch.drift import DriftConfig, detect_drift_many


def _get_collector() -> MetricCollector:
    return MetricCollector()


@click.group()
def drift() -> None:
    """Detect metric drift relative to a rolling baseline."""


@drift.command("show")
@click.option("--window", default=10, show_default=True, help="Baseline window size.")
@click.option(
    "--threshold",
    default=0.15,
    show_default=True,
    help="Error-rate delta to flag drift.",
)
def cmd_drift_show(window: int, threshold: float) -> None:
    """Print a human-readable drift report for all pipelines."""
    try:
        config = DriftConfig(window=window, threshold=threshold)
        config.validate()
    except ValueError as exc:
        click.echo(f"Invalid config: {exc}", err=True)
        sys.exit(1)

    collector = _get_collector()
    pipelines = collector.pipelines()
    if not pipelines:
        click.echo("No pipeline data available.")
        return

    all_history = collector.history()
    latest = [collector.latest(p) for p in pipelines if collector.latest(p)]
    results = detect_drift_many(all_history, latest, config)

    click.echo(f"{'Pipeline':<30} {'Baseline':>10} {'Current':>10} {'Delta':>8} {'Drifted':>8}")
    click.echo("-" * 72)
    for r in results:
        flag = "YES" if r.drifted else "no"
        click.echo(
            f"{r.pipeline:<30} {r.baseline_error_rate:>10.4f} "
            f"{r.current_error_rate:>10.4f} {r.delta:>8.4f} {flag:>8}"
        )


@drift.command("json")
@click.option("--window", default=10, show_default=True)
@click.option("--threshold", default=0.15, show_default=True)
def cmd_drift_json(window: int, threshold: float) -> None:
    """Emit drift results as JSON."""
    try:
        config = DriftConfig(window=window, threshold=threshold)
        config.validate()
    except ValueError as exc:
        click.echo(f"Invalid config: {exc}", err=True)
        sys.exit(1)

    collector = _get_collector()
    pipelines = collector.pipelines()
    all_history = collector.history()
    latest = [collector.latest(p) for p in pipelines if collector.latest(p)]
    results = detect_drift_many(all_history, latest, config)
    click.echo(json.dumps([r.to_dict() for r in results], indent=2))
