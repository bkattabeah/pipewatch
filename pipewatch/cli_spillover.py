"""CLI commands for spillover detection."""

from __future__ import annotations

import json
import sys

import click

from pipewatch.collector import MetricCollector
from pipewatch.spillover import SpilloverConfig, detect_spillover


def _get_collector() -> MetricCollector:
    return MetricCollector()


@click.group()
def spillover() -> None:
    """Detect pipelines whose error rates exceed a spillover threshold."""


@spillover.command("show")
@click.option("--window", default=10, show_default=True, help="Rolling window size.")
@click.option(
    "--threshold",
    default=0.25,
    show_default=True,
    help="Error-rate threshold (0–1).",
)
@click.option(
    "--min-samples",
    default=3,
    show_default=True,
    help="Minimum samples before flagging.",
)
def cmd_spillover_show(
    window: int, threshold: float, min_samples: int
) -> None:
    """Print spillover status for each pipeline."""
    try:
        cfg = SpilloverConfig(window=window, threshold=threshold, min_samples=min_samples)
        cfg.validate()
    except ValueError as exc:
        click.echo(f"[error] Invalid config: {exc}", err=True)
        sys.exit(1)

    collector = _get_collector()
    metrics = [m for ms in collector.history().values() for m in ms]
    if not metrics:
        click.echo("No metric data available.")
        return

    results = detect_spillover(metrics, cfg)
    if not results:
        click.echo("Not enough samples to evaluate spillover.")
        return

    click.echo(f"{'Pipeline':<30} {'Avg Err Rate':>12} {'Samples':>8} {'Spilling':>9}")
    click.echo("-" * 62)
    for r in sorted(results, key=lambda x: x.avg_error_rate, reverse=True):
        flag = "YES" if r.spilling else "no"
        click.echo(
            f"{r.pipeline:<30} {r.avg_error_rate:>12.4f} {r.sample_count:>8} {flag:>9}"
        )


@spillover.command("json")
@click.option("--window", default=10, show_default=True)
@click.option("--threshold", default=0.25, show_default=True)
@click.option("--min-samples", default=3, show_default=True)
def cmd_spillover_json(
    window: int, threshold: float, min_samples: int
) -> None:
    """Emit spillover results as JSON."""
    try:
        cfg = SpilloverConfig(window=window, threshold=threshold, min_samples=min_samples)
        cfg.validate()
    except ValueError as exc:
        click.echo(json.dumps({"error": str(exc)}))
        sys.exit(1)

    collector = _get_collector()
    metrics = [m for ms in collector.history().values() for m in ms]
    results = detect_spillover(metrics, cfg)
    click.echo(json.dumps([r.to_dict() for r in results], indent=2))
