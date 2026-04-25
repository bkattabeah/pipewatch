"""CLI commands for the pipeline grouper."""

from __future__ import annotations

import json

import click

from pipewatch.collector import MetricCollector
from pipewatch.grouper import group_by_prefix, group_metrics


def _get_collector() -> MetricCollector:
    return MetricCollector()


@click.group("grouper")
def grouper() -> None:
    """Group pipeline metrics by key and show per-group stats."""


@grouper.command("show")
@click.option("--separator", default="_", show_default=True, help="Prefix separator character.")
@click.option("--sort-by", default="total", show_default=True,
              type=click.Choice(["total", "critical", "warning", "avg_error_rate"]),
              help="Sort groups by this field.")
def cmd_grouper_show(separator: str, sort_by: str) -> None:
    """Print a grouped summary table to stdout."""
    collector = _get_collector()
    metrics = collector.latest()

    if not metrics:
        click.echo("No metrics available.")
        return

    report = group_by_prefix(metrics, separator=separator)
    rows = report.sorted_by(sort_by)

    click.echo(f"{'Group':<20} {'Total':>6} {'Healthy':>8} {'Warning':>8} {'Critical':>9} {'Avg Err%':>10}")
    click.echo("-" * 65)
    for g in rows:
        click.echo(
            f"{g.key:<20} {g.total:>6} {g.healthy:>8} {g.warning:>8} "
            f"{g.critical:>9} {g.avg_error_rate * 100:>9.2f}%"
        )


@grouper.command("json")
@click.option("--separator", default="_", show_default=True, help="Prefix separator character.")
@click.option("--indent", default=2, show_default=True, help="JSON indent level.")
def cmd_grouper_json(separator: str, indent: int) -> None:
    """Emit grouped stats as JSON."""
    collector = _get_collector()
    metrics = collector.latest()
    report = group_by_prefix(metrics, separator=separator)
    click.echo(json.dumps(report.to_dict(), indent=indent))
