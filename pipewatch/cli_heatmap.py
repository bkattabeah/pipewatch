"""CLI commands for the pipeline heatmap feature."""

from __future__ import annotations

import json
import click

from pipewatch.collector import MetricCollector
from pipewatch.heatmap import build_heatmap


def _get_collector() -> MetricCollector:
    return MetricCollector()


@click.group()
def heatmap() -> None:
    """Hourly error-rate heatmap for a pipeline."""


@heatmap.command("show")
@click.argument("pipeline")
@click.option("--limit", default=500, show_default=True, help="Max history entries to load.")
def cmd_heatmap_show(pipeline: str, limit: int) -> None:
    """Print a text heatmap of hourly error rates for PIPELINE."""
    collector = _get_collector()
    metrics = collector.history(pipeline, limit=limit)

    if not metrics:
        click.echo(f"No data found for pipeline '{pipeline}'.")
        return

    result = build_heatmap(pipeline, metrics)

    if not result.buckets:
        click.echo("No bucketed data available.")
        return

    click.echo(f"Heatmap for pipeline: {pipeline}")
    click.echo(f"  Peak hour: {result.peak_hour():02d}:00" if result.peak_hour() is not None else "  Peak hour: N/A")
    click.echo(f"  {'Hour':>6}  {'Samples':>8}  {'Avg Err%':>10}  {'Max Err%':>10}")
    click.echo("  " + "-" * 44)
    for bucket in result.buckets:
        bar = "#" * min(int(bucket.avg_error_rate * 40), 40)
        click.echo(
            f"  {bucket.hour:>4}h  {bucket.sample_count:>8}  "
            f"{bucket.avg_error_rate * 100:>9.2f}%  "
            f"{bucket.max_error_rate * 100:>9.2f}%  {bar}"
        )


@heatmap.command("json")
@click.argument("pipeline")
@click.option("--limit", default=500, show_default=True)
def cmd_heatmap_json(pipeline: str, limit: int) -> None:
    """Emit heatmap data as JSON for PIPELINE."""
    collector = _get_collector()
    metrics = collector.history(pipeline, limit=limit)
    result = build_heatmap(pipeline, metrics)
    click.echo(json.dumps(result.to_dict(), indent=2))
