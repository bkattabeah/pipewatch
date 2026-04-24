"""CLI commands for sliding window metric aggregation."""
from __future__ import annotations

import json
import click

from pipewatch.collector import MetricCollector
from pipewatch.window import WindowConfig, compute_window


def _get_collector() -> MetricCollector:
    return MetricCollector()


@click.group()
def window() -> None:
    """Sliding window aggregation commands."""


@window.command("show")
@click.argument("pipeline")
@click.option("--size", default=300, show_default=True, help="Window size in seconds.")
@click.option("--min-samples", default=1, show_default=True, help="Minimum samples required.")
def cmd_window_show(pipeline: str, size: int, min_samples: int) -> None:
    """Show window stats for a pipeline."""
    collector = _get_collector()
    config = WindowConfig(size_seconds=size, min_samples=min_samples)
    all_metrics = collector.history(pipeline)
    stats = compute_window(pipeline, all_metrics, config)

    if stats is None:
        click.echo(f"Not enough samples for pipeline '{pipeline}' in the last {size}s.")
        return

    click.echo(f"Pipeline : {stats.pipeline}")
    click.echo(f"Window   : {stats.window_seconds}s")
    click.echo(f"Samples  : {stats.sample_count}")
    click.echo(f"Avg err  : {stats.avg_error_rate:.2%}")
    click.echo(f"Max err  : {stats.max_error_rate:.2%}")
    click.echo(f"Min err  : {stats.min_error_rate:.2%}")
    click.echo(f"Processed: {stats.total_processed}")
    click.echo(f"Failed   : {stats.total_failed}")


@window.command("json")
@click.argument("pipeline")
@click.option("--size", default=300, show_default=True, help="Window size in seconds.")
@click.option("--min-samples", default=1, show_default=True, help="Minimum samples required.")
def cmd_window_json(pipeline: str, size: int, min_samples: int) -> None:
    """Output window stats as JSON."""
    collector = _get_collector()
    config = WindowConfig(size_seconds=size, min_samples=min_samples)
    all_metrics = collector.history(pipeline)
    stats = compute_window(pipeline, all_metrics, config)

    if stats is None:
        click.echo(json.dumps({"pipeline": pipeline, "error": "insufficient_samples"}))
        return

    click.echo(json.dumps(stats.to_dict(), indent=2))
