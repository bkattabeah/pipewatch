"""CLI commands for the error-rate histogram feature."""
from __future__ import annotations

import json
import click

from pipewatch.collector import MetricCollector
from pipewatch.histogram import build_histogram


def _get_collector() -> MetricCollector:
    return MetricCollector()


@click.group()
def histogram() -> None:
    """Error-rate histogram commands."""


@histogram.command("show")
@click.option("--pipeline", default=None, help="Filter to a single pipeline.")
@click.option("--buckets", default=10, show_default=True, help="Number of histogram buckets.")
def cmd_histogram_show(pipeline: str | None, buckets: int) -> None:
    """Print a text histogram of error rates."""
    collector = _get_collector()
    metrics = list(collector.latest().values())
    if pipeline:
        metrics = [m for m in metrics if m.pipeline_name == pipeline]
    if not metrics:
        click.echo("No data available.")
        return

    result = build_histogram(metrics, num_buckets=buckets)
    click.echo(f"Error-rate histogram  (total={result.total}, "
               f"min={result.min_rate:.4f}, max={result.max_rate:.4f}, "
               f"mean={result.mean_rate:.4f})")
    click.echo(f"{'Range':<22} {'Count':>6}  Bar")
    click.echo("-" * 50)
    peak = result.peak_bucket()
    for b in result.buckets:
        bar = "#" * b.count
        marker = " <-- peak" if peak and b is peak and b.count > 0 else ""
        click.echo(f"[{b.low:.4f}, {b.high:.4f})  {b.count:>6}  {bar}{marker}")


@histogram.command("json")
@click.option("--buckets", default=10, show_default=True, help="Number of histogram buckets.")
def cmd_histogram_json(buckets: int) -> None:
    """Emit histogram as JSON."""
    collector = _get_collector()
    metrics = list(collector.latest().values())
    result = build_histogram(metrics, num_buckets=buckets)
    click.echo(json.dumps(result.to_dict(), indent=2))
