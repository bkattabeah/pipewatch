"""CLI commands for decay analysis."""

from __future__ import annotations

import json
import click

from pipewatch.collector import MetricCollector
from pipewatch.decay import DecayConfig, analyze_decay


def _get_collector() -> MetricCollector:
    return MetricCollector()


@click.group()
def decay() -> None:
    """Analyze pipeline metric decay over time."""


@decay.command("show")
@click.argument("pipeline")
@click.option("--min-samples", default=5, show_default=True, help="Minimum samples required.")
@click.option("--threshold", default=0.01, show_default=True, help="Slope threshold for decay.")
@click.option("--window", default=20, show_default=True, help="Number of recent metrics to use.")
def cmd_decay_show(pipeline: str, min_samples: int, threshold: float, window: int) -> None:
    """Show decay analysis for a pipeline."""
    try:
        config = DecayConfig(min_samples=min_samples, decay_threshold=threshold, window=window)
        config.validate()
    except ValueError as exc:
        raise click.UsageError(str(exc)) from exc

    collector = _get_collector()
    metrics = collector.history(pipeline)

    if not metrics:
        click.echo(f"No data for pipeline: {pipeline}")
        return

    result = analyze_decay(metrics, config)
    if result is None:
        click.echo(f"Insufficient samples for pipeline: {pipeline} (need {min_samples})")
        return

    status = "DECAYING" if result.is_decaying else "STABLE"
    click.echo(f"Pipeline : {result.pipeline}")
    click.echo(f"Status   : {status}")
    click.echo(f"Slope    : {result.slope:.6f}")
    click.echo(f"Samples  : {result.sample_count}")
    click.echo(f"First ER : {result.first_error_rate:.4f}")
    click.echo(f"Last ER  : {result.last_error_rate:.4f}")


@decay.command("json")
@click.argument("pipeline")
@click.option("--min-samples", default=5, show_default=True)
@click.option("--threshold", default=0.01, show_default=True)
@click.option("--window", default=20, show_default=True)
def cmd_decay_json(pipeline: str, min_samples: int, threshold: float, window: int) -> None:
    """Output decay analysis as JSON."""
    try:
        config = DecayConfig(min_samples=min_samples, decay_threshold=threshold, window=window)
        config.validate()
    except ValueError as exc:
        raise click.UsageError(str(exc)) from exc

    collector = _get_collector()
    metrics = collector.history(pipeline)
    result = analyze_decay(metrics, config)
    data = result.to_dict() if result else {"pipeline": pipeline, "error": "insufficient data"}
    click.echo(json.dumps(data, indent=2))
