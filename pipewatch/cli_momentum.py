"""CLI commands for momentum analysis."""

from __future__ import annotations

import json
import click

from pipewatch.collector import MetricCollector
from pipewatch.momentum import MomentumConfig, analyze_momentum


def _get_collector() -> MetricCollector:
    return MetricCollector()


@click.group()
def momentum() -> None:
    """Analyse error-rate acceleration for pipelines."""


@momentum.command("show")
@click.option("--window", default=10, show_default=True, help="Metric window size.")
@click.option("--min-samples", default=4, show_default=True, help="Minimum samples.")
@click.option("--threshold", default=0.05, show_default=True, help="Acceleration threshold.")
def cmd_momentum_show(window: int, min_samples: int, threshold: float) -> None:
    """Print momentum analysis for all tracked pipelines."""
    collector = _get_collector()
    pipelines = collector.pipelines()

    if not pipelines:
        click.echo("No pipeline data available.")
        return

    try:
        cfg = MomentumConfig(window=window, min_samples=min_samples, accel_threshold=threshold)
        cfg.validate()
    except ValueError as exc:
        click.echo(f"Invalid config: {exc}", err=True)
        raise SystemExit(1)

    click.echo(f"{'Pipeline':<30} {'Direction':<12} {'Accel':>10} {'Samples':>8}")
    click.echo("-" * 64)

    for name in sorted(pipelines):
        metrics = collector.history(name)
        result = analyze_momentum(name, metrics, cfg)
        if result is None:
            click.echo(f"{name:<30} {'insufficient':<12} {'N/A':>10} {len(metrics):>8}")
        else:
            flag = "*" if result.is_accelerating else " "
            click.echo(
                f"{name:<30} {result.direction:<12} {result.acceleration:>10.4f}{flag} {result.sample_count:>8}"
            )


@momentum.command("json")
@click.option("--window", default=10, show_default=True)
@click.option("--min-samples", default=4, show_default=True)
@click.option("--threshold", default=0.05, show_default=True)
def cmd_momentum_json(window: int, min_samples: int, threshold: float) -> None:
    """Emit momentum analysis as JSON."""
    collector = _get_collector()
    pipelines = collector.pipelines()

    try:
        cfg = MomentumConfig(window=window, min_samples=min_samples, accel_threshold=threshold)
        cfg.validate()
    except ValueError as exc:
        click.echo(f"Invalid config: {exc}", err=True)
        raise SystemExit(1)

    results = []
    for name in sorted(pipelines):
        metrics = collector.history(name)
        result = analyze_momentum(name, metrics, cfg)
        if result is not None:
            results.append(result.to_dict())
        else:
            results.append({"pipeline": name, "direction": "insufficient", "sample_count": len(metrics)})

    click.echo(json.dumps(results, indent=2))
