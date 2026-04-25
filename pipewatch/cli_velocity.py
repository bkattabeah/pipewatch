"""CLI commands for pipeline velocity (error-rate change speed) reporting."""
from __future__ import annotations

import json
import click

from pipewatch.collector import MetricCollector
from pipewatch.velocity import VelocityConfig, compute_velocity


def _get_collector() -> MetricCollector:
    return MetricCollector()


@click.group()
def velocity() -> None:
    """Inspect how fast pipeline error rates are changing."""


@velocity.command("show")
@click.option("--window", default=10, show_default=True, help="History window size.")
@click.option(
    "--spike-threshold",
    default=0.10,
    show_default=True,
    help="Max-delta threshold to flag as spike.",
)
def cmd_velocity_show(window: int, spike_threshold: float) -> None:
    """Print a human-readable velocity table for all tracked pipelines."""
    collector = _get_collector()
    config = VelocityConfig(window=window, spike_threshold=spike_threshold)
    try:
        config.validate()
    except ValueError as exc:
        raise click.UsageError(str(exc)) from exc

    pipelines = collector.pipelines()
    if not pipelines:
        click.echo("No pipeline data available.")
        return

    click.echo(f"{'PIPELINE':<30} {'DIRECTION':<10} {'MEAN Δ':>10} {'MAX Δ':>10} {'SPIKE':>6}")
    click.echo("-" * 72)
    for name in sorted(pipelines):
        history = collector.history(name)
        result = compute_velocity(history, config)
        if result is None:
            click.echo(f"{name:<30} {'n/a':<10} {'—':>10} {'—':>10} {'—':>6}")
            continue
        spike_flag = "YES" if result.is_spike else "no"
        click.echo(
            f"{name:<30} {result.direction:<10} "
            f"{result.mean_delta:>10.4f} {result.max_delta:>10.4f} {spike_flag:>6}"
        )


@velocity.command("json")
@click.option("--window", default=10, show_default=True)
@click.option("--spike-threshold", default=0.10, show_default=True)
def cmd_velocity_json(window: int, spike_threshold: float) -> None:
    """Emit velocity results as JSON."""
    collector = _get_collector()
    config = VelocityConfig(window=window, spike_threshold=spike_threshold)
    try:
        config.validate()
    except ValueError as exc:
        raise click.UsageError(str(exc)) from exc

    results = []
    for name in sorted(collector.pipelines()):
        result = compute_velocity(collector.history(name), config)
        if result is not None:
            results.append(result.to_dict())

    click.echo(json.dumps(results, indent=2))
