"""CLI commands for the surge detector."""

import json
import click

from pipewatch.collector import MetricCollector
from pipewatch.surge import SurgeConfig, detect_surge


def _get_collector() -> MetricCollector:
    return MetricCollector()


@click.group()
def surge() -> None:
    """Detect sudden surges in pipeline error rates."""


@surge.command("show")
@click.option("--window", default=10, show_default=True, help="Recent sample window size.")
@click.option("--baseline", "baseline_window", default=30, show_default=True, help="Baseline window size.")
@click.option("--multiplier", default=2.0, show_default=True, help="Surge multiplier threshold.")
def cmd_surge_show(window: int, baseline_window: int, multiplier: float) -> None:
    """Print surge detection results for all pipelines."""
    try:
        config = SurgeConfig(window=window, baseline_window=baseline_window, multiplier=multiplier)
        config.validate()
    except ValueError as exc:
        raise click.BadParameter(str(exc))

    collector = _get_collector()
    pipelines = collector.pipelines()

    if not pipelines:
        click.echo("No pipeline data available.")
        return

    click.echo(f"{'Pipeline':<30} {'Current':>10} {'Baseline':>10} {'x':>8} {'Surge':>7}")
    click.echo("-" * 70)

    for name in sorted(pipelines):
        metrics = collector.history(name)
        result = detect_surge(name, metrics, config)
        if result is None:
            click.echo(f"{name:<30} {'insufficient data':>38}")
            continue
        flag = "YES" if result.is_surge else "no"
        click.echo(
            f"{name:<30} {result.current_rate:>10.4f} {result.baseline_rate:>10.4f}"
            f" {result.multiplier_observed:>8.2f} {flag:>7}"
        )


@surge.command("json")
@click.option("--window", default=10, show_default=True)
@click.option("--baseline", "baseline_window", default=30, show_default=True)
@click.option("--multiplier", default=2.0, show_default=True)
def cmd_surge_json(window: int, baseline_window: int, multiplier: float) -> None:
    """Emit surge detection results as JSON."""
    try:
        config = SurgeConfig(window=window, baseline_window=baseline_window, multiplier=multiplier)
        config.validate()
    except ValueError as exc:
        raise click.BadParameter(str(exc))

    collector = _get_collector()
    results = []
    for name in sorted(collector.pipelines()):
        metrics = collector.history(name)
        result = detect_surge(name, metrics, config)
        if result is not None:
            results.append(result.to_dict())

    click.echo(json.dumps(results, indent=2))
