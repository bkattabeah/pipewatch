"""CLI commands for pipeline maturity scoring."""

from __future__ import annotations

import json
import click

from pipewatch.collector import MetricCollector
from pipewatch.maturity import MaturityConfig, compute_maturity


def _get_collector() -> MetricCollector:
    return MetricCollector()


@click.group()
def maturity() -> None:
    """Pipeline maturity scoring commands."""


@maturity.command("show")
@click.option("--min-samples", default=10, show_default=True, help="Minimum samples required.")
@click.option("--window", default=20, show_default=True, help="Stable window size.")
def cmd_maturity_show(min_samples: int, window: int) -> None:
    """Print maturity scores for all tracked pipelines."""
    collector = _get_collector()
    cfg = MaturityConfig(min_samples=min_samples, stable_window=window)

    pipelines = collector.list_pipelines() if hasattr(collector, "list_pipelines") else []
    if not pipelines:
        click.echo("No pipeline data available.")
        return

    click.echo(f"{'Pipeline':<30} {'Grade':>5} {'Score':>7} {'Stable':>7}")
    click.echo("-" * 55)
    for name in sorted(pipelines):
        history = collector.history(name)
        result = compute_maturity(name, history, cfg)
        if result is None:
            click.echo(f"{name:<30} {'N/A':>5} {'N/A':>7} {'N/A':>7}")
        else:
            stable_str = "yes" if result.stable else "no"
            click.echo(
                f"{name:<30} {result.grade:>5} {result.score:>7.3f} {stable_str:>7}"
            )


@maturity.command("json")
@click.option("--min-samples", default=10, show_default=True)
@click.option("--window", default=20, show_default=True)
def cmd_maturity_json(min_samples: int, window: int) -> None:
    """Output maturity scores as JSON."""
    collector = _get_collector()
    cfg = MaturityConfig(min_samples=min_samples, stable_window=window)

    pipelines = collector.list_pipelines() if hasattr(collector, "list_pipelines") else []
    results = []
    for name in sorted(pipelines):
        history = collector.history(name)
        result = compute_maturity(name, history, cfg)
        if result is not None:
            results.append(result.to_dict())

    click.echo(json.dumps(results, indent=2))
