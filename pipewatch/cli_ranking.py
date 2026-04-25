"""CLI commands for pipeline ranking."""
from __future__ import annotations

import json

import click

from pipewatch.collector import MetricCollector
from pipewatch.ranking import rank_pipelines


def _get_collector() -> MetricCollector:
    return MetricCollector()


@click.group()
def ranking() -> None:
    """Rank pipelines by health risk."""


@ranking.command("show")
@click.option("--top", default=10, show_default=True, help="Number of top entries to display.")
@click.option("--pipeline", default=None, help="Filter to a specific pipeline.")
def cmd_ranking_show(top: int, pipeline: str | None) -> None:
    """Display ranked pipelines in a table."""
    collector = _get_collector()
    metrics = [
        collector.latest(name)
        for name in collector.list_pipelines()
        if collector.latest(name) is not None
    ]
    if pipeline:
        metrics = [m for m in metrics if m and m.pipeline_name == pipeline]

    result = rank_pipelines([m for m in metrics if m])
    entries = result.top(top)

    if not entries:
        click.echo("No pipeline data available.")
        return

    click.echo(f"{'Rank':<6} {'Pipeline':<30} {'Status':<10} {'Error Rate':>10} {'Score':>8}")
    click.echo("-" * 68)
    for e in entries:
        click.echo(
            f"{e.rank:<6} {e.pipeline_name:<30} {e.status.value:<10}"
            f" {e.error_rate:>10.2%} {e.score:>8.4f}"
        )


@ranking.command("json")
@click.option("--top", default=0, help="Limit output (0 = all).")
def cmd_ranking_json(top: int) -> None:
    """Output pipeline rankings as JSON."""
    collector = _get_collector()
    metrics = [
        collector.latest(name)
        for name in collector.list_pipelines()
        if collector.latest(name) is not None
    ]
    result = rank_pipelines([m for m in metrics if m])
    entries = result.top(top) if top > 0 else result.entries
    click.echo(json.dumps([e.to_dict() for e in entries], indent=2))
