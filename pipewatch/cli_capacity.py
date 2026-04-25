"""CLI commands for capacity planning."""
from __future__ import annotations

import json
import click

from pipewatch.capacity import CapacityConfig, compute_capacity
from pipewatch.collector import MetricCollector


def _get_collector() -> MetricCollector:
    return MetricCollector()


@click.group()
def capacity() -> None:
    """Capacity planning commands."""


@capacity.command("show")
@click.option("--pipeline", "-p", required=True, help="Pipeline name")
@click.option("--window", default=60, show_default=True, help="Sample window size")
@click.option("--warn", default=0.75, show_default=True, help="Warn utilisation threshold")
@click.option("--crit", default=0.90, show_default=True, help="Critical utilisation threshold")
def cmd_capacity_show(
    pipeline: str, window: int, warn: float, crit: float
) -> None:
    """Show capacity result for a pipeline."""
    cfg = CapacityConfig(window_size=window, headroom_warn_pct=warn, headroom_crit_pct=crit)
    try:
        cfg.validate()
    except ValueError as exc:
        raise click.BadParameter(str(exc)) from exc

    collector = _get_collector()
    metrics = collector.history(pipeline)
    result = compute_capacity(pipeline, metrics, cfg)
    if result is None:
        click.echo(f"No data for pipeline: {pipeline}")
        return

    colour = {"ok": "green", "warn": "yellow", "critical": "red"}.get(result.status, "white")
    click.echo(f"Pipeline : {result.pipeline}")
    click.echo(f"Samples  : {result.sample_count}")
    click.echo(f"Processed: {result.total_processed}")
    click.echo(f"Failed   : {result.total_failed}")
    click.echo(f"Utilisation: {result.utilisation:.2%}")
    click.echo(f"Headroom   : {result.headroom:.2%}")
    click.secho(f"Status     : {result.status.upper()}", fg=colour)


@capacity.command("json")
@click.option("--pipeline", "-p", required=True, help="Pipeline name")
@click.option("--window", default=60, show_default=True)
def cmd_capacity_json(pipeline: str, window: int) -> None:
    """Emit capacity result as JSON."""
    collector = _get_collector()
    metrics = collector.history(pipeline)
    cfg = CapacityConfig(window_size=window)
    result = compute_capacity(pipeline, metrics, cfg)
    if result is None:
        click.echo(json.dumps({"pipeline": pipeline, "error": "no data"}))
        return
    click.echo(json.dumps(result.to_dict(), indent=2))
