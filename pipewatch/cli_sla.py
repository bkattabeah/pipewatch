"""CLI commands for SLA compliance reporting."""
from __future__ import annotations

import json
import click

from pipewatch.collector import MetricCollector
from pipewatch.sla import SLAConfig, check_all_slas


def _get_collector() -> MetricCollector:
    return MetricCollector()


@click.group()
def sla() -> None:
    """SLA compliance commands."""


@sla.command("show")
@click.option("--max-error-rate", default=0.05, show_default=True, help="Max allowed error rate (0–1).")
@click.option("--window", default=60, show_default=True, help="Window in minutes to evaluate.")
@click.option("--min-samples", default=5, show_default=True, help="Minimum samples required.")
def cmd_sla_show(max_error_rate: float, window: int, min_samples: int) -> None:
    """Show SLA compliance status for all pipelines."""
    config = SLAConfig(
        max_error_rate=max_error_rate,
        window_minutes=window,
        min_samples=min_samples,
    )
    try:
        config.validate()
    except ValueError as exc:
        raise click.ClickException(str(exc))

    collector = _get_collector()
    history = {name: collector.history(name) for name in collector.pipelines()}
    results = check_all_slas(history, config)

    if not results:
        click.echo("No pipeline data available.")
        return

    for r in results:
        status = click.style("PASS", fg="green") if r.compliant else click.style("FAIL", fg="red")
        click.echo(f"[{status}] {r.pipeline} — error rate: {r.error_rate:.2%} (limit {r.max_error_rate:.2%}) | {r.message}")


@sla.command("json")
@click.option("--max-error-rate", default=0.05, show_default=True)
@click.option("--window", default=60, show_default=True)
@click.option("--min-samples", default=5, show_default=True)
def cmd_sla_json(max_error_rate: float, window: int, min_samples: int) -> None:
    """Output SLA compliance results as JSON."""
    config = SLAConfig(
        max_error_rate=max_error_rate,
        window_minutes=window,
        min_samples=min_samples,
    )
    try:
        config.validate()
    except ValueError as exc:
        raise click.ClickException(str(exc))

    collector = _get_collector()
    history = {name: collector.history(name) for name in collector.pipelines()}
    results = check_all_slas(history, config)
    click.echo(json.dumps([r.to_dict() for r in results], indent=2))
