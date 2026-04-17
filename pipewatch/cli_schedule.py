"""CLI commands for scheduled metric collection."""

from __future__ import annotations

import click

from pipewatch.cli import build_default_engine
from pipewatch.collector import MetricCollector
from pipewatch.schedule import ScheduleConfig, Scheduler


@click.group()
def schedule() -> None:
    """Scheduled pipeline monitoring commands."""


def _make_poll_fn(collector: MetricCollector, pipeline: str) -> None:
    engine = build_default_engine()

    def poll() -> None:
        metrics = collector.latest(pipeline)
        if metrics:
            alerts = engine.evaluate(metrics)
            for alert in alerts:
                click.echo(str(alert))
        else:
            click.echo(f"[schedule] no metrics for '{pipeline}'")

    return poll


@schedule.command("run")
@click.argument("pipeline")
@click.option("--interval", default=60.0, show_default=True, help="Poll interval in seconds.")
@click.option("--max-runs", default=None, type=int, help="Stop after N runs.")
@click.option("--stop-on-error", is_flag=True, default=False)
def cmd_run(pipeline: str, interval: float, max_runs: int | None, stop_on_error: bool) -> None:
    """Poll a pipeline on a fixed interval and emit alerts."""
    collector = MetricCollector()
    config = ScheduleConfig(
        interval_seconds=interval,
        max_runs=max_runs,
        stop_on_error=stop_on_error,
    )
    fn = _make_poll_fn(collector, pipeline)
    scheduler = Scheduler(fn, config)
    click.echo(f"Starting scheduler for '{pipeline}' every {interval}s ...")
    try:
        scheduler.start()
        while scheduler.is_running:
            import time
            time.sleep(1)
    except KeyboardInterrupt:
        click.echo("\nStopping scheduler.")
        scheduler.stop()
    click.echo(f"Completed {len(scheduler.results)} run(s).")
