"""CLI commands for alert correlation."""
import json
import click
from pipewatch.correlator import AlertCorrelator, CorrelatorConfig
from pipewatch.alerts import Alert, AlertRule
from pipewatch.collector import MetricCollector
from pipewatch.cli import build_default_engine


def _get_correlator(window: int, min_size: int) -> AlertCorrelator:
    config = CorrelatorConfig(window_seconds=window, min_group_size=min_size)
    return AlertCorrelator(config)


@click.group()
def correlator():
    """Correlate alerts across pipelines."""


@correlator.command("show")
@click.option("--window", default=60, help="Correlation window in seconds.")
@click.option("--min-size", default=2, help="Minimum alerts to form a group.")
@click.option("--pipelines", default="pipeline_a,pipeline_b", help="Comma-separated pipeline names.")
def cmd_correlate_show(window, min_size, pipelines):
    """Show correlated alert groups."""
    from pipewatch.metrics import PipelineMetric, PipelineStatus
    from datetime import datetime

    corr = _get_correlator(window, min_size)
    engine = build_default_engine()
    collector = MetricCollector()

    for name in pipelines.split(","):
        name = name.strip()
        metric = collector.latest(name)
        if metric:
            alerts = engine.evaluate(metric)
            for alert in alerts:
                corr.record(alert)

    groups = corr.correlate()
    if not groups:
        click.echo("No correlated alert groups found.")
        return
    for g in groups:
        click.echo(f"[{g.group_id}] {g.alert_count} alerts across: {', '.join(g.pipelines)}")


@correlator.command("json")
@click.option("--window", default=60)
@click.option("--min-size", default=2)
def cmd_correlate_json(window, min_size):
    """Output correlated groups as JSON."""
    corr = _get_correlator(window, min_size)
    groups = corr.correlate()
    click.echo(json.dumps([g.to_dict() for g in groups], indent=2))
