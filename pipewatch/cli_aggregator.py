"""CLI commands for pipeline metric aggregation."""
import json
import click
from pipewatch.collector import MetricCollector
from pipewatch.aggregator import aggregate, group_by_status

_collector: MetricCollector = MetricCollector()


def _get_collector() -> MetricCollector:
    return _collector


@click.group(name="aggregate")
def aggregator():
    """Aggregate pipeline health statistics."""


@aggregator.command(name="summary")
@click.option("--json", "as_json", is_flag=True, default=False, help="Output as JSON.")
def cmd_summary(as_json: bool):
    """Print aggregate stats across all tracked pipelines."""
    collector = _get_collector()
    metrics = [collector.latest(pid) for pid in collector.pipelines()]
    metrics = [m for m in metrics if m is not None]
    stats = aggregate(metrics)
    if as_json:
        click.echo(json.dumps(stats.to_dict(), indent=2))
    else:
        click.echo(f"Total pipelines : {stats.total}")
        click.echo(f"Healthy         : {stats.healthy}")
        click.echo(f"Warning         : {stats.warning}")
        click.echo(f"Critical        : {stats.critical}")
        click.echo(f"Unknown         : {stats.unknown}")
        click.echo(f"Avg error rate  : {stats.avg_error_rate:.2%}")
        click.echo(f"Max error rate  : {stats.max_error_rate:.2%}")


@aggregator.command(name="by-status")
def cmd_by_status():
    """List pipelines grouped by status."""
    collector = _get_collector()
    metrics = [collector.latest(pid) for pid in collector.pipelines()]
    metrics = [m for m in metrics if m is not None]
    groups = group_by_status(metrics)
    for status, items in sorted(groups.items()):
        click.echo(f"[{status.upper()}]")
        for m in items:
            click.echo(f"  - {m.pipeline_id}")
