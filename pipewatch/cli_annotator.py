"""CLI commands for pipeline metric annotations."""

import click
import json

from pipewatch.annotator import annotate_many
from pipewatch.collector import MetricCollector
from pipewatch.cli import build_default_engine


def _get_collector() -> MetricCollector:
    from pipewatch.collector import MetricCollector
    return MetricCollector()


@click.group()
def annotate_cmd():
    """Annotation commands for pipeline metrics."""
    pass


@annotate_cmd.command("show")
@click.argument("pipelines", nargs=-1)
def cmd_annotate_show(pipelines):
    """Print annotations for one or more pipelines."""
    collector = _get_collector()
    names = list(pipelines) if pipelines else list(collector.all_pipelines())
    metrics = [collector.latest(p) for p in names if collector.latest(p) is not None]

    if not metrics:
        click.echo("No metrics available.")
        return

    annotations = annotate_many(metrics)
    if not annotations:
        click.echo("No annotations generated.")
        return

    for ann in annotations:
        click.echo(str(ann))


@annotate_cmd.command("json")
@click.argument("pipelines", nargs=-1)
def cmd_annotate_json(pipelines):
    """Output annotations as JSON."""
    collector = _get_collector()
    names = list(pipelines) if pipelines else list(collector.all_pipelines())
    metrics = [collector.latest(p) for p in names if collector.latest(p) is not None]

    annotations = annotate_many(metrics)
    click.echo(json.dumps([a.to_dict() for a in annotations], indent=2))
