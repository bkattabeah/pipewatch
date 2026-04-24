"""CLI commands for tag-based metric grouping."""
from __future__ import annotations

import json
import click

from pipewatch.collector import MetricCollector
from pipewatch.tag import TagRule, apply_tags, group_by_tag
from pipewatch.metrics import PipelineStatus


def _get_collector() -> MetricCollector:
    return MetricCollector()


@click.group()
def tag() -> None:
    """Tag-based pipeline grouping commands."""


@tag.command("show")
@click.argument("pipeline")
@click.option("--prefix", default=None, help="Name prefix rule")
@click.option("--status", default=None, help="Status rule (healthy/warning/critical)")
@click.option("--key", default="env", show_default=True, help="Tag key")
@click.option("--value", default="production", show_default=True, help="Tag value")
def cmd_tag_show(pipeline: str, prefix: str, status: str, key: str, value: str) -> None:
    """Show tags applied to a pipeline's latest metric."""
    collector = _get_collector()
    metric = collector.latest(pipeline)
    if metric is None:
        click.echo(f"No data for pipeline: {pipeline}")
        return
    rule = TagRule(key=key, value=value, name_prefix=prefix, status=status)
    tags = apply_tags(metric, [rule])
    if tags:
        for t in tags:
            click.echo(f"  {t}")
    else:
        click.echo("No tags matched.")


@tag.command("group")
@click.option("--key", default="env", show_default=True, help="Tag key to group by")
@click.option("--status", default="critical", show_default=True, help="Tag value for status match")
def cmd_tag_group(key: str, status: str) -> None:
    """Group all latest metrics by a tag key."""
    collector = _get_collector()
    metrics = [collector.latest(p) for p in collector.pipelines()]
    metrics = [m for m in metrics if m is not None]
    rule = TagRule(key=key, value=status, status=status)
    groups = group_by_tag(metrics, [rule], key=key)
    for bucket, items in groups.items():
        click.echo(f"[{bucket}]")
        for m in items:
            click.echo(f"  {m.pipeline_name} ({m.status.value})")


@tag.command("json")
@click.option("--key", default="env", show_default=True)
@click.option("--status", default="critical", show_default=True)
def cmd_tag_json(key: str, status: str) -> None:
    """Output tag groups as JSON."""
    collector = _get_collector()
    metrics = [collector.latest(p) for p in collector.pipelines()]
    metrics = [m for m in metrics if m is not None]
    rule = TagRule(key=key, value=status, status=status)
    groups = group_by_tag(metrics, [rule], key=key)
    out = {bucket: [m.pipeline_name for m in items] for bucket, items in groups.items()}
    click.echo(json.dumps(out, indent=2))
