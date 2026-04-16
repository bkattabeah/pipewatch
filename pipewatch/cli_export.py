"""CLI sub-commands for exporting pipeline data."""

from __future__ import annotations

import sys
from typing import Optional

import click

from pipewatch.cli import build_default_engine
from pipewatch.export import export_csv, export_json, export_text
from pipewatch.reporter import Reporter


@click.group()
def export():
    """Export pipeline reports in various formats."""


def _collect_reports(pipeline: Optional[str]):
    engine = build_default_engine()
    reporter = Reporter(engine.collector, engine.threshold_registry)
    pipelines = [pipeline] if pipeline else engine.collector.pipelines()
    return [reporter.report(p) for p in pipelines]


@export.command("json")
@click.option("--pipeline", default=None, help="Filter to a single pipeline.")
@click.option("--indent", default=2, show_default=True, help="JSON indent level.")
@click.option("--output", type=click.Path(), default=None, help="Write to file instead of stdout.")
def cmd_export_json(pipeline, indent, output):
    """Export reports as JSON."""
    reports = _collect_reports(pipeline)
    content = export_json(reports, indent=indent)
    if output:
        with open(output, "w") as f:
            f.write(content)
        click.echo(f"Exported JSON to {output}")
    else:
        click.echo(content)


@export.command("csv")
@click.option("--pipeline", default=None, help="Filter to a single pipeline.")
@click.option("--output", type=click.Path(), default=None, help="Write to file instead of stdout.")
def cmd_export_csv(pipeline, output):
    """Export reports as CSV."""
    reports = _collect_reports(pipeline)
    content = export_csv(reports)
    if output:
        with open(output, "w", newline="") as f:
            f.write(content)
        click.echo(f"Exported CSV to {output}")
    else:
        click.echo(content, nl=False)


@export.command("text")
@click.option("--pipeline", default=None, help="Filter to a single pipeline.")
def cmd_export_text(pipeline):
    """Export reports as a formatted text table."""
    reports = _collect_reports(pipeline)
    click.echo(export_text(reports), nl=False)


if __name__ == "__main__":
    export()
