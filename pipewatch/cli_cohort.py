"""CLI commands for cohort analysis."""
from __future__ import annotations

import json
import sys

import click

from pipewatch.cohort import CohortConfig, build_cohort
from pipewatch.collector import MetricCollector


def _get_collector() -> MetricCollector:
    return MetricCollector()


@click.group()
def cohort() -> None:
    """Cohort analysis commands."""


@cohort.command("show")
@click.option("--pipeline", default=None, help="Filter to a specific pipeline.")
@click.option("--bucket-minutes", default=60, show_default=True, help="Bucket size in minutes.")
@click.option("--min-size", default=1, show_default=True, help="Minimum cohort size to display.")
def cmd_cohort_show(
    pipeline: str | None,
    bucket_minutes: int,
    min_size: int,
) -> None:
    """Show cohort error rate buckets in a table."""
    collector = _get_collector()
    names = [pipeline] if pipeline else collector.pipelines()
    all_metrics = [m for name in names for m in collector.history(name)]

    if not all_metrics:
        click.echo("No metric data available.")
        sys.exit(0)

    try:
        config = CohortConfig(bucket_minutes=bucket_minutes, min_cohort_size=min_size)
        result = build_cohort(all_metrics, config)
    except ValueError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)

    if not result.buckets:
        click.echo("No cohorts meet the minimum size requirement.")
        sys.exit(0)

    click.echo(f"{'Bucket':<22} {'Count':>6} {'Avg Error Rate':>16}")
    click.echo("-" * 48)
    for b in result.buckets:
        click.echo(f"{b.label:<22} {b.count:>6} {b.avg_error_rate:>15.2%}")

    peak = result.peak_bucket()
    if peak:
        click.echo(f"\nPeak bucket: {peak.label} ({peak.avg_error_rate:.2%} avg error rate)")


@cohort.command("json")
@click.option("--pipeline", default=None, help="Filter to a specific pipeline.")
@click.option("--bucket-minutes", default=60, show_default=True)
@click.option("--min-size", default=1, show_default=True)
def cmd_cohort_json(
    pipeline: str | None,
    bucket_minutes: int,
    min_size: int,
) -> None:
    """Emit cohort analysis as JSON."""
    collector = _get_collector()
    names = [pipeline] if pipeline else collector.pipelines()
    all_metrics = [m for name in names for m in collector.history(name)]

    try:
        config = CohortConfig(bucket_minutes=bucket_minutes, min_cohort_size=min_size)
        result = build_cohort(all_metrics, config)
    except ValueError as exc:
        click.echo(json.dumps({"error": str(exc)}), err=True)
        sys.exit(1)

    click.echo(json.dumps(result.to_dict(), indent=2))
