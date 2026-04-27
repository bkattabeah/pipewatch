"""CLI commands for outlier detection."""
from __future__ import annotations

import json
import click

from pipewatch.collector import MetricCollector
from pipewatch.outlier import OutlierConfig, detect_outliers


def _get_collector() -> MetricCollector:
    return MetricCollector()


@click.group("outlier")
def outlier() -> None:
    """Detect outlier pipelines by error rate."""


@outlier.command("show")
@click.option("--min-samples", default=4, show_default=True, help="Minimum samples required.")
@click.option("--multiplier", default=1.5, show_default=True, help="IQR multiplier for fences.")
@click.option("--all", "show_all", is_flag=True, default=False, help="Show non-outliers too.")
def cmd_outlier_show(min_samples: int, multiplier: float, show_all: bool) -> None:
    """Print outlier detection results to console."""
    collector = _get_collector()
    metrics = collector.latest_all()
    if not metrics:
        click.echo("No pipeline data available.")
        return

    try:
        config = OutlierConfig(min_samples=min_samples, iqr_multiplier=multiplier)
    except ValueError as exc:
        click.echo(f"Invalid config: {exc}", err=True)
        raise SystemExit(1)

    results = detect_outliers(metrics, config)
    if not results:
        click.echo(f"Not enough data (need >= {min_samples} pipelines).")
        return

    click.echo(f"{'Pipeline':<30} {'Error Rate':>12} {'Outlier':>8}  Reason")
    click.echo("-" * 70)
    for r in results:
        if not show_all and not r.is_outlier:
            continue
        flag = "YES" if r.is_outlier else "no"
        reason = r.reason or ""
        click.echo(f"{r.pipeline:<30} {r.error_rate:>12.4f} {flag:>8}  {reason}")


@outlier.command("json")
@click.option("--min-samples", default=4, show_default=True)
@click.option("--multiplier", default=1.5, show_default=True)
def cmd_outlier_json(min_samples: int, multiplier: float) -> None:
    """Emit outlier detection results as JSON."""
    collector = _get_collector()
    metrics = collector.latest_all()

    try:
        config = OutlierConfig(min_samples=min_samples, iqr_multiplier=multiplier)
    except ValueError as exc:
        click.echo(json.dumps({"error": str(exc)}), err=True)
        raise SystemExit(1)

    results = detect_outliers(metrics, config)
    click.echo(json.dumps([r.to_dict() for r in results], indent=2))
