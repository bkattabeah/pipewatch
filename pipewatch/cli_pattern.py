"""CLI commands for pattern detection."""

from __future__ import annotations

import json
import click

from pipewatch.collector import MetricCollector
from pipewatch.pattern import PatternConfig, detect_pattern


def _get_collector() -> MetricCollector:
    return MetricCollector()


@click.group()
def pattern() -> None:
    """Detect recurring failure patterns in pipelines."""


@pattern.command("show")
@click.argument("pipeline")
@click.option("--min-occurrences", default=3, show_default=True, help="Min failures to flag a pattern.")
@click.option("--window", default=20, show_default=True, help="Number of recent metrics to inspect.")
@click.option("--threshold", default=0.1, show_default=True, help="Error rate threshold.")
def cmd_pattern_show(
    pipeline: str,
    min_occurrences: int,
    window: int,
    threshold: float,
) -> None:
    """Show pattern detection result for a pipeline."""
    collector = _get_collector()
    metrics = collector.history(pipeline)
    if not metrics:
        click.echo(f"No data for pipeline: {pipeline}")
        return

    try:
        cfg = PatternConfig(
            min_occurrences=min_occurrences,
            window_size=window,
            error_rate_threshold=threshold,
        )
        cfg.validate()
    except ValueError as exc:
        raise click.BadParameter(str(exc))

    result = detect_pattern(pipeline, metrics, cfg)
    if result is None:
        click.echo("No pattern data available.")
        return

    flag = "YES" if result.has_pattern else "no"
    click.echo(f"Pipeline : {result.pipeline}")
    click.echo(f"Pattern  : {flag}")
    for m in result.matches:
        click.echo(
            f"  occurrences={m.occurrences}  avg_error_rate={m.avg_error_rate:.2%}  "
            f"recurring={m.is_recurring}"
        )


@pattern.command("json")
@click.argument("pipeline")
@click.option("--min-occurrences", default=3)
@click.option("--window", default=20)
@click.option("--threshold", default=0.1)
def cmd_pattern_json(
    pipeline: str,
    min_occurrences: int,
    window: int,
    threshold: float,
) -> None:
    """Output pattern detection result as JSON."""
    collector = _get_collector()
    metrics = collector.history(pipeline)
    cfg = PatternConfig(
        min_occurrences=min_occurrences,
        window_size=window,
        error_rate_threshold=threshold,
    )
    result = detect_pattern(pipeline, metrics, cfg)
    payload = result.to_dict() if result else {"pipeline": pipeline, "has_pattern": False, "matches": []}
    click.echo(json.dumps(payload, indent=2))
