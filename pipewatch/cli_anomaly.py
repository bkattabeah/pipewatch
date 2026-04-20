"""CLI commands for anomaly detection."""
from __future__ import annotations

import json
import click

from pipewatch.anomaly import AnomalyConfig, detect_anomalies
from pipewatch.collector import MetricCollector


def _get_collector() -> MetricCollector:
    return MetricCollector()


@click.group()
def anomaly() -> None:
    """Anomaly detection commands."""


@anomaly.command("show")
@click.option("--threshold", default=2.5, show_default=True, help="Z-score threshold for anomaly detection.")
@click.option("--min-samples", default=5, show_default=True, help="Minimum history samples required.")
def cmd_anomaly_show(threshold: float, min_samples: int) -> None:
    """Print anomaly detection results to console."""
    config = AnomalyConfig(z_score_threshold=threshold, min_samples=min_samples)
    collector = _get_collector()
    history_map = {name: collector.history(name) for name in collector.pipelines()}
    results = detect_anomalies(history_map, config)

    if not results:
        click.echo("No anomaly results (insufficient data or no pipelines).")
        return

    for r in results:
        flag = " [ANOMALY]" if r.is_anomaly else ""
        click.echo(
            f"{r.pipeline}: error_rate={r.current_error_rate:.4f}  "
            f"z={r.z_score:.2f}  mean={r.mean:.4f}  std={r.std_dev:.4f}{flag}"
        )


@anomaly.command("json")
@click.option("--threshold", default=2.5, show_default=True)
@click.option("--min-samples", default=5, show_default=True)
@click.option("--only-anomalies", is_flag=True, default=False, help="Only output anomalous pipelines.")
def cmd_anomaly_json(threshold: float, min_samples: int, only_anomalies: bool) -> None:
    """Output anomaly detection results as JSON."""
    config = AnomalyConfig(z_score_threshold=threshold, min_samples=min_samples)
    collector = _get_collector()
    history_map = {name: collector.history(name) for name in collector.pipelines()}
    results = detect_anomalies(history_map, config)

    if only_anomalies:
        results = [r for r in results if r.is_anomaly]

    click.echo(json.dumps([r.to_dict() for r in results], indent=2))
