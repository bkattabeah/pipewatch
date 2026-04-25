"""CLI commands for pipeline signal detection."""
from __future__ import annotations

import json
from typing import Optional

import click

from pipewatch.collector import MetricCollector
from pipewatch.signal import SignalConfig, detect_signals


def _get_collector() -> MetricCollector:
    return MetricCollector()


@click.group()
def signal() -> None:
    """Detect behavioral signals across pipeline metrics."""


@signal.command("show")
@click.option("--spike-threshold", default=0.10, show_default=True, help="Min error-rate delta to flag a spike.")
@click.option("--recovery-threshold", default=0.10, show_default=True, help="Min error-rate drop to flag recovery.")
@click.option("--pipeline", default=None, help="Filter to a single pipeline.")
def cmd_signal_show(
    spike_threshold: float,
    recovery_threshold: float,
    pipeline: Optional[str],
) -> None:
    """Print signal table comparing latest two observations."""
    collector = _get_collector()
    pipelines = [pipeline] if pipeline else collector.pipelines()

    current, previous = [], []
    for p in pipelines:
        hist = collector.history(p)
        if len(hist) >= 2:
            current.append(hist[-1])
            previous.append(hist[-2])

    if not current:
        click.echo("No data available for signal detection.")
        return

    config = SignalConfig(
        min_error_rate_spike=spike_threshold,
        min_recovery_drop=recovery_threshold,
    )
    results = detect_signals(current, previous, config)

    click.echo(f"{'PIPELINE':<30} {'SIGNAL':<12} {'CURRENT':>8} {'PREVIOUS':>9} {'DELTA':>8}")
    click.echo("-" * 72)
    for r in results:
        click.echo(
            f"{r.pipeline:<30} {r.signal:<12} {r.current_error_rate:>7.1%} "
            f"{r.previous_error_rate:>8.1%} {r.delta:>+8.1%}  {r.note}"
        )


@signal.command("json")
@click.option("--spike-threshold", default=0.10, show_default=True)
@click.option("--recovery-threshold", default=0.10, show_default=True)
def cmd_signal_json(spike_threshold: float, recovery_threshold: float) -> None:
    """Emit signal results as JSON."""
    collector = _get_collector()
    current, previous = [], []
    for p in collector.pipelines():
        hist = collector.history(p)
        if len(hist) >= 2:
            current.append(hist[-1])
            previous.append(hist[-2])

    config = SignalConfig(
        min_error_rate_spike=spike_threshold,
        min_recovery_drop=recovery_threshold,
    )
    results = detect_signals(current, previous, config)
    click.echo(json.dumps([r.to_dict() for r in results], indent=2))
