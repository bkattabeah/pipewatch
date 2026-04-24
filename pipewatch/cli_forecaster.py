"""CLI commands for pipeline error-rate forecasting."""
from __future__ import annotations

import json
import click

from pipewatch.collector import MetricCollector
from pipewatch.forecaster import ForecastConfig, forecast


def _get_collector() -> MetricCollector:
    return MetricCollector()


@click.group()
def forecaster() -> None:
    """Forecast future error rates for monitored pipelines."""


@forecaster.command("show")
@click.argument("pipeline")
@click.option("--horizon", default=5, show_default=True, help="Ticks to forecast ahead.")
@click.option("--min-samples", default=3, show_default=True, help="Minimum history required.")
def cmd_forecast_show(pipeline: str, horizon: int, min_samples: int) -> None:
    """Print a human-readable forecast for PIPELINE."""
    collector = _get_collector()
    config = ForecastConfig(horizon=horizon, min_samples=min_samples)
    history = collector.history(pipeline)
    result = forecast(pipeline, history, config)

    if result.insufficient_data:
        click.echo(f"[{pipeline}] Insufficient data for forecast (need >= {min_samples} samples).")
        return

    click.echo(f"[{pipeline}] slope={result.slope:+.4f}")
    for pt in result.points:
        bar = "#" * int(pt.predicted_error_rate * 20)
        click.echo(f"  t+{pt.tick:>3}  error_rate={pt.predicted_error_rate:.4f}  {bar}")


@forecaster.command("json")
@click.argument("pipeline")
@click.option("--horizon", default=5, show_default=True)
@click.option("--min-samples", default=3, show_default=True)
def cmd_forecast_json(pipeline: str, horizon: int, min_samples: int) -> None:
    """Emit forecast for PIPELINE as JSON."""
    collector = _get_collector()
    config = ForecastConfig(horizon=horizon, min_samples=min_samples)
    history = collector.history(pipeline)
    result = forecast(pipeline, history, config)
    click.echo(json.dumps(result.to_dict(), indent=2))


if __name__ == "__main__":
    forecaster()
