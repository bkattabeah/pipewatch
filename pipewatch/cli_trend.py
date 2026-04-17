"""CLI commands for trend analysis."""
import click
import json
from pipewatch.collector import MetricCollector
from pipewatch.trend import analyze_trend


_collector: MetricCollector = MetricCollector()


def _get_collector() -> MetricCollector:
    return _collector


@click.group()
def trend():
    """Trend analysis commands."""


@trend.command("show")
@click.argument("pipeline_id")
@click.option("--limit", default=20, show_default=True, help="Max history samples.")
def cmd_trend_show(pipeline_id: str, limit: int):
    """Show error rate trend for a pipeline."""
    collector = _get_collector()
    metrics = collector.history(pipeline_id, limit=limit)
    result = analyze_trend(pipeline_id, metrics)
    if result is None:
        click.echo(f"No data for pipeline '{pipeline_id}'.")
        return
    click.echo(f"Pipeline:   {result.pipeline_id}")
    click.echo(f"Samples:    {result.sample_count}")
    click.echo(f"Avg error:  {result.avg_error_rate:.2%}")
    click.echo(f"Min error:  {result.min_error_rate:.2%}")
    click.echo(f"Max error:  {result.max_error_rate:.2%}")
    click.echo(f"Trend:      {result.trend_direction} (slope={result.slope:.6f})")


@trend.command("json")
@click.argument("pipeline_id")
@click.option("--limit", default=20, show_default=True)
def cmd_trend_json(pipeline_id: str, limit: int):
    """Output trend analysis as JSON."""
    collector = _get_collector()
    metrics = collector.history(pipeline_id, limit=limit)
    result = analyze_trend(pipeline_id, metrics)
    if result is None:
        click.echo(json.dumps({"error": f"No data for '{pipeline_id}'"}))
        return
    click.echo(json.dumps(result.to_dict(), indent=2))
