"""CLI commands for filtered metric views."""
import click
from pipewatch.filter import FilterCriteria, filter_metrics
from pipewatch.metrics import PipelineStatus
from pipewatch.cli import build_default_engine


@click.group()
def filter_cmd():
    """Filter and query pipeline metrics."""
    pass


@filter_cmd.command("show")
@click.option("--status", type=click.Choice([s.value for s in PipelineStatus]), default=None)
@click.option("--min-error-rate", type=float, default=None)
@click.option("--max-error-rate", type=float, default=None)
@click.option("--name", default=None, help="Filter by pipeline name substring")
def cmd_filter_show(status, min_error_rate, max_error_rate, name):
    """Show pipelines matching filter criteria."""
    engine = build_default_engine()
    all_metrics = [engine.collector.latest(p) for p in engine.collector.pipelines()]
    all_metrics = [m for m in all_metrics if m is not None]

    criteria = FilterCriteria(
        status=PipelineStatus(status) if status else None,
        min_error_rate=min_error_rate,
        max_error_rate=max_error_rate,
        name_contains=name,
    )
    matched = filter_metrics(all_metrics, criteria)

    if not matched:
        click.echo("No pipelines match the given criteria.")
        return

    for m in matched:
        from pipewatch.metrics import error_rate, evaluate_status
        s = evaluate_status(m)
        r = error_rate(m)
        click.echo(f"[{s.value.upper()}] {m.pipeline_name}  error_rate={r:.2%}")


@filter_cmd.command("unhealthy")
def cmd_filter_unhealthy():
    """Shortcut: show all warning or critical pipelines."""
    engine = build_default_engine()
    all_metrics = [engine.collector.latest(p) for p in engine.collector.pipelines()]
    all_metrics = [m for m in all_metrics if m is not None]

    from pipewatch.metrics import evaluate_status, error_rate, PipelineStatus
    unhealthy = [m for m in all_metrics if evaluate_status(m) in (PipelineStatus.WARNING, PipelineStatus.CRITICAL)]

    if not unhealthy:
        click.echo("All pipelines are healthy.")
        return

    for m in unhealthy:
        s = evaluate_status(m)
        r = error_rate(m)
        click.echo(f"[{s.value.upper()}] {m.pipeline_name}  error_rate={r:.2%}")
