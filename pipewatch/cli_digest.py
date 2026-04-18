"""CLI commands for pipeline health digests."""
import json
import click

from pipewatch.collector import MetricCollector
from pipewatch.digest import DigestConfig, build_digest


def _get_collector() -> MetricCollector:
    return MetricCollector()


@click.group()
def digest():
    """Generate pipeline health digest summaries."""


@digest.command("show")
@click.option("--include-healthy", is_flag=True, default=False)
@click.option("--top", default=5, show_default=True, help="Worst N pipelines to show.")
@click.option("--title", default="Pipeline Health Digest", show_default=True)
def cmd_digest_show(include_healthy: bool, top: int, title: str):
    """Print a human-readable digest to stdout."""
    collector = _get_collector()
    metrics = collector.latest_all()
    config = DigestConfig(title=title, include_healthy=include_healthy, top_n_worst=top)
    d = build_digest(metrics, config)

    click.echo(f"=== {d.title} ===")
    click.echo(f"Generated: {d.generated_at}")
    s = d.stats
    click.echo(f"Total: {s.total}  Healthy: {s.healthy}  Warning: {s.warning}  Critical: {s.critical}")
    if d.entries:
        click.echo("\nTop issues:")
        for e in d.entries:
            click.echo(f"  [{e.status.upper()}] {e.pipeline_id}  error_rate={e.error_rate:.2%}")
    else:
        click.echo("\nNo issues found.")


@digest.command("json")
@click.option("--include-healthy", is_flag=True, default=False)
@click.option("--top", default=5, show_default=True)
def cmd_digest_json(include_healthy: bool, top: int):
    """Output digest as JSON."""
    collector = _get_collector()
    metrics = collector.latest_all()
    config = DigestConfig(include_healthy=include_healthy, top_n_worst=top)
    d = build_digest(metrics, config)
    click.echo(json.dumps(d.to_dict(), indent=2))
