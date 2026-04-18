"""CLI commands for inspecting alert rate limiter state."""
import json
import click
from pipewatch.rate_limiter import AlertRateLimiter, RateLimiterConfig

_limiter: AlertRateLimiter = AlertRateLimiter()


def get_limiter() -> AlertRateLimiter:
    return _limiter


@click.group()
def rate_limiter() -> None:
    """Manage alert rate limiting."""


@rate_limiter.command("status")
def cmd_status() -> None:
    """Show current rate limit counters."""
    limiter = get_limiter()
    entries = limiter.status()
    if not entries:
        click.echo("No rate limit entries recorded.")
        return
    for e in entries:
        click.echo(
            f"{e['pipeline']} / {e['rule_name']}: "
            f"{e['count']} alerts since {e['window_start']}"
        )


@rate_limiter.command("json")
def cmd_json() -> None:
    """Dump rate limit state as JSON."""
    limiter = get_limiter()
    click.echo(json.dumps(limiter.status(), indent=2))


@rate_limiter.command("clear")
@click.option("--pipeline", default=None, help="Pipeline name")
@click.option("--rule", default=None, help="Rule name")
def cmd_clear(pipeline: str, rule: str) -> None:
    """Clear rate limit state (all or specific entry)."""
    limiter = get_limiter()
    if pipeline and rule:
        limiter.reset(pipeline, rule)
        click.echo(f"Cleared rate limit for {pipeline} / {rule}.")
    else:
        limiter.reset_all()
        click.echo("All rate limit entries cleared.")
