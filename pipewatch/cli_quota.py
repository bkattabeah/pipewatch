import click
import json
from pipewatch.quota import AlertQuotaManager, QuotaConfig

_manager: AlertQuotaManager = AlertQuotaManager()


def get_quota_manager() -> AlertQuotaManager:
    return _manager


@click.group()
def quota() -> None:
    """Manage per-pipeline alert quotas."""


@quota.command("status")
def cmd_status() -> None:
    """Show current quota usage."""
    mgr = get_quota_manager()
    s = mgr.status()
    click.echo(f"Total alerts this hour : {s['total']}")
    click.echo(f"Window start           : {s['window_start']}")
    if s["entries"]:
        click.echo("\nPer-pipeline breakdown:")
        for e in s["entries"]:
            click.echo(f"  {e['pipeline']} [{e['severity']}]: {e['count']}")
    else:
        click.echo("No quota entries recorded yet.")


@quota.command("json")
def cmd_json() -> None:
    """Dump quota status as JSON."""
    mgr = get_quota_manager()
    click.echo(json.dumps(mgr.status(), indent=2))


@quota.command("clear")
def cmd_clear() -> None:
    """Reset all quota counters."""
    get_quota_manager().clear()
    click.echo("Quota counters cleared.")
