"""CLI commands for alert budget management."""

from __future__ import annotations

import json
import click

from pipewatch.budget import AlertBudget, BudgetConfig

_budget: AlertBudget | None = None


def get_budget() -> AlertBudget:
    global _budget
    if _budget is None:
        _budget = AlertBudget()
    return _budget


@click.group()
def budget() -> None:
    """Manage alert budget."""


@budget.command("status")
@click.option("--window", default=3600, show_default=True, help="Window in seconds.")
@click.option("--max-alerts", default=100, show_default=True, help="Max alerts per window.")
def cmd_status(window: int, max_alerts: int) -> None:
    """Show current budget usage."""
    b = AlertBudget(BudgetConfig(window_seconds=window, max_alerts=max_alerts))
    s = b.status()
    click.echo(f"Budget: {s.used}/{s.limit} alerts used in last {s.window_seconds}s")
    click.echo(f"Remaining : {s.remaining}")
    click.echo(f"Exhausted : {s.exhausted}")


@budget.command("json")
def cmd_json() -> None:
    """Dump budget status as JSON."""
    b = get_budget()
    click.echo(json.dumps(b.status().to_dict(), indent=2))


@budget.command("clear")
def cmd_clear() -> None:
    """Clear recorded budget entries."""
    get_budget().clear()
    click.echo("Budget cleared.")
