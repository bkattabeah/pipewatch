"""Extended main entry-point that includes budget commands."""

from __future__ import annotations

import click

from pipewatch.cli import main
from pipewatch.cli_budget import budget


@click.group()
def extended_main_budget() -> None:
    """Pipewatch — with alert budget support."""


extended_main_budget.add_command(main, name="core")
extended_main_budget.add_command(budget, name="budget")

if __name__ == "__main__":
    extended_main_budget()
