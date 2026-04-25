"""Entry point extension that adds the ranking command group."""
from __future__ import annotations

import click

from pipewatch.cli import main
from pipewatch.cli_ranking import ranking


@click.group()
def extended_main_ranking() -> None:
    """Pipewatch CLI — extended with ranking commands."""


extended_main_ranking.add_command(ranking)

# Allow standalone execution
for _cmd in main.commands.values():
    extended_main_ranking.add_command(_cmd)


if __name__ == "__main__":
    extended_main_ranking()
