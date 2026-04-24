"""Extended main entry point including incident commands."""
from __future__ import annotations

import click

from pipewatch.cli import main
from pipewatch.cli_incident import incident


@click.group()
def extended_main_incident() -> None:
    """Pipewatch CLI with incident management."""


extended_main_incident.add_command(incident)

for cmd in main.commands.values():  # type: ignore[attr-defined]
    extended_main_incident.add_command(cmd)


if __name__ == "__main__":
    extended_main_incident()
