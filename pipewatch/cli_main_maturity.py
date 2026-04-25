"""Extended main entry point that includes maturity commands."""

from __future__ import annotations

import click

from pipewatch.cli import main
from pipewatch.cli_maturity import maturity


@click.group()
def extended_main_maturity() -> None:
    """Pipewatch CLI with maturity scoring support."""


extended_main_maturity.add_command(maturity)

# Attach all commands from the base main group
for name, cmd in main.commands.items():  # type: ignore[attr-defined]
    extended_main_maturity.add_command(cmd, name=name)


if __name__ == "__main__":
    extended_main_maturity()
