"""Extended CLI entry point that wires baseline commands into the main CLI."""

from __future__ import annotations

import click

from pipewatch.cli import main as _base_main
from pipewatch.cli_baseline import baseline


@click.group()
def extended_main():
    """Pipewatch extended CLI with baseline support.

    This CLI extends the base pipewatch commands with additional baseline
    functionality for tracking and comparing pipeline metrics over time.
    """


extended_main.add_command(baseline)

# Re-export commands from the base CLI so users have a single entry point.
for _cmd in _base_main.commands.values():
    extended_main.add_command(_cmd)


if __name__ == "__main__":
    extended_main()
