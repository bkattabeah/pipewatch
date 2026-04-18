"""Extended CLI entry point that wires baseline commands into the main CLI."""

from __future__ import annotations

import click

from pipewatch.cli import main as _base_main
from pipewatch.cli_baseline import baseline


@click.group()
def extended_main():
    """Pipewatch extended CLI with baseline support."""


extended_main.add_command(baseline)


if __name__ == "__main__":
    extended_main()
