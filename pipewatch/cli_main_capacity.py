"""Entry-point extension that adds capacity commands to the main CLI."""
from __future__ import annotations

import click

from pipewatch.cli import main
from pipewatch.cli_capacity import capacity


def extended_main_capacity() -> None:
    """Main entry point with capacity sub-commands attached."""
    main.add_command(capacity)
    main(standalone_mode=True)


if __name__ == "__main__":  # pragma: no cover
    extended_main_capacity()
