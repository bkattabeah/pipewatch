"""Entry-point extension that registers the histogram sub-command group."""
from __future__ import annotations

import click

from pipewatch.cli import main
from pipewatch.cli_histogram import histogram


def extended_main_histogram() -> None:
    """Attach the histogram command group to the root CLI and invoke it."""
    main.add_command(histogram)
    main(standalone_mode=False)


if __name__ == "__main__":
    extended_main_histogram()
