"""Extended main entry point including digest commands."""
import click

from pipewatch.cli import main
from pipewatch.cli_digest import digest


@click.group()
def extended_main_digest():
    """Pipewatch — ETL pipeline health monitor (with digest support)."""


for cmd in [digest]:
    extended_main_digest.add_command(cmd)


if __name__ == "__main__":
    extended_main_digest()
