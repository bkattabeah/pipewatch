"""Extended CLI entry point including checkpoint commands."""
import click
from pipewatch.cli import main
from pipewatch.cli_checkpoint import checkpoint


@click.group()
def extended_main_checkpoint():
    """Pipewatch CLI with checkpoint support."""


for cmd in main.commands.values():
    extended_main_checkpoint.add_command(cmd)

extended_main_checkpoint.add_command(checkpoint)

if __name__ == "__main__":
    extended)
