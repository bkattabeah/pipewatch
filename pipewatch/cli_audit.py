"""CLI commands for viewing the audit log."""
import click
import json
from pipewatch.audit import AuditLog

_audit_log: AuditLog = AuditLog()


def get_audit_log() -> AuditLog:
    return _audit_log


@click.group()
def audit():
    """View and manage the audit log."""
    pass


@audit.command("show")
@click.option("--pipeline", default=None, help="Filter by pipeline name")
@click.option("--type", "event_type", default=None, help="Filter by event type")
@click.option("--limit", default=20, show_default=True, help="Max events to show")
def cmd_audit_show(pipeline, event_type, limit):
    """Display recent audit events."""
    log = get_audit_log()
    events = log.all()
    if pipeline:
        events = [e for e in events if e.pipeline == pipeline]
    if event_type:
        events = [e for e in events if e.event_type == event_type]
    events = events[-limit:]
    if not events:
        click.echo("No audit events found.")
        return
    for e in events:
        click.echo(f"[{e.timestamp.strftime('%Y-%m-%dT%H:%M:%S')}] "
                   f"{e.event_type.upper():15s} {e.pipeline:20s} "
                   f"({e.severity}) {e.message}")


@audit.command("json")
@click.option("--pipeline", default=None)
@click.option("--limit", default=50, show_default=True)
def cmd_audit_json(pipeline, limit):
    """Dump audit log as JSON."""
    log = get_audit_log()
    events = log.for_pipeline(pipeline) if pipeline else log.all()
    events = events[-limit:]
    click.echo(json.dumps([e.to_dict() for e in events], indent=2))


@audit.command("clear")
def cmd_audit_clear():
    """Clear all audit events."""
    get_audit_log().clear()
    click.echo("Audit log cleared.")
