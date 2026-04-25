"""CLI commands for inspecting notifier history and status."""

from __future__ import annotations

import json
import argparse
from typing import Optional

from pipewatch.notifier import Notifier, NotificationConfig
from pipewatch.handlers import console_handler


def _get_notifier() -> Notifier:
    """Build a default notifier with a console channel for demo purposes."""
    notifier = Notifier()
    cfg = NotificationConfig(channel="console", enabled=True, min_severity="warning")
    notifier.register(cfg, console_handler)
    return notifier


def cmd_notifier_show(args: argparse.Namespace) -> None:
    notifier = _get_notifier()
    channel: Optional[str] = getattr(args, "channel", None)
    records = notifier.history(channel=channel)
    if not records:
        print("No notification records found.")
        return
    for rec in records:
        status = "OK" if rec.success else f"FAIL({rec.error})"
        print(f"[{rec.sent_at.strftime('%H:%M:%S')}] {rec.channel} | {rec.pipeline} | {rec.severity} | {status}")


def cmd_notifier_json(args: argparse.Namespace) -> None:
    notifier = _get_notifier()
    channel: Optional[str] = getattr(args, "channel", None)
    records = notifier.history(channel=channel)
    print(json.dumps([r.to_dict() for r in records], indent=2))


def cmd_notifier_clear(args: argparse.Namespace) -> None:  # noqa: ARG001
    notifier = _get_notifier()
    notifier.clear_history()
    print("Notification history cleared.")


def notifier_cmd(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("notifier", help="Notification channel commands")
    sub = p.add_subparsers(dest="notifier_cmd")

    show_p = sub.add_parser("show", help="Show notification history")
    show_p.add_argument("--channel", default=None, help="Filter by channel name")
    show_p.set_defaults(func=cmd_notifier_show)

    json_p = sub.add_parser("json", help="Output notification history as JSON")
    json_p.add_argument("--channel", default=None)
    json_p.set_defaults(func=cmd_notifier_json)

    clear_p = sub.add_parser("clear", help="Clear notification history")
    clear_p.set_defaults(func=cmd_notifier_clear)
