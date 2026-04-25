"""Summarise notification history for reporting and diagnostics."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from pipewatch.notifier import NotificationRecord


@dataclass
class ChannelSummary:
    channel: str
    total: int = 0
    successes: int = 0
    failures: int = 0
    pipelines: List[str] = field(default_factory=list)

    @property
    def failure_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return self.failures / self.total

    def to_dict(self) -> Dict:
        return {
            "channel": self.channel,
            "total": self.total,
            "successes": self.successes,
            "failures": self.failures,
            "failure_rate": round(self.failure_rate, 4),
            "pipelines": sorted(set(self.pipelines)),
        }


@dataclass
class NotificationSummary:
    channels: List[ChannelSummary] = field(default_factory=list)

    @property
    def total_sent(self) -> int:
        return sum(c.total for c in self.channels)

    @property
    def total_failures(self) -> int:
        return sum(c.failures for c in self.channels)

    def to_dict(self) -> Dict:
        return {
            "total_sent": self.total_sent,
            "total_failures": self.total_failures,
            "channels": [c.to_dict() for c in self.channels],
        }


def build_notification_summary(records: List[NotificationRecord]) -> NotificationSummary:
    """Aggregate notification records into per-channel summaries."""
    channel_map: Dict[str, ChannelSummary] = {}
    for rec in records:
        if rec.channel not in channel_map:
            channel_map[rec.channel] = ChannelSummary(channel=rec.channel)
        summary = channel_map[rec.channel]
        summary.total += 1
        if rec.success:
            summary.successes += 1
        else:
            summary.failures += 1
        summary.pipelines.append(rec.pipeline)
    return NotificationSummary(channels=list(channel_map.values()))


def format_notification_summary(summary: NotificationSummary) -> str:
    lines = [f"Notifications: {summary.total_sent} sent, {summary.total_failures} failed"]
    for ch in summary.channels:
        lines.append(
            f"  [{ch.channel}] total={ch.total} ok={ch.successes} fail={ch.failures} "
            f"rate={ch.failure_rate:.1%} pipelines={len(set(ch.pipelines))}"
        )
    return "\n".join(lines)
