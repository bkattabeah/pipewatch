"""Notification channel abstraction for delivering alerts to external services."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional
from datetime import datetime, timezone

from pipewatch.alerts import Alert


@dataclass
class NotificationConfig:
    channel: str
    enabled: bool = True
    min_severity: str = "warning"  # warning | critical
    tags: List[str] = field(default_factory=list)

    def validate(self) -> None:
        valid_severities = {"warning", "critical"}
        if self.min_severity not in valid_severities:
            raise ValueError(f"min_severity must be one of {valid_severities}")
        if not self.channel.strip():
            raise ValueError("channel must not be empty")


@dataclass
class NotificationRecord:
    channel: str
    alert_id: str
    pipeline: str
    severity: str
    sent_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    success: bool = True
    error: Optional[str] = None

    def to_dict(self) -> Dict:
        return {
            "channel": self.channel,
            "alert_id": self.alert_id,
            "pipeline": self.pipeline,
            "severity": self.severity,
            "sent_at": self.sent_at.isoformat(),
            "success": self.success,
            "error": self.error,
        }


HandlerFn = Callable[[Alert], None]


class Notifier:
    """Routes alerts to registered notification channels."""

    def __init__(self) -> None:
        self._channels: Dict[str, tuple[NotificationConfig, HandlerFn]] = {}
        self._history: List[NotificationRecord] = []

    def register(self, config: NotificationConfig, handler: HandlerFn) -> None:
        config.validate()
        self._channels[config.channel] = (config, handler)

    def notify(self, alert: Alert) -> List[NotificationRecord]:
        records: List[NotificationRecord] = []
        for channel, (cfg, fn) in self._channels.items():
            if not cfg.enabled:
                continue
            if cfg.min_severity == "critical" and alert.rule.severity != "critical":
                continue
            rec = self._dispatch(channel, cfg, fn, alert)
            records.append(rec)
        self._history.extend(records)
        return records

    def _dispatch(
        self,
        channel: str,
        cfg: NotificationConfig,
        fn: HandlerFn,
        alert: Alert,
    ) -> NotificationRecord:
        try:
            fn(alert)
            return NotificationRecord(
                channel=channel,
                alert_id=f"{alert.pipeline}:{alert.rule.name}",
                pipeline=alert.pipeline,
                severity=alert.rule.severity,
            )
        except Exception as exc:  # noqa: BLE001
            return NotificationRecord(
                channel=channel,
                alert_id=f"{alert.pipeline}:{alert.rule.name}",
                pipeline=alert.pipeline,
                severity=alert.rule.severity,
                success=False,
                error=str(exc),
            )

    def history(self, channel: Optional[str] = None) -> List[NotificationRecord]:
        if channel is None:
            return list(self._history)
        return [r for r in self._history if r.channel == channel]

    def clear_history(self) -> None:
        self._history.clear()
