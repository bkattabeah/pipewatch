"""Alert throttling: limits how frequently the same alert can fire within a rolling window."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List

from pipewatch.alerts import Alert


@dataclass
class ThrottlerConfig:
    window_seconds: int = 60
    max_firings: int = 3

    def validate(self) -> None:
        if self.window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        if self.max_firings <= 0:
            raise ValueError("max_firings must be positive")


@dataclass
class ThrottleEntry:
    pipeline: str
    rule_name: str
    firings: List[datetime] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "rule_name": self.rule_name,
            "firings": [ts.isoformat() for ts in self.firings],
        }


class AlertThrottler:
    """Tracks recent alert firings and suppresses those that exceed the configured rate."""

    def __init__(self, config: ThrottlerConfig | None = None) -> None:
        self.config = config or ThrottlerConfig()
        self.config.validate()
        self._entries: Dict[str, ThrottleEntry] = {}

    def _key(self, alert: Alert) -> str:
        return f"{alert.pipeline}::{alert.rule_name}"

    def _prune(self, entry: ThrottleEntry, now: datetime) -> None:
        cutoff = now - timedelta(seconds=self.config.window_seconds)
        entry.firings = [ts for ts in entry.firings if ts >= cutoff]

    def is_throttled(self, alert: Alert, now: datetime | None = None) -> bool:
        """Return True if the alert should be suppressed due to throttling."""
        now = now or datetime.utcnow()
        key = self._key(alert)
        entry = self._entries.get(key)
        if entry is None:
            return False
        self._prune(entry, now)
        return len(entry.firings) >= self.config.max_firings

    def record(self, alert: Alert, now: datetime | None = None) -> None:
        """Record a firing for the given alert."""
        now = now or datetime.utcnow()
        key = self._key(alert)
        if key not in self._entries:
            self._entries[key] = ThrottleEntry(pipeline=alert.pipeline, rule_name=alert.rule_name)
        entry = self._entries[key]
        self._prune(entry, now)
        entry.firings.append(now)

    def status(self) -> List[dict]:
        return [e.to_dict() for e in self._entries.values()]

    def clear(self) -> None:
        self._entries.clear()
