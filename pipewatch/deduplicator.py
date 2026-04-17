"""Deduplication of pipeline alerts to suppress repeated firing."""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional

from pipewatch.alerts import Alert


@dataclass
class DedupeEntry:
    alert: Alert
    first_seen: datetime
    last_seen: datetime
    count: int = 1

    def to_dict(self) -> dict:
        return {
            "pipeline": self.alert.pipeline,
            "level": self.alert.level,
            "message": self.alert.message,
            "first_seen": self.first_seen.isoformat(),
            "last_seen": self.last_seen.isoformat(),
            "count": self.count,
        }


@dataclass
class DeduplicatorConfig:
    window_seconds: int = 60


class AlertDeduplicator:
    """Suppress duplicate alerts fired within a configurable time window."""

    def __init__(self, config: Optional[DeduplicatorConfig] = None):
        self.config = config or DeduplicatorConfig()
        self._seen: Dict[str, DedupeEntry] = {}

    def _key(self, alert: Alert) -> str:
        return f"{alert.pipeline}:{alert.level}:{alert.message}"

    def is_duplicate(self, alert: Alert, now: Optional[datetime] = None) -> bool:
        now = now or datetime.utcnow()
        key = self._key(alert)
        if key not in self._seen:
            return False
        entry = self._seen[key]
        window = timedelta(seconds=self.config.window_seconds)
        return (now - entry.last_seen) < window

    def record(self, alert: Alert, now: Optional[datetime] = None) -> DedupeEntry:
        now = now or datetime.utcnow()
        key = self._key(alert)
        if key in self._seen:
            entry = self._seen[key]
            entry.last_seen = now
            entry.count += 1
        else:
            entry = DedupeEntry(alert=alert, first_seen=now, last_seen=now)
            self._seen[key] = entry
        return entry

    def filter(self, alerts: list, now: Optional[datetime] = None) -> list:
        """Return only non-duplicate alerts, recording all seen."""
        now = now or datetime.utcnow()
        result = []
        for alert in alerts:
            if not self.is_duplicate(alert, now):
                result.append(alert)
            self.record(alert, now)
        return result

    def entries(self) -> list:
        return list(self._seen.values())

    def clear(self) -> None:
        self._seen.clear()
