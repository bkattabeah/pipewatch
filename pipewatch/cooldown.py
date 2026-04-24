"""Alert cooldown manager: suppresses repeat alerts for a pipeline within a cooldown window."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pipewatch.alerts import Alert


@dataclass
class CooldownConfig:
    window_seconds: int = 300  # 5 minutes default
    max_suppressed: int = 100

    def validate(self) -> None:
        if self.window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        if self.max_suppressed <= 0:
            raise ValueError("max_suppressed must be positive")


@dataclass
class CooldownEntry:
    pipeline: str
    last_fired: datetime
    suppressed_count: int = 0

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "last_fired": self.last_fired.isoformat(),
            "suppressed_count": self.suppressed_count,
        }


class AlertCooldownManager:
    def __init__(self, config: Optional[CooldownConfig] = None) -> None:
        self.config = config or CooldownConfig()
        self.config.validate()
        self._entries: Dict[str, CooldownEntry] = {}

    def _key(self, alert: Alert) -> str:
        return f"{alert.pipeline}:{alert.rule.name}"

    def is_suppressed(self, alert: Alert, now: Optional[datetime] = None) -> bool:
        now = now or datetime.utcnow()
        key = self._key(alert)
        entry = self._entries.get(key)
        if entry is None:
            return False
        window = timedelta(seconds=self.config.window_seconds)
        return (now - entry.last_fired) < window

    def record(self, alert: Alert, now: Optional[datetime] = None) -> None:
        now = now or datetime.utcnow()
        key = self._key(alert)
        entry = self._entries.get(key)
        if entry is None:
            self._entries[key] = CooldownEntry(pipeline=alert.pipeline, last_fired=now)
        else:
            if self.is_suppressed(alert, now):
                entry.suppressed_count = min(
                    entry.suppressed_count + 1, self.config.max_suppressed
                )
            else:
                entry.last_fired = now
                entry.suppressed_count = 0

    def allow(self, alert: Alert, now: Optional[datetime] = None) -> bool:
        """Return True if alert should be dispatched (not in cooldown)."""
        now = now or datetime.utcnow()
        suppressed = self.is_suppressed(alert, now)
        self.record(alert, now)
        return not suppressed

    def status(self) -> List[dict]:
        return [e.to_dict() for e in self._entries.values()]

    def clear(self, pipeline: Optional[str] = None) -> None:
        if pipeline is None:
            self._entries.clear()
        else:
            keys = [k for k in self._entries if self._entries[k].pipeline == pipeline]
            for k in keys:
                del self._entries[k]
