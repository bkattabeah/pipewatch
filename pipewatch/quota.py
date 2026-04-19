from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Optional
from datetime import datetime


@dataclass
class QuotaConfig:
    max_critical_per_hour: int = 20
    max_warning_per_hour: int = 50
    max_total_per_hour: int = 100

    def validate(self) -> None:
        for attr in ("max_critical_per_hour", "max_warning_per_hour", "max_total_per_hour"):
            if getattr(self, attr) < 1:
                raise ValueError(f"{attr} must be >= 1")
        if self.max_critical_per_hour > self.max_total_per_hour:
            raise ValueError("max_critical_per_hour cannot exceed max_total_per_hour")
        if self.max_warning_per_hour > self.max_total_per_hour:
            raise ValueError("max_warning_per_hour cannot exceed max_total_per_hour")


@dataclass
class QuotaEntry:
    pipeline: str
    severity: str
    count: int = 0
    window_start: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "severity": self.severity,
            "count": self.count,
            "window_start": self.window_start.isoformat(),
        }


class AlertQuotaManager:
    def __init__(self, config: Optional[QuotaConfig] = None) -> None:
        self.config = config or QuotaConfig()
        self._entries: Dict[str, QuotaEntry] = {}
        self._total: int = 0
        self._window_start: datetime = datetime.utcnow()

    def _key(self, pipeline: str, severity: str) -> str:
        return f"{pipeline}:{severity}"

    def _reset_if_needed(self, now: datetime) -> None:
        elapsed = (now - self._window_start).total_seconds()
        if elapsed >= 3600:
            self._entries.clear()
            self._total = 0
            self._window_start = now

    def is_allowed(self, pipeline: str, severity: str, now: Optional[datetime] = None) -> bool:
        now = now or datetime.utcnow()
        self._reset_if_needed(now)
        if self._total >= self.config.max_total_per_hour:
            return False
        key = self._key(pipeline, severity)
        entry = self._entries.get(key)
        count = entry.count if entry else 0
        if severity == "critical" and count >= self.config.max_critical_per_hour:
            return False
        if severity == "warning" and count >= self.config.max_warning_per_hour:
            return False
        return True

    def record(self, pipeline: str, severity: str, now: Optional[datetime] = None) -> None:
        now = now or datetime.utcnow()
        self._reset_if_needed(now)
        key = self._key(pipeline, severity)
        if key not in self._entries:
            self._entries[key] = QuotaEntry(pipeline=pipeline, severity=severity, window_start=now)
        self._entries[key].count += 1
        self._total += 1

    def status(self) -> dict:
        return {
            "total": self._total,
            "window_start": self._window_start.isoformat(),
            "entries": [e.to_dict() for e in self._entries.values()],
        }

    def clear(self) -> None:
        self._entries.clear()
        self._total = 0
        self._window_start = datetime.utcnow()
