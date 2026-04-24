"""Alert reaper: automatically closes stale open incidents/alerts after a configurable TTL."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional


@dataclass
class ReaperConfig:
    ttl_seconds: int = 3600  # how long before an unresolved alert is reaped
    max_reaped_per_run: int = 100

    def validate(self) -> None:
        if self.ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        if self.max_reaped_per_run <= 0:
            raise ValueError("max_reaped_per_run must be positive")


@dataclass
class ReapedEntry:
    alert_id: str
    pipeline: str
    reason: str
    reaped_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "alert_id": self.alert_id,
            "pipeline": self.pipeline,
            "reason": self.reason,
            "reaped_at": self.reaped_at.isoformat(),
        }


@dataclass
class ReapResult:
    reaped: List[ReapedEntry] = field(default_factory=list)
    skipped: int = 0

    @property
    def total_reaped(self) -> int:
        return len(self.reaped)

    def to_dict(self) -> dict:
        return {
            "total_reaped": self.total_reaped,
            "skipped": self.skipped,
            "reaped": [e.to_dict() for e in self.reaped],
        }


class AlertReaper:
    def __init__(self, config: Optional[ReaperConfig] = None) -> None:
        self.config = config or ReaperConfig()
        self.config.validate()
        self._log: List[ReapedEntry] = []

    def reap(self, incidents: list, now: Optional[datetime] = None) -> ReapResult:
        """Reap stale open incidents. Expects objects with .is_open, .alert, .opened_at, .resolve()."""
        now = now or datetime.utcnow()
        cutoff = now - timedelta(seconds=self.config.ttl_seconds)
        result = ReapResult()

        for incident in incidents:
            if result.total_reaped >= self.config.max_reaped_per_run:
                result.skipped += 1
                continue
            if not incident.is_open:
                continue
            if incident.opened_at <= cutoff:
                entry = ReapedEntry(
                    alert_id=incident.alert.rule_name,
                    pipeline=incident.alert.pipeline,
                    reason=f"TTL of {self.config.ttl_seconds}s exceeded",
                    reaped_at=now,
                )
                incident.resolve()
                result.reaped.append(entry)
                self._log.append(entry)

        return result

    def log(self) -> List[ReapedEntry]:
        return list(self._log)

    def clear_log(self) -> None:
        self._log.clear()
