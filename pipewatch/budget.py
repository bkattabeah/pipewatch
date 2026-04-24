"""Alert budget tracking: limits total alerts fired within a rolling time window."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List

from pipewatch.alerts import Alert


@dataclass
class BudgetConfig:
    window_seconds: int = 3600
    max_alerts: int = 100

    def validate(self) -> None:
        if self.window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        if self.max_alerts <= 0:
            raise ValueError("max_alerts must be positive")


@dataclass
class BudgetEntry:
    pipeline: str
    alert_id: str
    fired_at: datetime

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "alert_id": self.alert_id,
            "fired_at": self.fired_at.isoformat(),
        }


@dataclass
class BudgetStatus:
    used: int
    remaining: int
    limit: int
    window_seconds: int
    exhausted: bool

    def to_dict(self) -> dict:
        return {
            "used": self.used,
            "remaining": self.remaining,
            "limit": self.limit,
            "window_seconds": self.window_seconds,
            "exhausted": self.exhausted,
        }


class AlertBudget:
    def __init__(self, config: BudgetConfig | None = None) -> None:
        self.config = config or BudgetConfig()
        self.config.validate()
        self._entries: List[BudgetEntry] = []

    def _prune(self, now: datetime) -> None:
        cutoff = now - timedelta(seconds=self.config.window_seconds)
        self._entries = [e for e in self._entries if e.fired_at >= cutoff]

    def is_allowed(self, alert: Alert, now: datetime | None = None) -> bool:
        now = now or datetime.utcnow()
        self._prune(now)
        return len(self._entries) < self.config.max_alerts

    def record(self, alert: Alert, now: datetime | None = None) -> None:
        now = now or datetime.utcnow()
        self._prune(now)
        self._entries.append(
            BudgetEntry(
                pipeline=alert.rule.name,
                alert_id=id(alert).__str__(),
                fired_at=now,
            )
        )

    def status(self, now: datetime | None = None) -> BudgetStatus:
        now = now or datetime.utcnow()
        self._prune(now)
        used = len(self._entries)
        remaining = max(0, self.config.max_alerts - used)
        return BudgetStatus(
            used=used,
            remaining=remaining,
            limit=self.config.max_alerts,
            window_seconds=self.config.window_seconds,
            exhausted=remaining == 0,
        )

    def clear(self) -> None:
        self._entries.clear()
