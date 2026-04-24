"""Incident tracking for persistent alert conditions."""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipewatch.alerts import Alert


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Incident:
    pipeline: str
    severity: str
    message: str
    incident_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    opened_at: datetime = field(default_factory=_now)
    resolved_at: Optional[datetime] = None
    alert_count: int = 1

    @property
    def is_open(self) -> bool:
        return self.resolved_at is None

    def resolve(self) -> None:
        if self.is_open:
            self.resolved_at = _now()

    def increment(self) -> None:
        self.alert_count += 1

    def to_dict(self) -> Dict:
        return {
            "incident_id": self.incident_id,
            "pipeline": self.pipeline,
            "severity": self.severity,
            "message": self.message,
            "opened_at": self.opened_at.isoformat(),
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "alert_count": self.alert_count,
            "is_open": self.is_open,
        }


@dataclass
class IncidentManagerConfig:
    auto_resolve: bool = True
    reopen_on_new_alert: bool = True


class IncidentManager:
    """Tracks open/closed incidents per pipeline."""

    def __init__(self, config: Optional[IncidentManagerConfig] = None) -> None:
        self.config = config or IncidentManagerConfig()
        self._incidents: Dict[str, Incident] = {}
        self._history: List[Incident] = []

    def process(self, alert: Alert) -> Incident:
        key = alert.pipeline
        existing = self._incidents.get(key)

        if existing and existing.is_open:
            existing.increment()
            return existing

        if existing and not existing.is_open and self.config.reopen_on_new_alert:
            self._history.append(existing)

        incident = Incident(
            pipeline=alert.pipeline,
            severity=alert.severity,
            message=alert.message,
        )
        self._incidents[key] = incident
        return incident

    def resolve(self, pipeline: str) -> Optional[Incident]:
        incident = self._incidents.get(pipeline)
        if incident and incident.is_open:
            incident.resolve()
            return incident
        return None

    def open_incidents(self) -> List[Incident]:
        return [i for i in self._incidents.values() if i.is_open]

    def all_incidents(self) -> List[Incident]:
        return list(self._incidents.values()) + self._history

    def clear(self) -> None:
        self._incidents.clear()
        self._history.clear()
