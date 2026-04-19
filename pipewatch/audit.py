"""Audit log for alert and status change events."""
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional
from pipewatch.metrics import PipelineStatus


@dataclass
class AuditEvent:
    pipeline: str
    event_type: str  # 'alert', 'status_change', 'stale', 'recovery'
    severity: str
    message: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "event_type": self.event_type,
            "severity": self.severity,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
        }


class AuditLog:
    def __init__(self, max_size: int = 500):
        self._events: List[AuditEvent] = []
        self.max_size = max_size

    def record(self, event: AuditEvent) -> None:
        self._events.append(event)
        if len(self._events) > self.max_size:
            self._events = self._events[-self.max_size:]

    def all(self) -> List[AuditEvent]:
        return list(self._events)

    def for_pipeline(self, pipeline: str) -> List[AuditEvent]:
        return [e for e in self._events if e.pipeline == pipeline]

    def by_type(self, event_type: str) -> List[AuditEvent]:
        return [e for e in self._events if e.event_type == event_type]

    def since(self, dt: datetime) -> List[AuditEvent]:
        return [e for e in self._events if e.timestamp >= dt]

    def clear(self) -> None:
        self._events = []

    def to_dict_list(self) -> List[dict]:
        return [e.to_dict() for e in self._events]
