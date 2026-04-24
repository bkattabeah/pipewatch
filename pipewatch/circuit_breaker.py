"""Circuit breaker for alert pipelines — prevents alert storms by opening the circuit after repeated failures."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Optional


class CircuitState(str, Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Blocking; too many failures
    HALF_OPEN = "half_open"  # Testing if recovery is possible


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5        # failures before opening
    recovery_timeout: int = 60        # seconds before moving to HALF_OPEN
    success_threshold: int = 2        # successes in HALF_OPEN before closing

    def validate(self) -> None:
        if self.failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if self.recovery_timeout < 1:
            raise ValueError("recovery_timeout must be >= 1")
        if self.success_threshold < 1:
            raise ValueError("success_threshold must be >= 1")


@dataclass
class CircuitEntry:
    pipeline: str
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    opened_at: Optional[datetime] = None
    last_updated: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "opened_at": self.opened_at.isoformat() if self.opened_at else None,
            "last_updated": self.last_updated.isoformat(),
        }


class AlertCircuitBreaker:
    def __init__(self, config: Optional[CircuitBreakerConfig] = None) -> None:
        self.config = config or CircuitBreakerConfig()
        self.config.validate()
        self._entries: Dict[str, CircuitEntry] = {}

    def _get(self, pipeline: str) -> CircuitEntry:
        if pipeline not in self._entries:
            self._entries[pipeline] = CircuitEntry(pipeline=pipeline)
        return self._entries[pipeline]

    def _maybe_transition_to_half_open(self, entry: CircuitEntry) -> None:
        if entry.state == CircuitState.OPEN and entry.opened_at:
            elapsed = (datetime.utcnow() - entry.opened_at).total_seconds()
            if elapsed >= self.config.recovery_timeout:
                entry.state = CircuitState.HALF_OPEN
                entry.success_count = 0

    def is_allowed(self, pipeline: str) -> bool:
        entry = self._get(pipeline)
        self._maybe_transition_to_half_open(entry)
        return entry.state != CircuitState.OPEN

    def record_success(self, pipeline: str) -> None:
        entry = self._get(pipeline)
        self._maybe_transition_to_half_open(entry)
        if entry.state == CircuitState.HALF_OPEN:
            entry.success_count += 1
            if entry.success_count >= self.config.success_threshold:
                entry.state = CircuitState.CLOSED
                entry.failure_count = 0
                entry.success_count = 0
                entry.opened_at = None
        elif entry.state == CircuitState.CLOSED:
            entry.failure_count = max(0, entry.failure_count - 1)
        entry.last_updated = datetime.utcnow()

    def record_failure(self, pipeline: str) -> None:
        entry = self._get(pipeline)
        self._maybe_transition_to_half_open(entry)
        if entry.state in (CircuitState.CLOSED, CircuitState.HALF_OPEN):
            entry.failure_count += 1
            if entry.failure_count >= self.config.failure_threshold:
                entry.state = CircuitState.OPEN
                entry.opened_at = datetime.utcnow()
                entry.success_count = 0
        entry.last_updated = datetime.utcnow()

    def status(self) -> Dict[str, dict]:
        return {p: e.to_dict() for p, e in self._entries.items()}

    def reset(self, pipeline: str) -> None:
        self._entries.pop(pipeline, None)
