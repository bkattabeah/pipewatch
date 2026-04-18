"""Alert escalation: track repeated alerts and escalate severity after threshold."""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from pipewatch.alerts import Alert
from pipewatch.metrics import PipelineStatus


@dataclass
class EscalationConfig:
    repeat_threshold: int = 3  # escalate after this many repeats
    window_seconds: float = 300.0  # within this window


@dataclass
class EscalationEntry:
    pipeline: str
    rule_name: str
    occurrences: List[datetime] = field(default_factory=list)
    escalated: bool = False

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "rule_name": self.rule_name,
            "occurrences": [t.isoformat() for t in self.occurrences],
            "escalated": self.escalated,
        }


class AlertEscalator:
    def __init__(self, config: Optional[EscalationConfig] = None):
        self.config = config or EscalationConfig()
        self._entries: Dict[str, EscalationEntry] = {}

    def _key(self, alert: Alert) -> str:
        return f"{alert.pipeline}::{alert.rule_name}"

    def _prune(self, entry: EscalationEntry, now: datetime) -> None:
        cutoff = now.timestamp() - self.config.window_seconds
        entry.occurrences = [t for t in entry.occurrences if t.timestamp() >= cutoff]

    def record(self, alert: Alert, now: Optional[datetime] = None) -> bool:
        """Record alert occurrence. Returns True if escalation threshold reached."""
        now = now or datetime.utcnow()
        key = self._key(alert)
        if key not in self._entries:
            self._entries[key] = EscalationEntry(
                pipeline=alert.pipeline, rule_name=alert.rule_name
            )
        entry = self._entries[key]
        self._prune(entry, now)
        entry.occurrences.append(now)
        if len(entry.occurrences) >= self.config.repeat_threshold:
            entry.escalated = True
            return True
        return False

    def is_escalated(self, alert: Alert) -> bool:
        key = self._key(alert)
        return self._entries.get(key, EscalationEntry("", "")).escalated

    def clear(self, alert: Alert) -> None:
        self._entries.pop(self._key(alert), None)

    def all_entries(self) -> List[EscalationEntry]:
        return list(self._entries.values())
