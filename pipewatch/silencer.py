"""Alert silencing rules — suppress alerts for pipelines during maintenance windows."""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class SilenceRule:
    pipeline: str
    reason: str
    start: datetime
    end: datetime
    created_by: str = "user"

    def is_active(self, at: Optional[datetime] = None) -> bool:
        now = at or datetime.utcnow()
        return self.start <= now <= self.end

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "reason": self.reason,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "created_by": self.created_by,
        }


@dataclass
class Silencer:
    _rules: list = field(default_factory=list)

    def add(self, rule: SilenceRule) -> None:
        self._rules.append(rule)

    def remove(self, pipeline: str) -> int:
        before = len(self._rules)
        self._rules = [r for r in self._rules if r.pipeline != pipeline]
        return before - len(self._rules)

    def is_silenced(self, pipeline: str, at: Optional[datetime] = None) -> bool:
        return any(r.pipeline == pipeline and r.is_active(at) for r in self._rules)

    def active_rules(self, at: Optional[datetime] = None) -> list:
        return [r for r in self._rules if r.is_active(at)]

    def all_rules(self) -> list:
        return list(self._rules)

    def clear_expired(self, at: Optional[datetime] = None) -> int:
        now = at or datetime.utcnow()
        before = len(self._rules)
        self._rules = [r for r in self._rules if r.end >= now]
        return before - len(self._rules)
