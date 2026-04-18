"""Rate limiter to suppress repeated alerts within a time window."""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional


@dataclass
class RateLimiterConfig:
    window_seconds: int = 60
    max_alerts: int = 3

    def validate(self) -> None:
        if self.window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        if self.max_alerts <= 0:
            raise ValueError("max_alerts must be positive")


@dataclass
class RateLimitEntry:
    pipeline: str
    rule_name: str
    count: int = 0
    window_start: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "rule_name": self.rule_name,
            "count": self.count,
            "window_start": self.window_start.isoformat(),
        }


class AlertRateLimiter:
    def __init__(self, config: Optional[RateLimiterConfig] = None) -> None:
        self.config = config or RateLimiterConfig()
        self._entries: Dict[str, RateLimitEntry] = {}

    def _key(self, pipeline: str, rule_name: str) -> str:
        return f"{pipeline}::{rule_name}"

    def is_allowed(self, pipeline: str, rule_name: str) -> bool:
        key = self._key(pipeline, rule_name)
        now = datetime.utcnow()
        window = timedelta(seconds=self.config.window_seconds)

        if key not in self._entries:
            self._entries[key] = RateLimitEntry(pipeline=pipeline, rule_name=rule_name, count=1, window_start=now)
            return True

        entry = self._entries[key]
        if now - entry.window_start > window:
            entry.count = 1
            entry.window_start = now
            return True

        if entry.count < self.config.max_alerts:
            entry.count += 1
            return True

        return False

    def status(self) -> list:
        return [e.to_dict() for e in self._entries.values()]

    def reset(self, pipeline: str, rule_name: str) -> None:
        key = self._key(pipeline, rule_name)
        self._entries.pop(key, None)

    def reset_all(self) -> None:
        self._entries.clear()
