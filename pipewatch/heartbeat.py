"""Heartbeat tracking for pipeline liveness monitoring."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional


@dataclass
class HeartbeatConfig:
    timeout_seconds: float = 60.0
    max_missed: int = 3

    def validate(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if self.max_missed < 1:
            raise ValueError("max_missed must be at least 1")


@dataclass
class HeartbeatEntry:
    pipeline: str
    last_seen: datetime
    missed: int = 0

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "last_seen": self.last_seen.isoformat(),
            "missed": self.missed,
        }


@dataclass
class HeartbeatStatus:
    pipeline: str
    alive: bool
    missed: int
    last_seen: Optional[datetime]
    message: str

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "alive": self.alive,
            "missed": self.missed,
            "last_seen": self.last_seen.isoformat() if self.last_seen else None,
            "message": self.message,
        }


class HeartbeatMonitor:
    def __init__(self, config: Optional[HeartbeatConfig] = None) -> None:
        self.config = config or HeartbeatConfig()
        self.config.validate()
        self._entries: Dict[str, HeartbeatEntry] = {}

    def ping(self, pipeline: str) -> None:
        """Record a heartbeat for the given pipeline."""
        now = datetime.now(timezone.utc)
        self._entries[pipeline] = HeartbeatEntry(
            pipeline=pipeline, last_seen=now, missed=0
        )

    def check(self, pipeline: str) -> HeartbeatStatus:
        """Check liveness of a pipeline."""
        now = datetime.now(timezone.utc)
        entry = self._entries.get(pipeline)
        if entry is None:
            return HeartbeatStatus(
                pipeline=pipeline,
                alive=False,
                missed=self.config.max_missed,
                last_seen=None,
                message="No heartbeat recorded",
            )
        elapsed = (now - entry.last_seen).total_seconds()
        missed = int(elapsed // self.config.timeout_seconds)
        alive = missed < self.config.max_missed
        msg = "alive" if alive else f"missed {missed} heartbeat(s)"
        return HeartbeatStatus(
            pipeline=pipeline,
            alive=alive,
            missed=missed,
            last_seen=entry.last_seen,
            message=msg,
        )

    def check_all(self) -> List[HeartbeatStatus]:
        return [self.check(p) for p in self._entries]

    def pipelines(self) -> List[str]:
        return list(self._entries.keys())
