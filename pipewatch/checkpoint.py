"""Checkpoint tracking: record and compare pipeline run progress markers."""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any


@dataclass
class Checkpoint:
    pipeline: str
    marker: str
    recorded_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline": self.pipeline,
            "marker": self.marker,
            "recorded_at": self.recorded_at,
            "metadata": self.metadata,
        }

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Checkpoint":
        return Checkpoint(
            pipeline=d["pipeline"],
            marker=d["marker"],
            recorded_at=d.get("recorded_at", ""),
            metadata=d.get("metadata", {}),
        )


@dataclass
class CheckpointDiff:
    pipeline: str
    previous: Optional[str]
    current: Optional[str]

    @property
    def changed(self) -> bool:
        return self.previous != self.current

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline": self.pipeline,
            "previous": self.previous,
            "current": self.current,
            "changed": self.changed,
        }


class CheckpointStore:
    def __init__(self) -> None:
        self._store: Dict[str, Checkpoint] = {}

    def record(self, checkpoint: Checkpoint) -> None:
        self._store[checkpoint.pipeline] = checkpoint

    def get(self, pipeline: str) -> Optional[Checkpoint]:
        return self._store.get(pipeline)

    def all(self) -> Dict[str, Checkpoint]:
        return dict(self._store)

    def compare(self, pipeline: str, new_marker: str) -> CheckpointDiff:
        existing = self._store.get(pipeline)
        return CheckpointDiff(
            pipeline=pipeline,
            previous=existing.marker if existing else None,
            current=new_marker,
        )

    def clear(self, pipeline: str) -> bool:
        if pipeline in self._store:
            del self._store[pipeline]
            return True
        return False
