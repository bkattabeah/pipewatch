"""Snapshot: capture and persist pipeline metric state at a point in time."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import List, Optional

from pipewatch.metrics import PipelineMetric, to_dict


@dataclass
class Snapshot:
    timestamp: float
    pipeline_id: str
    metrics: List[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "pipeline_id": self.pipeline_id,
            "metrics": self.metrics,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Snapshot":
        return cls(
            timestamp=data["timestamp"],
            pipeline_id=data["pipeline_id"],
            metrics=data.get("metrics", []),
        )


def capture(pipeline_id: str, metrics: List[PipelineMetric]) -> Snapshot:
    """Create a snapshot from current metrics."""
    return Snapshot(
        timestamp=time.time(),
        pipeline_id=pipeline_id,
        metrics=[to_dict(m) for m in metrics],
    )


def save_snapshot(snapshot: Snapshot, path: Path) -> None:
    """Persist a snapshot to a JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(snapshot.to_dict(), f, indent=2)


def load_snapshot(path: Path) -> Optional[Snapshot]:
    """Load a snapshot from a JSON file, returns None if file missing."""
    if not path.exists():
        return None
    with open(path) as f:
        data = json.load(f)
    return Snapshot.from_dict(data)
