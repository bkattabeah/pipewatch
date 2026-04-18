"""Retention policy for snapshot cleanup."""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List

from pipewatch.snapshot import Snapshot


@dataclass
class RetentionConfig:
    max_snapshots: int = 50
    max_age_days: int = 30

    def validate(self) -> None:
        if self.max_snapshots < 1:
            raise ValueError("max_snapshots must be >= 1")
        if self.max_age_days < 1:
            raise ValueError("max_age_days must be >= 1")


@dataclass
class RetentionResult:
    removed: List[str] = field(default_factory=list)
    kept: int = 0

    def to_dict(self) -> dict:
        return {"removed": self.removed, "kept": self.kept, "total_removed": len(self.removed)}


def apply_retention(snapshots: List[Snapshot], config: RetentionConfig) -> RetentionResult:
    """Return a RetentionResult indicating which snapshot IDs should be removed."""
    config.validate()
    cutoff = datetime.utcnow() - timedelta(days=config.max_age_days)
    result = RetentionResult()

    # Filter by age first
    survivors = [s for s in snapshots if s.timestamp >= cutoff]
    aged_out = [s for s in snapshots if s.timestamp < cutoff]
    result.removed.extend(s.snapshot_id for s in aged_out)

    # Then trim by count (keep newest)
    survivors.sort(key=lambda s: s.timestamp, reverse=True)
    if len(survivors) > config.max_snapshots:
        excess = survivors[config.max_snapshots:]
        result.removed.extend(s.snapshot_id for s in excess)
        survivors = survivors[: config.max_snapshots]

    result.kept = len(survivors)
    return result
