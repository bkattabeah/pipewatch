"""Snapshot diffing: compare two snapshots and report changes."""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewatch.snapshot import Snapshot


@dataclass
class MetricDiff:
    pipeline: str
    old_status: Optional[str]
    new_status: Optional[str]
    old_error_rate: Optional[float]
    new_error_rate: Optional[float]

    @property
    def status_changed(self) -> bool:
        return self.old_status != self.new_status

    @property
    def error_rate_delta(self) -> Optional[float]:
        if self.old_error_rate is not None and self.new_error_rate is not None:
            return round(self.new_error_rate - self.old_error_rate, 4)
        return None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "old_status": self.old_status,
            "new_status": self.new_status,
            "old_error_rate": self.old_error_rate,
            "new_error_rate": self.new_error_rate,
            "error_rate_delta": self.error_rate_delta,
            "status_changed": self.status_changed,
        }


@dataclass
class SnapshotDiff:
    added: List[str] = field(default_factory=list)
    removed: List[str] = field(default_factory=list)
    changed: List[MetricDiff] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "added": self.added,
            "removed": self.removed,
            "changed": [c.to_dict() for c in self.changed],
        }

    def has_changes(self) -> bool:
        return bool(self.added or self.removed or self.changed)


def diff_snapshots(old: Snapshot, new: Snapshot) -> SnapshotDiff:
    """Compare two snapshots and return a SnapshotDiff."""
    old_map = {r["pipeline"]: r for r in old.to_dict()["reports"]}
    new_map = {r["pipeline"]: r for r in new.to_dict()["reports"]}

    added = [p for p in new_map if p not in old_map]
    removed = [p for p in old_map if p not in new_map]

    changed = []
    for pipeline in old_map:
        if pipeline not in new_map:
            continue
        o, n = old_map[pipeline], new_map[pipeline]
        if o.get("status") != n.get("status") or o.get("error_rate") != n.get("error_rate"):
            changed.append(MetricDiff(
                pipeline=pipeline,
                old_status=o.get("status"),
                new_status=n.get("status"),
                old_error_rate=o.get("error_rate"),
                new_error_rate=n.get("error_rate"),
            ))

    return SnapshotDiff(added=added, removed=removed, changed=changed)
