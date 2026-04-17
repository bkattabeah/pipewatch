"""Replay historical snapshots to simulate pipeline metric progression."""

from dataclasses import dataclass, field
from typing import List, Optional, Iterator
from pipewatch.snapshot import Snapshot
from pipewatch.snapshot_store import SnapshotStore


@dataclass
class ReplayFrame:
    index: int
    snapshot: Snapshot
    is_last: bool

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "is_last": self.is_last,
            "snapshot": self.snapshot.to_dict(),
        }


@dataclass
class ReplayConfig:
    store_dir: str
    pipeline: Optional[str] = None
    limit: int = 10
    reverse: bool = False


def load_snapshots(config: ReplayConfig) -> List[Snapshot]:
    store = SnapshotStore(config.store_dir)
    names = store.list()
    if config.reverse:
        names = list(reversed(names))
    names = names[: config.limit]
    snapshots = [store.load(n) for n in names]
    return [s for s in snapshots if s is not None]


def replay(config: ReplayConfig) -> Iterator[ReplayFrame]:
    snapshots = load_snapshots(config)
    total = len(snapshots)
    for i, snap in enumerate(snapshots):
        if config.pipeline:
            if config.pipeline not in snap.metrics:
                continue
        yield ReplayFrame(index=i, snapshot=snap, is_last=(i == total - 1))
