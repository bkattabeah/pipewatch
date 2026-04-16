"""SnapshotStore: manage multiple snapshots across pipelines."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

from pipewatch.snapshot import Snapshot, load_snapshot, save_snapshot


DEFAULT_STORE_DIR = Path(".pipewatch") / "snapshots"


class SnapshotStore:
    def __init__(self, store_dir: Path = DEFAULT_STORE_DIR) -> None:
        self.store_dir = store_dir
        self._cache: Dict[str, List[Snapshot]] = {}

    def _path(self, pipeline_id: str, index: int = 0) -> Path:
        return self.store_dir / pipeline_id / f"{index:04d}.json"

    def save(self, snapshot: Snapshot, max_history: int = 10) -> None:
        pid = snapshot.pipeline_id
        history = self.list(pid)
        history.append(snapshot)
        if len(history) > max_history:
            history = history[-max_history:]
        self._cache[pid] = history
        for i, snap in enumerate(history):
            save_snapshot(snap, self._path(pid, i))

    def list(self, pipeline_id: str) -> List[Snapshot]:
        if pipeline_id in self._cache:
            return list(self._cache[pipeline_id])
        snapshots: List[Snapshot] = []
        pipeline_dir = self.store_dir / pipeline_id
        if pipeline_dir.exists():
            for p in sorted(pipeline_dir.glob("*.json")):
                s = load_snapshot(p)
                if s is not None:
                    snapshots.append(s)
        self._cache[pipeline_id] = snapshots
        return list(snapshots)

    def latest(self, pipeline_id: str) -> Optional[Snapshot]:
        history = self.list(pipeline_id)
        return history[-1] if history else None

    def clear(self, pipeline_id: str) -> None:
        self._cache.pop(pipeline_id, None)
        pipeline_dir = self.store_dir / pipeline_id
        if pipeline_dir.exists():
            for p in pipeline_dir.glob("*.json"):
                p.unlink()
