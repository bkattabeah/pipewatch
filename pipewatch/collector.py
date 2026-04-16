"""Metric collector that aggregates and stores recent pipeline metrics."""

from collections import deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric, PipelineStatus

DEFAULT_WINDOW_SIZE = 60  # keep last 60 samples per pipeline


class MetricCollector:
    def __init__(self, window_size: int = DEFAULT_WINDOW_SIZE):
        self.window_size = window_size
        self._store: Dict[str, deque] = {}

    def record(self, metric: PipelineMetric) -> None:
        pid = metric.pipeline_id
        if pid not in self._store:
            self._store[pid] = deque(maxlen=self.window_size)
        self._store[pid].append(metric)

    def latest(self, pipeline_id: str) -> Optional[PipelineMetric]:
        buf = self._store.get(pipeline_id)
        if not buf:
            return None
        return buf[-1]

    def history(
        self, pipeline_id: str, since: Optional[datetime] = None
    ) -> List[PipelineMetric]:
        buf = self._store.get(pipeline_id, [])
        if since is None:
            return list(buf)
        return [m for m in buf if m.timestamp >= since]

    def pipeline_ids(self) -> List[str]:
        return list(self._store.keys())

    def summary(self, pipeline_id: str) -> Optional[dict]:
        metrics = self.history(pipeline_id)
        if not metrics:
            return None
        latest = metrics[-1]
        avg_latency = sum(m.latency_ms for m in metrics) / len(metrics)
        avg_throughput = sum(m.throughput_per_sec for m in metrics) / len(metrics)
        status = latest.evaluate_status()
        return {
            "pipeline_id": pipeline_id,
            "status": status.value,
            "sample_count": len(metrics),
            "latest_error_rate": round(latest.error_rate, 4),
            "avg_latency_ms": round(avg_latency, 2),
            "avg_throughput_per_sec": round(avg_throughput, 2),
            "last_updated": latest.timestamp.isoformat(),
        }
