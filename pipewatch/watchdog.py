"""Watchdog: detect pipelines that have stopped reporting metrics."""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional

from pipewatch.collector import MetricCollector


@dataclass
class StaleEntry:
    pipeline_id: str
    last_seen: datetime
    stale_for_seconds: float

    def to_dict(self) -> dict:
        return {
            "pipeline_id": self.pipeline_id,
            "last_seen": self.last_seen.isoformat(),
            "stale_for_seconds": round(self.stale_for_seconds, 2),
        }


@dataclass
class WatchdogConfig:
    stale_threshold_seconds: float = 60.0
    pipelines: List[str] = field(default_factory=list)


class Watchdog:
    def __init__(self, collector: MetricCollector, config: Optional[WatchdogConfig] = None):
        self.collector = collector
        self.config = config or WatchdogConfig()

    def check(self, now: Optional[datetime] = None) -> List[StaleEntry]:
        now = now or datetime.utcnow()
        threshold = timedelta(seconds=self.config.stale_threshold_seconds)
        stale: List[StaleEntry] = []

        pipelines = self.config.pipelines or list(self.collector._store.keys())

        for pid in pipelines:
            metric = self.collector.latest(pid)
            if metric is None:
                stale.append(StaleEntry(
                    pipeline_id=pid,
                    last_seen=datetime.min,
                    stale_for_seconds=(now - datetime.min).total_seconds(),
                ))
                continue
            age = now - metric.timestamp
            if age > threshold:
                stale.append(StaleEntry(
                    pipeline_id=pid,
                    last_seen=metric.timestamp,
                    stale_for_seconds=age.total_seconds(),
                ))

        return stale

    def is_healthy(self, now: Optional[datetime] = None) -> bool:
        return len(self.check(now=now)) == 0
