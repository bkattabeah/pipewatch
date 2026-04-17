"""Summarize a replay sequence — status changes and error rate trends."""

from dataclasses import dataclass, field
from typing import List, Dict, Optional
from pipewatch.replay import ReplayConfig, replay, ReplayFrame
from pipewatch.metrics import PipelineStatus


@dataclass
class PipelineReplaySummary:
    pipeline: str
    frames_seen: int
    status_changes: int
    min_error_rate: float
    max_error_rate: float
    final_status: Optional[str]

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "frames_seen": self.frames_seen,
            "status_changes": self.status_changes,
            "min_error_rate": round(self.min_error_rate, 4),
            "max_error_rate": round(self.max_error_rate, 4),
            "final_status": self.final_status,
        }


def summarize_replay(config: ReplayConfig) -> List[PipelineReplaySummary]:
    pipeline_data: Dict[str, dict] = {}

    for frame in replay(config):
        for name, metric in frame.snapshot.metrics.items():
            if config.pipeline and name != config.pipeline:
                continue
            if name not in pipeline_data:
                pipeline_data[name] = {
                    "frames": 0,
                    "status_changes": 0,
                    "last_status": None,
                    "error_rates": [],
                    "final_status": None,
                }
            d = pipeline_data[name]
            er = metric.failed_runs / metric.total_runs if metric.total_runs else 0.0
            d["error_rates"].append(er)
            d["frames"] += 1
            if d["last_status"] is not None and d["last_status"] != metric.status:
                d["status_changes"] += 1
            d["last_status"] = metric.status
            d["final_status"] = metric.status.value

    results = []
    for name, d in pipeline_data.items():
        rates = d["error_rates"]
        results.append(PipelineReplaySummary(
            pipeline=name,
            frames_seen=d["frames"],
            status_changes=d["status_changes"],
            min_error_rate=min(rates) if rates else 0.0,
            max_error_rate=max(rates) if rates else 0.0,
            final_status=d["final_status"],
        ))
    return results
