"""Pipeline scoring: assign a numeric priority score to alerts based on severity, recency, and error rate."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipewatch.alerts import Alert
from pipewatch.metrics import PipelineStatus


# Weight constants
_SEVERITY_WEIGHTS = {
    PipelineStatus.CRITICAL: 100,
    PipelineStatus.WARNING: 50,
    PipelineStatus.HEALTHY: 0,
    PipelineStatus.UNKNOWN: 10,
}
_ERROR_RATE_MULTIPLIER = 200.0
_RECENCY_DECAY = 0.95  # per second older than reference


@dataclass
class ScoredAlert:
    alert: Alert
    score: float

    def to_dict(self) -> dict:
        return {
            "pipeline": self.alert.metric.pipeline_name,
            "status": self.alert.metric.status.value,
            "score": round(self.score, 4),
            "rule": self.alert.rule.name,
        }


def _severity_score(alert: Alert) -> float:
    return float(_SEVERITY_WEIGHTS.get(alert.metric.status, 0))


def _error_rate_score(alert: Alert) -> float:
    return alert.metric.error_rate * _ERROR_RATE_MULTIPLIER


def _recency_score(alert: Alert, reference_ts: float) -> float:
    """More recent alerts score higher; decay applied per second of age."""
    age = max(0.0, reference_ts - alert.metric.timestamp)
    return 50.0 * (_RECENCY_DECAY ** age)


def score_alert(alert: Alert, reference_ts: float) -> ScoredAlert:
    """Compute a composite priority score for a single alert."""
    score = (
        _severity_score(alert)
        + _error_rate_score(alert)
        + _recency_score(alert, reference_ts)
    )
    return ScoredAlert(alert=alert, score=score)


def rank_alerts(alerts: List[Alert], reference_ts: float) -> List[ScoredAlert]:
    """Return alerts sorted by descending priority score."""
    scored = [score_alert(a, reference_ts) for a in alerts]
    scored.sort(key=lambda s: s.score, reverse=True)
    return scored
