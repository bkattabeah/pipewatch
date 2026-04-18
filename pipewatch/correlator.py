"""Correlate alerts across pipelines to detect common-cause failures."""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from pipewatch.alerts import Alert


@dataclass
class CorrelationGroup:
    group_id: str
    alerts: List[Alert]
    window_seconds: int
    created_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "group_id": self.group_id,
            "alert_count": len(self.alerts),
            "pipelines": [a.pipeline for a in self.alerts],
            "window_seconds": self.window_seconds,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class CorrelatorConfig:
    window_seconds: int = 60
    min_group_size: int = 2

    def validate(self) -> None:
        if self.window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        if self.min_group_size < 2:
            raise ValueError("min_group_size must be at least 2")


class AlertCorrelator:
    def __init__(self, config: Optional[CorrelatorConfig] = None):
        self.config = config or CorrelatorConfig()
        self.config.validate()
        self._alerts: List[Alert] = []

    def record(self, alert: Alert) -> None:
        self._alerts.append(alert)

    def _is_recent(self, alert: Alert) -> bool:
        cutoff = datetime.utcnow() - timedelta(seconds=self.config.window_seconds)
        return alert.triggered_at >= cutoff

    def correlate(self) -> List[CorrelationGroup]:
        recent = [a for a in self._alerts if self._is_recent(a)]
        by_severity: Dict[str, List[Alert]] = {}
        for alert in recent:
            key = alert.severity
            by_severity.setdefault(key, []).append(alert)

        groups = []
        for severity, alerts in by_severity.items():
            if len(alerts) >= self.config.min_group_size:
                group_id = f"{severity}_{int(datetime.utcnow().timestamp())}"
                groups.append(CorrelationGroup(
                    group_id=group_id,
                    alerts=alerts,
                    window_seconds=self.config.window_seconds,
                ))
        return groups

    def clear(self) -> None:
        self._alerts.clear()
