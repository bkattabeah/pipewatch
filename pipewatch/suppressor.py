"""Alert suppressor: skip alerts below a minimum severity threshold."""
from dataclasses import dataclass, field
from typing import Optional
from pipewatch.alerts import Alert
from pipewatch.metrics import PipelineStatus

_SEVERITY_ORDER = [
    PipelineStatus.HEALTHY,
    PipelineStatus.WARNING,
    PipelineStatus.CRITICAL,
    PipelineStatus.UNKNOWN,
]


@dataclass
class SuppressorConfig:
    min_severity: PipelineStatus = PipelineStatus.WARNING

    def validate(self) -> None:
        if self.min_severity not in _SEVERITY_ORDER:
            raise ValueError(f"Invalid min_severity: {self.min_severity}")


@dataclass
class SuppressResult:
    alert: Alert
    suppressed: bool
    reason: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.alert.pipeline,
            "status": self.alert.status.value,
            "suppressed": self.suppressed,
            "reason": self.reason,
        }


class AlertSuppressor:
    def __init__(self, config: Optional[SuppressorConfig] = None) -> None:
        self.config = config or SuppressorConfig()
        self.config.validate()
        self._suppressed_count: int = 0

    def _severity_index(self, status: PipelineStatus) -> int:
        try:
            return _SEVERITY_ORDER.index(status)
        except ValueError:
            return len(_SEVERITY_ORDER) - 1

    def check(self, alert: Alert) -> SuppressResult:
        min_idx = self._severity_index(self.config.min_severity)
        alert_idx = self._severity_index(alert.status)
        if alert_idx < min_idx:
            self._suppressed_count += 1
            return SuppressResult(
                alert=alert,
                suppressed=True,
                reason=f"severity {alert.status.value} below minimum {self.config.min_severity.value}",
            )
        return SuppressResult(alert=alert, suppressed=False)

    def filter(self, alerts: list) -> list:
        return [a for a in alerts if not self.check(a).suppressed]

    @property
    def suppressed_count(self) -> int:
        return self._suppressed_count

    def reset(self) -> None:
        self._suppressed_count = 0
