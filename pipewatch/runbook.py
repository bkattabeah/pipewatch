"""Runbook suggestions: map pipeline alert conditions to remediation steps."""
from dataclasses import dataclass, field
from typing import Optional
from pipewatch.metrics import PipelineStatus
from pipewatch.alerts import Alert


@dataclass
class RunbookEntry:
    pipeline: str
    status: PipelineStatus
    title: str
    steps: list[str] = field(default_factory=list)
    reference_url: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "status": self.status.value,
            "title": self.title,
            "steps": self.steps,
            "reference_url": self.reference_url,
        }


_DEFAULT_STEPS: dict[PipelineStatus, list[str]] = {
    PipelineStatus.CRITICAL: [
        "Check pipeline logs for recent errors.",
        "Verify upstream data sources are reachable.",
        "Inspect error_rate metric and identify failing records.",
        "Restart the pipeline if errors are transient.",
        "Escalate to on-call engineer if issue persists > 15 min.",
    ],
    PipelineStatus.WARNING: [
        "Review recent metric trends for gradual degradation.",
        "Check for upstream slowdowns or schema changes.",
        "Monitor for escalation to CRITICAL within the next polling window.",
    ],
    PipelineStatus.UNKNOWN: [
        "Confirm the pipeline is actively emitting metrics.",
        "Check collector connectivity and poller configuration.",
    ],
}


class RunbookRegistry:
    def __init__(self) -> None:
        self._entries: dict[tuple[str, PipelineStatus], RunbookEntry] = {}

    def register(self, entry: RunbookEntry) -> None:
        self._entries[(entry.pipeline, entry.status)] = entry

    def lookup(self, pipeline: str, status: PipelineStatus) -> RunbookEntry:
        key = (pipeline, status)
        if key in self._entries:
            return self._entries[key]
        steps = _DEFAULT_STEPS.get(status, ["No remediation steps available."])
        return RunbookEntry(
            pipeline=pipeline,
            status=status,
            title=f"Default runbook for {status.value} on '{pipeline}'",
            steps=steps,
        )

    def suggest(self, alert: Alert) -> RunbookEntry:
        return self.lookup(alert.pipeline, alert.status)

    def all_entries(self) -> list[RunbookEntry]:
        return list(self._entries.values())
