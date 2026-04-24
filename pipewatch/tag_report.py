"""Summarize metrics grouped by tag."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.tag import TagRule, group_by_tag


@dataclass
class TagGroupSummary:
    tag_value: str
    total: int
    healthy: int
    warning: int
    critical: int
    unknown: int

    def to_dict(self) -> Dict:
        return {
            "tag_value": self.tag_value,
            "total": self.total,
            "healthy": self.healthy,
            "warning": self.warning,
            "critical": self.critical,
            "unknown": self.unknown,
        }


def _count_status(metrics: List[PipelineMetric], status: PipelineStatus) -> int:
    return sum(1 for m in metrics if m.status == status)


def build_tag_report(
    metrics: List[PipelineMetric],
    rules: List[TagRule],
    key: str,
) -> List[TagGroupSummary]:
    """Build per-tag-group summaries for a given tag key."""
    groups = group_by_tag(metrics, rules, key=key)
    summaries: List[TagGroupSummary] = []
    for tag_value, group_metrics in sorted(groups.items()):
        summaries.append(
            TagGroupSummary(
                tag_value=tag_value,
                total=len(group_metrics),
                healthy=_count_status(group_metrics, PipelineStatus.HEALTHY),
                warning=_count_status(group_metrics, PipelineStatus.WARNING),
                critical=_count_status(group_metrics, PipelineStatus.CRITICAL),
                unknown=_count_status(group_metrics, PipelineStatus.UNKNOWN),
            )
        )
    return summaries


def format_tag_report(summaries: List[TagGroupSummary]) -> str:
    """Format tag report summaries as a plain-text table."""
    if not summaries:
        return "No tag groups found."
    lines = [f"{'Tag':<20} {'Total':>6} {'OK':>6} {'WARN':>6} {'CRIT':>6} {'UNK':>6}"]
    lines.append("-" * 54)
    for s in summaries:
        lines.append(
            f"{s.tag_value:<20} {s.total:>6} {s.healthy:>6} {s.warning:>6} {s.critical:>6} {s.unknown:>6}"
        )
    return "\n".join(lines)
