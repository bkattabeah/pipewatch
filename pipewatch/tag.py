"""Tag-based grouping and filtering for pipeline metrics."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class Tag:
    key: str
    value: str

    def __str__(self) -> str:
        return f"{self.key}:{self.value}"

    def to_dict(self) -> Dict[str, str]:
        return {"key": self.key, "value": self.value}


@dataclass
class TagRule:
    key: str
    value: str
    name_prefix: Optional[str] = None
    status: Optional[str] = None

    def matches(self, metric: PipelineMetric) -> bool:
        if self.name_prefix and not metric.pipeline_name.startswith(self.name_prefix):
            return False
        if self.status and metric.status.value != self.status:
            return False
        return True

    def to_tag(self) -> Tag:
        return Tag(key=self.key, value=self.value)


def apply_tags(metric: PipelineMetric, rules: List[TagRule]) -> List[Tag]:
    """Return all tags whose rules match the given metric."""
    return [rule.to_tag() for rule in rules if rule.matches(metric)]


def group_by_tag(metrics: List[PipelineMetric], rules: List[TagRule], key: str) -> Dict[str, List[PipelineMetric]]:
    """Group metrics by the value of a specific tag key."""
    groups: Dict[str, List[PipelineMetric]] = {}
    for metric in metrics:
        tags = apply_tags(metric, rules)
        matched = [t.value for t in tags if t.key == key]
        bucket = matched[0] if matched else "__untagged__"
        groups.setdefault(bucket, []).append(metric)
    return groups
