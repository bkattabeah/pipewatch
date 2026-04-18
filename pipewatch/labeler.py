from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewatch.metrics import PipelineMetric, PipelineStatus


@dataclass
class Label:
    key: str
    value: str

    def __str__(self) -> str:
        return f"{self.key}={self.value}"

    def to_dict(self) -> Dict:
        return {"key": self.key, "value": self.value}


@dataclass
class LabelRule:
    key: str
    value: str
    status: Optional[PipelineStatus] = None
    name_prefix: Optional[str] = None

    def matches(self, metric: PipelineMetric) -> bool:
        if self.status is not None and metric.status != self.status:
            return False
        if self.name_prefix is not None and not metric.pipeline_name.startswith(self.name_prefix):
            return False
        return True


def apply_labels(metric: PipelineMetric, rules: List[LabelRule]) -> List[Label]:
    return [
        Label(key=r.key, value=r.value)
        for r in rules
        if r.matches(metric)
    ]


class Labeler:
    def __init__(self, rules: Optional[List[LabelRule]] = None) -> None:
        self._rules: List[LabelRule] = rules or []

    def add_rule(self, rule: LabelRule) -> None:
        self._rules.append(rule)

    def label(self, metric: PipelineMetric) -> List[Label]:
        return apply_labels(metric, self._rules)

    def label_many(self, metrics: List[PipelineMetric]) -> Dict[str, List[Label]]:
        return {m.pipeline_name: self.label(m) for m in metrics}
