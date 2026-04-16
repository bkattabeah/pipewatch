from dataclasses import dataclass, field
from typing import Callable, List, Optional
from pipewatch.metrics import PipelineMetric, PipelineStatus, evaluate_status


@dataclass
class AlertRule:
    name: str
    condition: Callable[[PipelineMetric], bool]
    message: str
    severity: str = "warning"  # warning | critical


@dataclass
class Alert:
    rule_name: str
    severity: str
    message: str
    metric: PipelineMetric

    def __str__(self) -> str:
        return (
            f"[{self.severity.upper()}] {self.rule_name}: {self.message} "
            f"(pipeline={self.metric.pipeline_id}, "
            f"error_rate={self.metric.error_rate:.2%})"
        )


class AlertEngine:
    def __init__(self) -> None:
        self._rules: List[AlertRule] = []
        self._handlers: List[Callable[[Alert], None]] = []
        self._register_default_rules()

    def _register_default_rules(self) -> None:
        self.add_rule(AlertRule(
            name="high_error_rate",
            condition=lambda m: m.error_rate >= 0.10,
            message="Error rate exceeded 10%",
            severity="warning",
        ))
        self.add_rule(AlertRule(
            name="critical_error_rate",
            condition=lambda m: m.error_rate >= 0.25,
            message="Error rate exceeded 25%",
            severity="critical",
        ))
        self.add_rule(AlertRule(
            name="pipeline_down",
            condition=lambda m: evaluate_status(m) == PipelineStatus.DOWN,
            message="Pipeline is DOWN (no successful records)",
            severity="critical",
        ))

    def add_rule(self, rule: AlertRule) -> None:
        self._rules.append(rule)

    def add_handler(self, handler: Callable[[Alert], None]) -> None:
        self._handlers.append(handler)

    def evaluate(self, metric: PipelineMetric) -> List[Alert]:
        triggered: List[Alert] = []
        for rule in self._rules:
            if rule.condition(metric):
                alert = Alert(
                    rule_name=rule.name,
                    severity=rule.severity,
                    message=rule.message,
                    metric=metric,
                )
                triggered.append(alert)
                for handler in self._handlers:
                    handler(alert)
        return triggered
