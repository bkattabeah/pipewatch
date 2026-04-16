"""Report generation for pipeline health summaries."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from pipewatch.collector import MetricCollector
from pipewatch.metrics import PipelineMetric, PipelineStatus, evaluate_status, to_dict
from pipewatch.alerts import Alert, AlertEngine


@dataclass
class PipelineReport:
    pipeline_id: str
    generated_at: datetime
    status: PipelineStatus
    total_records: int
    error_rate: float
    alert_count: int
    alerts: List[Alert] = field(default_factory=list)
    latest_metric: Optional[dict] = None

    def to_dict(self) -> dict:
        return {
            "pipeline_id": self.pipeline_id,
            "generated_at": self.generated_at.isoformat(),
            "status": self.status.value,
            "total_records": self.total_records,
            "error_rate": round(self.error_rate, 4),
            "alert_count": self.alert_count,
            "alerts": [str(a) for a in self.alerts],
            "latest_metric": self.latest_metric,
        }

    def summary(self) -> str:
        lines = [
            f"Pipeline Report: {self.pipeline_id}",
            f"  Generated : {self.generated_at.strftime('%Y-%m-%d %H:%M:%S')}",
            f"  Status    : {self.status.value.upper()}",
            f"  Records   : {self.total_records}",
            f"  Error Rate: {self.error_rate:.2%}",
            f"  Alerts    : {self.alert_count}",
        ]
        for alert in self.alerts:
            lines.append(f"    - {alert}")
        return "\n".join(lines)


class Reporter:
    def __init__(self, collector: MetricCollector, alert_engine: AlertEngine):
        self.collector = collector
        self.alert_engine = alert_engine

    def generate(self, pipeline_id: str) -> PipelineReport:
        metric = self.collector.latest(pipeline_id)
        if metric is None:
            return PipelineReport(
                pipeline_id=pipeline_id,
                generated_at=datetime.utcnow(),
                status=PipelineStatus.UNKNOWN,
                total_records=0,
                error_rate=0.0,
                alert_count=0,
            )

        from pipewatch.metrics import error_rate
        rate = error_rate(metric)
        status = evaluate_status(metric)
        alerts = self.alert_engine.evaluate(metric)

        return PipelineReport(
            pipeline_id=pipeline_id,
            generated_at=datetime.utcnow(),
            status=status,
            total_records=metric.total_records,
            error_rate=rate,
            alert_count=len(alerts),
            alerts=alerts,
            latest_metric=to_dict(metric),
        )
