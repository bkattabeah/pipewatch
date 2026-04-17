"""High-level poller that wires collector, alert engine, and handlers together."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List

from pipewatch.alerts import Alert, AlertEngine
from pipewatch.collector import MetricCollector
from pipewatch.metrics import PipelineMetric
from pipewatch.schedule import ScheduleConfig, Scheduler

HandlerFn = Callable[[Alert], None]


@dataclass
class PollerConfig:
    pipelines: List[str]
    schedule: ScheduleConfig = field(default_factory=ScheduleConfig)


class Poller:
    """Coordinates scheduled polling across multiple pipelines."""

    def __init__(
        self,
        collector: MetricCollector,
        engine: AlertEngine,
        handlers: list[HandlerFn],
        config: PollerConfig,
    ) -> None:
        self._collector = collector
        self._engine = engine
        self._handlers = handlers
        self._config = config
        self._schedulers: list[Scheduler] = []

    def _make_fn(self, pipeline: str) -> Callable[[], None]:
        def poll() -> None:
            metric: PipelineMetric | None = self._collector.latest(pipeline)
            if metric is None:
                return
            alerts = self._engine.evaluate(metric)
            for alert in alerts:
                for handler in self._handlers:
                    handler(alert)

        return poll

    def start(self) -> None:
        for pipeline in self._config.pipelines:
            s = Scheduler(self._make_fn(pipeline), self._config.schedule)
            s.start()
            self._schedulers.append(s)

    def stop(self) -> None:
        for s in self._schedulers:
            s.stop()
        self._schedulers.clear()

    @property
    def is_running(self) -> bool:
        return any(s.is_running for s in self._schedulers)
