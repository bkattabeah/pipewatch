"""Routes alerts through silence checks before dispatching to handlers."""
from typing import Callable, List
from pipewatch.alerts import Alert
from pipewatch.silencer import Silencer

HandlerFn = Callable[[Alert], None]


class AlertRouter:
    """Wraps handlers with silence-aware routing."""

    def __init__(self, silencer: Silencer, handlers: List[HandlerFn] = None):
        self.silencer = silencer
        self.handlers: List[HandlerFn] = handlers or []
        self._suppressed: List[Alert] = []

    def add_handler(self, fn: HandlerFn) -> None:
        self.handlers.append(fn)

    def route(self, alert: Alert) -> bool:
        """Dispatch alert unless pipeline is silenced. Returns True if dispatched."""
        if self.silencer.is_silenced(alert.pipeline):
            self._suppressed.append(alert)
            return False
        for handler in self.handlers:
            handler(alert)
        return True

    def route_many(self, alerts: List[Alert]) -> dict:
        dispatched, suppressed = 0, 0
        for alert in alerts:
            if self.route(alert):
                dispatched += 1
            else:
                suppressed += 1
        return {"dispatched": dispatched, "suppressed": suppressed}

    def suppressed_alerts(self) -> List[Alert]:
        return list(self._suppressed)

    def clear_suppressed(self) -> None:
        self._suppressed.clear()
