"""Scheduled polling support for pipewatch metrics collection."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class ScheduleConfig:
    interval_seconds: float = 60.0
    max_runs: Optional[int] = None
    stop_on_error: bool = False


@dataclass
class RunResult:
    run_number: int
    success: bool
    error: Optional[Exception] = None
    elapsed: float = 0.0


class Scheduler:
    """Runs a callable on a fixed interval in a background thread."""

    def __init__(self, fn: Callable[[], None], config: ScheduleConfig) -> None:
        self._fn = fn
        self._config = config
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self.results: list[RunResult] = []

    def start(self) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=self._config.interval_seconds + 2)

    def _loop(self) -> None:
        run = 0
        while not self._stop_event.is_set():
            if self._config.max_runs is not None and run >= self._config.max_runs:
                break
            t0 = time.monotonic()
            error = None
            try:
                self._fn()
                success = True
            except Exception as exc:  # noqa: BLE001
                success = False
                error = exc
            elapsed = time.monotonic() - t0
            run += 1
            self.results.append(RunResult(run, success, error, elapsed))
            if not success and self._config.stop_on_error:
                break
            self._stop_event.wait(self._config.interval_seconds)

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()
