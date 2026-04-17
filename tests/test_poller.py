"""Tests for pipewatch.poller."""

from __future__ import annotations

import time
from datetime import datetime, timezone

import pytest

from pipewatch.alerts import Alert, AlertEngine, AlertRule
from pipewatch.collector import MetricCollector
from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.poller import Poller, PollerConfig
from pipewatch.schedule import ScheduleConfig
from pipewatch.thresholds import ThresholdConfig


def make_metric(name: str, failures: int = 0, total: int = 10) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        timestamp=datetime.now(timezone.utc),
        total_records=total,
        failed_records=failures,
        duration_seconds=1.0,
    )


def make_engine() -> AlertEngine:
    rule = AlertRule(
        pipeline_name="pipe1",
        threshold=ThresholdConfig(warning_error_rate=0.1, critical_error_rate=0.5),
    )
    return AlertEngine(rules=[rule])


def test_poller_starts_and_stops():
    collector = MetricCollector()
    collector.record(make_metric("pipe1"))
    engine = make_engine()
    cfg = PollerConfig(
        pipelines=["pipe1"],
        schedule=ScheduleConfig(interval_seconds=0.1, max_runs=2),
    )
    poller = Poller(collector, engine, [], cfg)
    poller.start()
    time.sleep(0.5)
    poller.stop()
    assert not poller.is_running


def test_poller_invokes_handlers():
    collector = MetricCollector()
    collector.record(make_metric("pipe1", failures=8, total=10))
    engine = make_engine()
    received: list[Alert] = []
    cfg = PollerConfig(
        pipelines=["pipe1"],
        schedule=ScheduleConfig(interval_seconds=0.05, max_runs=1),
    )
    poller = Poller(collector, engine, [received.append], cfg)
    poller.start()
    time.sleep(0.4)
    poller.stop()
    assert len(received) >= 1


def test_poller_skips_missing_pipeline():
    collector = MetricCollector()
    engine = make_engine()
    received: list = []
    cfg = PollerConfig(
        pipelines=["nonexistent"],
        schedule=ScheduleConfig(interval_seconds=0.05, max_runs=2),
    )
    poller = Poller(collector, engine, [received.append], cfg)
    poller.start()
    time.sleep(0.4)
    poller.stop()
    assert received == []
