"""Tests for pipewatch.watchdog."""
from datetime import datetime, timedelta

import pytest

from pipewatch.collector import MetricCollector
from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.watchdog import Watchdog, WatchdogConfig


def make_metric(pipeline_id: str, timestamp: datetime) -> PipelineMetric:
    return PipelineMetric(
        pipeline_id=pipeline_id,
        timestamp=timestamp,
        records_processed=100,
        records_failed=0,
        duration_seconds=1.0,
        status=PipelineStatus.HEALTHY,
    )


NOW = datetime(2024, 1, 1, 12, 0, 0)


@pytest.fixture
def collector():
    return MetricCollector()


def test_no_stale_when_recent(collector):
    m = make_metric("pipe_a", NOW - timedelta(seconds=10))
    collector.record(m)
    wd = Watchdog(collector, WatchdogConfig(stale_threshold_seconds=60, pipelines=["pipe_a"]))
    assert wd.check(now=NOW) == []


def test_stale_when_old(collector):
    m = make_metric("pipe_a", NOW - timedelta(seconds=120))
    collector.record(m)
    wd = Watchdog(collector, WatchdogConfig(stale_threshold_seconds=60, pipelines=["pipe_a"]))
    result = wd.check(now=NOW)
    assert len(result) == 1
    assert result[0].pipeline_id == "pipe_a"
    assert result[0].stale_for_seconds == pytest.approx(120.0)


def test_missing_pipeline_reported_stale(collector):
    wd = Watchdog(collector, WatchdogConfig(stale_threshold_seconds=60, pipelines=["missing"]))
    result = wd.check(now=NOW)
    assert len(result) == 1
    assert result[0].pipeline_id == "missing"


def test_is_healthy_true_when_no_stale(collector):
    m = make_metric("pipe_b", NOW - timedelta(seconds=5))
    collector.record(m)
    wd = Watchdog(collector, WatchdogConfig(stale_threshold_seconds=60, pipelines=["pipe_b"]))
    assert wd.is_healthy(now=NOW) is True


def test_is_healthy_false_when_stale(collector):
    m = make_metric("pipe_b", NOW - timedelta(seconds=200))
    collector.record(m)
    wd = Watchdog(collector, WatchdogConfig(stale_threshold_seconds=60, pipelines=["pipe_b"]))
    assert wd.is_healthy(now=NOW) is False


def test_to_dict_format(collector):
    m = make_metric("pipe_c", NOW - timedelta(seconds=90))
    collector.record(m)
    wd = Watchdog(collector, WatchdogConfig(stale_threshold_seconds=60, pipelines=["pipe_c"]))
    entries = wd.check(now=NOW)
    d = entries[0].to_dict()
    assert d["pipeline_id"] == "pipe_c"
    assert "last_seen" in d
    assert d["stale_for_seconds"] == pytest.approx(90.0)


def test_auto_discovers_pipelines(collector):
    collector.record(make_metric("auto_a", NOW - timedelta(seconds=5)))
    collector.record(make_metric("auto_b", NOW - timedelta(seconds=200)))
    wd = Watchdog(collector, WatchdogConfig(stale_threshold_seconds=60))
    stale_ids = {e.pipeline_id for e in wd.check(now=NOW)}
    assert "auto_b" in stale_ids
    assert "auto_a" not in stale_ids
