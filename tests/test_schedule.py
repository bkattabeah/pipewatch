"""Tests for pipewatch.schedule."""

from __future__ import annotations

import time

import pytest

from pipewatch.schedule import RunResult, ScheduleConfig, Scheduler


def test_schedule_config_defaults():
    cfg = ScheduleConfig()
    assert cfg.interval_seconds == 60.0
    assert cfg.max_runs is None
    assert cfg.stop_on_error is False


def test_run_result_fields():
    r = RunResult(run_number=1, success=True, elapsed=0.5)
    assert r.run_number == 1
    assert r.success is True
    assert r.error is None


def test_scheduler_runs_fn(tmp_path):
    calls = []

    def fn():
        calls.append(1)

    cfg = ScheduleConfig(interval_seconds=0.05, max_runs=3)
    s = Scheduler(fn, cfg)
    s.start()
    s._thread.join(timeout=2)
    assert len(calls) == 3
    assert all(r.success for r in s.results)


def test_scheduler_records_error():
    def bad():
        raise ValueError("boom")

    cfg = ScheduleConfig(interval_seconds=0.05, max_runs=2, stop_on_error=False)
    s = Scheduler(bad, cfg)
    s.start()
    s._thread.join(timeout=2)
    assert len(s.results) == 2
    assert all(not r.success for r in s.results)
    assert all(isinstance(r.error, ValueError) for r in s.results)


def test_scheduler_stop_on_error():
    def bad():
        raise RuntimeError("fail")

    cfg = ScheduleConfig(interval_seconds=0.05, max_runs=5, stop_on_error=True)
    s = Scheduler(bad, cfg)
    s.start()
    s._thread.join(timeout=2)
    assert len(s.results) == 1


def test_scheduler_stop_early():
    calls = []

    def fn():
        calls.append(1)
        time.sleep(0.1)

    cfg = ScheduleConfig(interval_seconds=0.5, max_runs=10)
    s = Scheduler(fn, cfg)
    s.start()
    time.sleep(0.15)
    s.stop()
    assert len(calls) >= 1
    assert not s.is_running


def test_is_running_false_before_start():
    s = Scheduler(lambda: None, ScheduleConfig())
    assert not s.is_running
