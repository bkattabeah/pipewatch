"""Tests for pipewatch.circuit_breaker."""

from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from pipewatch.circuit_breaker import (
    AlertCircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
)


def make_breaker(**kwargs) -> AlertCircuitBreaker:
    config = CircuitBreakerConfig(**kwargs)
    return AlertCircuitBreaker(config=config)


class TestCircuitBreakerConfig:
    def test_defaults(self):
        cfg = CircuitBreakerConfig()
        assert cfg.failure_threshold == 5
        assert cfg.recovery_timeout == 60
        assert cfg.success_threshold == 2

    def test_validate_passes(self):
        CircuitBreakerConfig(failure_threshold=3, recovery_timeout=30, success_threshold=1).validate()

    def test_validate_rejects_zero_failure_threshold(self):
        with pytest.raises(ValueError, match="failure_threshold"):
            CircuitBreakerConfig(failure_threshold=0).validate()

    def test_validate_rejects_zero_recovery_timeout(self):
        with pytest.raises(ValueError, match="recovery_timeout"):
            CircuitBreakerConfig(recovery_timeout=0).validate()

    def test_validate_rejects_zero_success_threshold(self):
        with pytest.raises(ValueError, match="success_threshold"):
            CircuitBreakerConfig(success_threshold=0).validate()


class TestAlertCircuitBreaker:
    def setup_method(self):
        self.breaker = make_breaker(failure_threshold=3, recovery_timeout=30, success_threshold=2)

    def test_new_pipeline_is_allowed(self):
        assert self.breaker.is_allowed("pipe-a") is True

    def test_opens_after_threshold_failures(self):
        for _ in range(3):
            self.breaker.record_failure("pipe-a")
        assert not self.breaker.is_allowed("pipe-a")

    def test_state_is_open_after_threshold(self):
        for _ in range(3):
            self.breaker.record_failure("pipe-a")
        entry = self.breaker._get("pipe-a")
        assert entry.state == CircuitState.OPEN

    def test_below_threshold_stays_closed(self):
        for _ in range(2):
            self.breaker.record_failure("pipe-a")
        assert self.breaker.is_allowed("pipe-a")

    def test_transitions_to_half_open_after_timeout(self):
        for _ in range(3):
            self.breaker.record_failure("pipe-a")
        future = datetime.utcnow() + timedelta(seconds=31)
        with patch("pipewatch.circuit_breaker.datetime") as mock_dt:
            mock_dt.utcnow.return_value = future
            assert self.breaker.is_allowed("pipe-a")
            entry = self.breaker._get("pipe-a")
            assert entry.state == CircuitState.HALF_OPEN

    def test_closes_after_enough_successes_in_half_open(self):
        for _ in range(3):
            self.breaker.record_failure("pipe-a")
        entry = self.breaker._get("pipe-a")
        entry.state = CircuitState.HALF_OPEN
        self.breaker.record_success("pipe-a")
        self.breaker.record_success("pipe-a")
        assert entry.state == CircuitState.CLOSED

    def test_reopens_on_failure_in_half_open(self):
        for _ in range(3):
            self.breaker.record_failure("pipe-a")
        entry = self.breaker._get("pipe-a")
        entry.state = CircuitState.HALF_OPEN
        self.breaker.record_failure("pipe-a")
        assert entry.state == CircuitState.OPEN

    def test_reset_removes_entry(self):
        self.breaker.record_failure("pipe-a")
        self.breaker.reset("pipe-a")
        assert "pipe-a" not in self.breaker._entries

    def test_status_returns_dict(self):
        self.breaker.record_failure("pipe-a")
        s = self.breaker.status()
        assert "pipe-a" in s
        assert s["pipe-a"]["state"] == CircuitState.CLOSED.value

    def test_to_dict_keys(self):
        self.breaker.record_failure("pipe-a")
        d = self.breaker._get("pipe-a").to_dict()
        assert set(d.keys()) == {"pipeline", "state", "failure_count", "success_count", "opened_at", "last_updated"}
