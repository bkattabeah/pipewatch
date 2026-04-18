"""Tests for AlertRateLimiter."""
from datetime import datetime, timedelta
import pytest
from pipewatch.rate_limiter import AlertRateLimiter, RateLimiterConfig, RateLimitEntry


def make_limiter(window_seconds=60, max_alerts=3) -> AlertRateLimiter:
    return AlertRateLimiter(RateLimiterConfig(window_seconds=window_seconds, max_alerts=max_alerts))


class TestAlertRateLimiter:
    def test_first_alert_always_allowed(self):
        limiter = make_limiter()
        assert limiter.is_allowed("pipe_a", "high_error_rate") is True

    def test_allows_up_to_max(self):
        limiter = make_limiter(max_alerts=3)
        for _ in range(3):
            assert limiter.is_allowed("pipe_a", "rule") is True

    def test_blocks_beyond_max(self):
        limiter = make_limiter(max_alerts=2)
        limiter.is_allowed("pipe_a", "rule")
        limiter.is_allowed("pipe_a", "rule")
        assert limiter.is_allowed("pipe_a", "rule") is False

    def test_different_pipelines_tracked_independently(self):
        limiter = make_limiter(max_alerts=1)
        limiter.is_allowed("pipe_a", "rule")
        assert limiter.is_allowed("pipe_b", "rule") is True

    def test_different_rules_tracked_independently(self):
        limiter = make_limiter(max_alerts=1)
        limiter.is_allowed("pipe_a", "rule_x")
        assert limiter.is_allowed("pipe_a", "rule_y") is True

    def test_window_reset_allows_new_alerts(self):
        limiter = make_limiter(window_seconds=10, max_alerts=1)
        limiter.is_allowed("pipe_a", "rule")
        # Manually expire the window
        key = "pipe_a::rule"
        limiter._entries[key].window_start = datetime.utcnow() - timedelta(seconds=20)
        assert limiter.is_allowed("pipe_a", "rule") is True

    def test_reset_clears_specific_entry(self):
        limiter = make_limiter(max_alerts=1)
        limiter.is_allowed("pipe_a", "rule")
        assert limiter.is_allowed("pipe_a", "rule") is False
        limiter.reset("pipe_a", "rule")
        assert limiter.is_allowed("pipe_a", "rule") is True

    def test_reset_all_clears_everything(self):
        limiter = make_limiter(max_alerts=1)
        limiter.is_allowed("pipe_a", "rule")
        limiter.is_allowed("pipe_b", "rule")
        limiter.reset_all()
        assert limiter.status() == []

    def test_status_returns_entries(self):
        limiter = make_limiter()
        limiter.is_allowed("pipe_a", "rule")
        status = limiter.status()
        assert len(status) == 1
        assert status[0]["pipeline"] == "pipe_a"
        assert status[0]["rule_name"] == "rule"
        assert status[0]["count"] == 1


def test_config_validates_window():
    config = RateLimiterConfig(window_seconds=0)
    with pytest.raises(ValueError, match="window_seconds"):
        config.validate()


def test_config_validates_max_alerts():
    config = RateLimiterConfig(max_alerts=0)
    with pytest.raises(ValueError, match="max_alerts"):
        config.validate()
