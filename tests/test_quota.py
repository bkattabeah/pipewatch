import pytest
from datetime import datetime, timedelta
from pipewatch.quota import AlertQuotaManager, QuotaConfig


def make_manager(**kwargs) -> AlertQuotaManager:
    return AlertQuotaManager(QuotaConfig(**kwargs))


class TestQuotaConfig:
    def test_defaults(self):
        cfg = QuotaConfig()
        assert cfg.max_critical_per_hour == 20
        assert cfg.max_warning_per_hour == 50
        assert cfg.max_total_per_hour == 100

    def test_validate_passes(self):
        QuotaConfig().validate()

    def test_validate_rejects_zero(self):
        with pytest.raises(ValueError):
            QuotaConfig(max_total_per_hour=0).validate()

    def test_validate_critical_exceeds_total(self):
        with pytest.raises(ValueError):
            QuotaConfig(max_critical_per_hour=200, max_total_per_hour=10).validate()


class TestAlertQuotaManager:
    def setup_method(self):
        self.mgr = make_manager(max_critical_per_hour=3, max_warning_per_hour=5, max_total_per_hour=10)
        self.t0 = datetime(2024, 1, 1, 12, 0, 0)

    def test_first_alert_allowed(self):
        assert self.mgr.is_allowed("pipe_a", "critical", now=self.t0) is True

    def test_record_increments_count(self):
        self.mgr.record("pipe_a", "critical", now=self.t0)
        s = self.mgr.status()
        assert s["total"] == 1

    def test_blocks_after_critical_limit(self):
        for _ in range(3):
            self.mgr.record("pipe_a", "critical", now=self.t0)
        assert self.mgr.is_allowed("pipe_a", "critical", now=self.t0) is False

    def test_warning_independent_of_critical(self):
        for _ in range(3):
            self.mgr.record("pipe_a", "critical", now=self.t0)
        assert self.mgr.is_allowed("pipe_a", "warning", now=self.t0) is True

    def test_blocks_at_total_limit(self):
        for i in range(10):
            self.mgr.record(f"pipe_{i}", "warning", now=self.t0)
        assert self.mgr.is_allowed("pipe_x", "warning", now=self.t0) is False

    def test_window_resets_after_hour(self):
        for _ in range(3):
            self.mgr.record("pipe_a", "critical", now=self.t0)
        future = self.t0 + timedelta(hours=1, seconds=1)
        assert self.mgr.is_allowed("pipe_a", "critical", now=future) is True

    def test_clear_resets_all(self):
        self.mgr.record("pipe_a", "critical", now=self.t0)
        self.mgr.clear()
        assert self.mgr.status()["total"] == 0
        assert self.mgr.status()["entries"] == []

    def test_status_contains_entry(self):
        self.mgr.record("pipe_a", "warning", now=self.t0)
        entries = self.mgr.status()["entries"]
        assert len(entries) == 1
        assert entries[0]["pipeline"] == "pipe_a"
        assert entries[0]["severity"] == "warning"
        assert entries[0]["count"] == 1
