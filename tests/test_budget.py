"""Tests for pipewatch.budget."""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from pipewatch.budget import AlertBudget, BudgetConfig, BudgetStatus


def make_alert(name: str = "pipe-a") -> MagicMock:
    alert = MagicMock()
    alert.rule.name = name
    return alert


class TestBudgetConfig:
    def test_defaults(self):
        cfg = BudgetConfig()
        assert cfg.window_seconds == 3600
        assert cfg.max_alerts == 100

    def test_validate_passes(self):
        BudgetConfig(window_seconds=60, max_alerts=10).validate()

    def test_validate_rejects_zero_window(self):
        with pytest.raises(ValueError, match="window_seconds"):
            BudgetConfig(window_seconds=0).validate()

    def test_validate_rejects_zero_max(self):
        with pytest.raises(ValueError, match="max_alerts"):
            BudgetConfig(max_alerts=0).validate()


class TestAlertBudget:
    def setup_method(self):
        self.cfg = BudgetConfig(window_seconds=60, max_alerts=3)
        self.budget = AlertBudget(self.cfg)
        self.t0 = datetime(2024, 1, 1, 12, 0, 0)

    def test_initial_status_empty(self):
        s = self.budget.status(now=self.t0)
        assert s.used == 0
        assert s.remaining == 3
        assert not s.exhausted

    def test_first_alert_allowed(self):
        assert self.budget.is_allowed(make_alert(), now=self.t0)

    def test_allows_up_to_max(self):
        for _ in range(3):
            self.budget.record(make_alert(), now=self.t0)
        assert not self.budget.is_allowed(make_alert(), now=self.t0)

    def test_status_exhausted_after_max(self):
        for _ in range(3):
            self.budget.record(make_alert(), now=self.t0)
        s = self.budget.status(now=self.t0)
        assert s.exhausted
        assert s.remaining == 0
        assert s.used == 3

    def test_entries_pruned_after_window(self):
        for _ in range(3):
            self.budget.record(make_alert(), now=self.t0)
        future = self.t0 + timedelta(seconds=61)
        assert self.budget.is_allowed(make_alert(), now=future)

    def test_partial_prune(self):
        self.budget.record(make_alert(), now=self.t0)
        self.budget.record(make_alert(), now=self.t0 + timedelta(seconds=30))
        later = self.t0 + timedelta(seconds=61)
        # first entry pruned, second still live
        s = self.budget.status(now=later)
        assert s.used == 1

    def test_clear_resets_entries(self):
        for _ in range(3):
            self.budget.record(make_alert(), now=self.t0)
        self.budget.clear()
        assert self.budget.status(now=self.t0).used == 0

    def test_to_dict_keys(self):
        s = self.budget.status(now=self.t0)
        d = s.to_dict()
        assert set(d.keys()) == {"used", "remaining", "limit", "window_seconds", "exhausted"}

    def test_budget_entry_to_dict(self):
        self.budget.record(make_alert("my-pipe"), now=self.t0)
        entry = self.budget._entries[0]
        d = entry.to_dict()
        assert d["pipeline"] == "my-pipe"
        assert "fired_at" in d
