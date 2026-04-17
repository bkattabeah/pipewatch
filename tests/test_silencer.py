"""Tests for pipewatch.silencer."""
import pytest
from datetime import datetime, timedelta
from pipewatch.silencer import SilenceRule, Silencer


def make_rule(pipeline="etl_main", offset_start=-5, offset_end=60, reason="test"):
    now = datetime.utcnow()
    return SilenceRule(
        pipeline=pipeline,
        reason=reason,
        start=now + timedelta(minutes=offset_start),
        end=now + timedelta(minutes=offset_end),
    )


class TestSilenceRule:
    def test_active_within_window(self):
        rule = make_rule()
        assert rule.is_active() is True

    def test_inactive_before_start(self):
        now = datetime.utcnow()
        rule = SilenceRule("p", "r", now + timedelta(hours=1), now + timedelta(hours=2))
        assert rule.is_active() is False

    def test_inactive_after_end(self):
        now = datetime.utcnow()
        rule = SilenceRule("p", "r", now - timedelta(hours=2), now - timedelta(hours=1))
        assert rule.is_active() is False

    def test_to_dict_keys(self):
        rule = make_rule()
        d = rule.to_dict()
        assert set(d.keys()) == {"pipeline", "reason", "start", "end", "created_by"}


class TestSilencer:
    def setup_method(self):
        self.silencer = Silencer()

    def test_empty_not_silenced(self):
        assert self.silencer.is_silenced("etl_main") is False

    def test_add_and_check(self):
        self.silencer.add(make_rule("etl_main"))
        assert self.silencer.is_silenced("etl_main") is True

    def test_other_pipeline_not_silenced(self):
        self.silencer.add(make_rule("etl_main"))
        assert self.silencer.is_silenced("etl_other") is False

    def test_remove_rule(self):
        self.silencer.add(make_rule("etl_main"))
        removed = self.silencer.remove("etl_main")
        assert removed == 1
        assert self.silencer.is_silenced("etl_main") is False

    def test_remove_nonexistent_returns_zero(self):
        assert self.silencer.remove("ghost") == 0

    def test_active_rules_excludes_expired(self):
        now = datetime.utcnow()
        expired = SilenceRule("old", "r", now - timedelta(hours=2), now - timedelta(hours=1))
        active = make_rule("new")
        self.silencer.add(expired)
        self.silencer.add(active)
        names = [r.pipeline for r in self.silencer.active_rules()]
        assert "new" in names
        assert "old" not in names

    def test_clear_expired(self):
        now = datetime.utcnow()
        expired = SilenceRule("old", "r", now - timedelta(hours=2), now - timedelta(hours=1))
        self.silencer.add(expired)
        self.silencer.add(make_rule("active"))
        cleared = self.silencer.clear_expired()
        assert cleared == 1
        assert len(self.silencer.all_rules()) == 1
