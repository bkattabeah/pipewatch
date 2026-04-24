"""Tests for pipewatch.scorer."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from pipewatch.alerts import Alert, AlertRule
from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.scorer import ScoredAlert, rank_alerts, score_alert


_NOW = 1_700_000_000.0


def make_metric(
    name: str = "pipe",
    status: PipelineStatus = PipelineStatus.CRITICAL,
    error_rate: float = 0.5,
    ts: float = _NOW,
) -> PipelineMetric:
    m = MagicMock(spec=PipelineMetric)
    m.pipeline_name = name
    m.status = status
    m.error_rate = error_rate
    m.timestamp = ts
    return m


def make_alert(metric: PipelineMetric, rule_name: str = "default") -> Alert:
    rule = MagicMock(spec=AlertRule)
    rule.name = rule_name
    return Alert(rule=rule, metric=metric)


class TestScoreAlert:
    def test_critical_scores_higher_than_warning(self):
        crit = make_alert(make_metric(status=PipelineStatus.CRITICAL, error_rate=0.0))
        warn = make_alert(make_metric(status=PipelineStatus.WARNING, error_rate=0.0))
        s_crit = score_alert(crit, _NOW)
        s_warn = score_alert(warn, _NOW)
        assert s_crit.score > s_warn.score

    def test_higher_error_rate_increases_score(self):
        low = make_alert(make_metric(status=PipelineStatus.WARNING, error_rate=0.1))
        high = make_alert(make_metric(status=PipelineStatus.WARNING, error_rate=0.9))
        assert score_alert(high, _NOW).score > score_alert(low, _NOW).score

    def test_recent_alert_scores_higher_than_old(self):
        recent = make_alert(make_metric(ts=_NOW - 1))
        old = make_alert(make_metric(ts=_NOW - 3600))
        assert score_alert(recent, _NOW).score > score_alert(old, _NOW).score

    def test_to_dict_contains_expected_keys(self):
        alert = make_alert(make_metric(name="etl", rule_name="high_err"))
        scored = score_alert(alert, _NOW)
        d = scored.to_dict()
        assert d["pipeline"] == "etl"
        assert d["rule"] == "high_err"
        assert "score" in d
        assert "status" in d

    def test_healthy_metric_has_low_score(self):
        alert = make_alert(make_metric(status=PipelineStatus.HEALTHY, error_rate=0.0))
        scored = score_alert(alert, _NOW)
        # Healthy base weight is 0; score comes only from recency
        assert scored.score < 60.0


class TestRankAlerts:
    def test_empty_list_returns_empty(self):
        assert rank_alerts([], _NOW) == []

    def test_sorted_descending_by_score(self):
        alerts = [
            make_alert(make_metric(status=PipelineStatus.HEALTHY, error_rate=0.0)),
            make_alert(make_metric(status=PipelineStatus.CRITICAL, error_rate=0.8)),
            make_alert(make_metric(status=PipelineStatus.WARNING, error_rate=0.3)),
        ]
        ranked = rank_alerts(alerts, _NOW)
        scores = [r.score for r in ranked]
        assert scores == sorted(scores, reverse=True)

    def test_returns_scored_alert_instances(self):
        alert = make_alert(make_metric())
        result = rank_alerts([alert], _NOW)
        assert len(result) == 1
        assert isinstance(result[0], ScoredAlert)
