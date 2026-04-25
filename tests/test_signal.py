"""Tests for pipewatch.signal."""
from __future__ import annotations

import pytest

from pipewatch.metrics import PipelineStatus
from pipewatch.signal import SignalConfig, SignalResult, detect_signals


def make_metric(pipeline: str, failed: int, total: int, status: PipelineStatus = PipelineStatus.HEALTHY):
    from pipewatch.metrics import PipelineMetric
    return PipelineMetric(pipeline=pipeline, failed=failed, total=total, status=status)


# ---------------------------------------------------------------------------
# SignalConfig validation
# ---------------------------------------------------------------------------

class TestSignalConfig:
    def test_defaults(self):
        cfg = SignalConfig()
        assert cfg.min_error_rate_spike == 0.10
        assert cfg.min_recovery_drop == 0.10
        assert cfg.min_samples == 2

    def test_validate_passes(self):
        SignalConfig(min_error_rate_spike=0.05, min_recovery_drop=0.05, min_samples=3).validate()

    def test_validate_rejects_zero_spike(self):
        with pytest.raises(ValueError, match="min_error_rate_spike"):
            SignalConfig(min_error_rate_spike=0.0).validate()

    def test_validate_rejects_zero_recovery(self):
        with pytest.raises(ValueError, match="min_recovery_drop"):
            SignalConfig(min_recovery_drop=0.0).validate()

    def test_validate_rejects_low_samples(self):
        with pytest.raises(ValueError, match="min_samples"):
            SignalConfig(min_samples=1).validate()


# ---------------------------------------------------------------------------
# detect_signals
# ---------------------------------------------------------------------------

class TestDetectSignals:
    def test_returns_empty_when_no_overlap(self):
        current = [make_metric("a", 5, 10)]
        previous = [make_metric("b", 5, 10)]
        assert detect_signals(current, previous) == []

    def test_stable_signal_when_no_change(self):
        m = make_metric("pipe", 1, 10)
        results = detect_signals([m], [m])
        assert len(results) == 1
        assert results[0].signal == "stable"

    def test_spike_detected(self):
        prev = make_metric("pipe", 0, 10)
        curr = make_metric("pipe", 5, 10)  # 50% error rate
        results = detect_signals([curr], [prev])
        assert results[0].signal == "spike"
        assert results[0].delta == pytest.approx(0.5)

    def test_recovery_detected(self):
        prev = make_metric("pipe", 8, 10)  # 80%
        curr = make_metric("pipe", 1, 10)  # 10%
        results = detect_signals([curr], [prev])
        assert results[0].signal == "recovery"
        assert results[0].delta == pytest.approx(-0.7)

    def test_flapping_detected_on_status_change(self):
        prev = make_metric("pipe", 1, 10, PipelineStatus.HEALTHY)
        curr = make_metric("pipe", 2, 10, PipelineStatus.WARNING)  # delta=0.1, boundary
        cfg = SignalConfig(min_error_rate_spike=0.20)  # raise threshold so spike not triggered
        results = detect_signals([curr], [prev], cfg)
        assert results[0].signal == "flapping"

    def test_to_dict_keys(self):
        prev = make_metric("pipe", 0, 10)
        curr = make_metric("pipe", 3, 10)
        r = detect_signals([curr], [prev])[0]
        d = r.to_dict()
        assert set(d.keys()) == {"pipeline", "signal", "current_error_rate", "previous_error_rate", "delta", "note"}

    def test_multiple_pipelines(self):
        current = [make_metric("a", 5, 10), make_metric("b", 0, 10)]
        previous = [make_metric("a", 0, 10), make_metric("b", 0, 10)]
        results = detect_signals(current, previous)
        signals = {r.pipeline: r.signal for r in results}
        assert signals["a"] == "spike"
        assert signals["b"] == "stable"

    def test_zero_total_handled_gracefully(self):
        prev = make_metric("pipe", 0, 0)
        curr = make_metric("pipe", 0, 0)
        results = detect_signals([curr], [prev])
        assert results[0].current_error_rate == 0.0
