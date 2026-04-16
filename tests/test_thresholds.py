"""Tests for pipewatch.thresholds module."""

import json
import os
import pytest
from pipewatch.thresholds import (
    ThresholdConfig,
    ThresholdRegistry,
    load_thresholds,
    save_thresholds,
)


class TestThresholdConfig:
    def test_defaults(self):
        cfg = ThresholdConfig()
        assert cfg.warning_error_rate == 0.05
        assert cfg.critical_error_rate == 0.20

    def test_validate_passes(self):
        cfg = ThresholdConfig(warning_error_rate=0.05, critical_error_rate=0.15)
        cfg.validate()  # should not raise

    def test_validate_error_rate_out_of_range(self):
        cfg = ThresholdConfig(warning_error_rate=-0.1)
        with pytest.raises(ValueError, match="warning_error_rate"):
            cfg.validate()

    def test_validate_warning_exceeds_critical(self):
        cfg = ThresholdConfig(warning_error_rate=0.3, critical_error_rate=0.1)
        with pytest.raises(ValueError, match="must not exceed"):
            cfg.validate()

    def test_validate_latency_warning_exceeds_critical(self):
        cfg = ThresholdConfig(warning_latency_ms=9000, critical_latency_ms=1000)
        with pytest.raises(ValueError, match="warning_latency_ms"):
            cfg.validate()


class TestThresholdRegistry:
    def test_get_returns_defaults_for_unknown_pipeline(self):
        registry = ThresholdRegistry()
        cfg = registry.get("unknown_pipeline")
        assert cfg is registry.defaults

    def test_set_and_get_pipeline_config(self):
        registry = ThresholdRegistry()
        custom = ThresholdConfig(warning_error_rate=0.10, critical_error_rate=0.30)
        registry.set("my_pipeline", custom)
        assert registry.get("my_pipeline") is custom

    def test_set_validates_config(self):
        registry = ThresholdRegistry()
        bad = ThresholdConfig(warning_error_rate=0.9, critical_error_rate=0.1)
        with pytest.raises(ValueError):
            registry.set("pipe", bad)


class TestLoadSaveThresholds:
    def test_load_missing_file_returns_defaults(self, tmp_path):
        registry = load_thresholds(str(tmp_path / "nonexistent.json"))
        assert isinstance(registry, ThresholdRegistry)
        assert registry.defaults.warning_error_rate == 0.05

    def test_round_trip(self, tmp_path):
        path = str(tmp_path / "thresholds.json")
        registry = ThresholdRegistry()
        registry.set("etl_main", ThresholdConfig(warning_error_rate=0.08, critical_error_rate=0.25))
        save_thresholds(registry, path)

        loaded = load_thresholds(path)
        assert loaded.get("etl_main").warning_error_rate == 0.08
        assert loaded.get("etl_main").critical_error_rate == 0.25

    def test_load_applies_defaults_section(self, tmp_path):
        path = str(tmp_path / "thresholds.json")
        data = {"defaults": {"warning_error_rate": 0.02, "critical_error_rate": 0.10,
                             "warning_latency_ms": 500, "critical_latency_ms": 2000,
                             "min_throughput": 1.0}, "pipelines": {}}
        with open(path, "w") as f:
            json.dump(data, f)

        loaded = load_thresholds(path)
        assert loaded.defaults.warning_error_rate == 0.02
        assert loaded.defaults.min_throughput == 1.0
