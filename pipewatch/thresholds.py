"""Threshold configuration for pipeline alert rules."""

from dataclasses import dataclass, field
from typing import Dict, Optional
import json
import os


@dataclass
class ThresholdConfig:
    """Threshold values for a single pipeline or global defaults."""
    warning_error_rate: float = 0.05
    critical_error_rate: float = 0.20
    warning_latency_ms: float = 1000.0
    critical_latency_ms: float = 5000.0
    min_throughput: float = 0.0

    def validate(self) -> None:
        if not (0.0 <= self.warning_error_rate <= 1.0):
            raise ValueError("warning_error_rate must be between 0 and 1")
        if not (0.0 <= self.critical_error_rate <= 1.0):
            raise ValueError("critical_error_rate must be between 0 and 1")
        if self.warning_error_rate > self.critical_error_rate:
            raise ValueError("warning_error_rate must not exceed critical_error_rate")
        if self.warning_latency_ms > self.critical_latency_ms:
            raise ValueError("warning_latency_ms must not exceed critical_latency_ms")


@dataclass
class ThresholdRegistry:
    """Registry of threshold configs per pipeline with global defaults."""
    defaults: ThresholdConfig = field(default_factory=ThresholdConfig)
    pipelines: Dict[str, ThresholdConfig] = field(default_factory=dict)

    def get(self, pipeline_name: str) -> ThresholdConfig:
        return self.pipelines.get(pipeline_name, self.defaults)

    def set(self, pipeline_name: str, config: ThresholdConfig) -> None:
        config.validate()
        self.pipelines[pipeline_name] = config


def load_thresholds(path: str) -> ThresholdRegistry:
    """Load threshold registry from a JSON config file."""
    if not os.path.exists(path):
        return ThresholdRegistry()

    with open(path, "r") as f:
        data = json.load(f)

    defaults_data = data.get("defaults", {})
    defaults = ThresholdConfig(**defaults_data)
    defaults.validate()

    registry = ThresholdRegistry(defaults=defaults)
    for name, cfg in data.get("pipelines", {}).items():
        tc = ThresholdConfig(**cfg)
        registry.set(name, tc)

    return registry


def save_thresholds(registry: ThresholdRegistry, path: str) -> None:
    """Persist threshold registry to a JSON config file."""
    data = {
        "defaults": registry.defaults.__dict__,
        "pipelines": {name: cfg.__dict__ for name, cfg in registry.pipelines.items()},
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
