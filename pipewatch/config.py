"""Central configuration loader for pipewatch CLI."""

import os
from dataclasses import dataclass, field
from typing import Optional

from pipewatch.thresholds import ThresholdRegistry, load_thresholds

DEFAULT_CONFIG_PATH = os.path.expanduser("~/.pipewatch/thresholds.json")
ENV_CONFIG_PATH = "PIPEWATCH_THRESHOLDS"


@dataclass
class PipewatchConfig:
    """Runtime configuration for pipewatch."""
    thresholds: ThresholdRegistry = field(default_factory=ThresholdRegistry)
    history_limit: int = 100
    log_level: str = "INFO"
    output_format: str = "console"  # console | json | logging


def resolve_config_path(override: Optional[str] = None) -> str:
    """Resolve threshold config path from override, env var, or default."""
    if override:
        return override
    return os.environ.get(ENV_CONFIG_PATH, DEFAULT_CONFIG_PATH)


def load_config(
    config_path: Optional[str] = None,
    history_limit: int = 100,
    log_level: str = "INFO",
    output_format: str = "console",
) -> PipewatchConfig:
    """Load and return a PipewatchConfig, reading thresholds from disk if available."""
    path = resolve_config_path(config_path)
    thresholds = load_thresholds(path)
    return PipewatchConfig(
        thresholds=thresholds,
        history_limit=history_limit,
        log_level=log_level,
        output_format=output_format,
    )


def config_summary(config: PipewatchConfig) -> dict:
    """Return a serialisable summary of the active configuration."""
    defaults = config.thresholds.defaults
    return {
        "history_limit": config.history_limit,
        "log_level": config.log_level,
        "output_format": config.output_format,
        "default_thresholds": {
            "warning_error_rate": defaults.warning_error_rate,
            "critical_error_rate": defaults.critical_error_rate,
            "warning_latency_ms": defaults.warning_latency_ms,
            "critical_latency_ms": defaults.critical_latency_ms,
            "min_throughput": defaults.min_throughput,
        },
        "pipeline_overrides": list(config.thresholds.pipelines.keys()),
    }
