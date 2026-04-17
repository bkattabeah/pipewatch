"""Normalize raw pipeline metric dicts into PipelineMetric objects."""

from typing import Any
from pipewatch.metrics import PipelineMetric, evaluate_status


class NormalizationError(ValueError):
    """Raised when a raw metric dict cannot be normalized."""


REQUIRED_FIELDS = ("pipeline_id", "total_runs", "failed_runs", "duration_seconds")


def _coerce_int(value: Any, field: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        raise NormalizationError(f"Field '{field}' must be an integer, got {value!r}")


def _coerce_float(value: Any, field: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        raise NormalizationError(f"Field '{field}' must be a float, got {value!r}")


def normalize(raw: dict) -> PipelineMetric:
    """Convert a raw dict to a PipelineMetric, coercing types where possible."""
    if not isinstance(raw, dict):
        raise NormalizationError(f"Expected a dict, got {type(raw).__name__}")

    missing = [f for f in REQUIRED_FIELDS if f not in raw]
    if missing:
        raise NormalizationError(f"Missing required fields: {missing}")

    pipeline_id = str(raw["pipeline_id"]).strip()
    if not pipeline_id:
        raise NormalizationError("Field 'pipeline_id' must not be empty")

    total_runs = _coerce_int(raw["total_runs"], "total_runs")
    failed_runs = _coerce_int(raw["failed_runs"], "failed_runs")
    duration_seconds = _coerce_float(raw["duration_seconds"], "duration_seconds")

    if total_runs < 0:
        raise NormalizationError("'total_runs' must be >= 0")
    if failed_runs < 0:
        raise NormalizationError("'failed_runs' must be >= 0")
    if failed_runs > total_runs:
        raise NormalizationError("'failed_runs' cannot exceed 'total_runs'")

    metric = PipelineMetric(
        pipeline_id=pipeline_id,
        total_runs=total_runs,
        failed_runs=failed_runs,
        duration_seconds=duration_seconds,
    )
    metric.status = evaluate_status(metric)
    return metric


def normalize_many(raws: list) -> tuple[list, list]:
    """Normalize a list of raw dicts. Returns (metrics, errors) tuple."""
    metrics, errors = [], []
    for i, raw in enumerate(raws):
        try:
            metrics.append(normalize(raw))
        except NormalizationError as exc:
            errors.append({"index": i, "error": str(exc), "raw": raw})
    return metrics, errors
