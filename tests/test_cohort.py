"""Tests for pipewatch.cohort."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.cohort import CohortConfig, build_cohort
from pipewatch.metrics import PipelineMetric, PipelineStatus


def make_metric(
    name: str = "pipe",
    failed: int = 0,
    processed: int = 10,
    ts: datetime | None = None,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        processed=processed,
        failed=failed,
        timestamp=ts or datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        status=PipelineStatus.HEALTHY,
    )


class TestCohortConfig:
    def test_defaults(self) -> None:
        cfg = CohortConfig()
        assert cfg.bucket_minutes == 60
        assert cfg.min_cohort_size == 1

    def test_validate_passes(self) -> None:
        CohortConfig(bucket_minutes=30, min_cohort_size=2).validate()

    def test_validate_rejects_zero_bucket(self) -> None:
        with pytest.raises(ValueError, match="bucket_minutes"):
            CohortConfig(bucket_minutes=0).validate()

    def test_validate_rejects_zero_min_size(self) -> None:
        with pytest.raises(ValueError, match="min_cohort_size"):
            CohortConfig(min_cohort_size=0).validate()


class TestBuildCohort:
    def test_empty_metrics_returns_empty_result(self) -> None:
        result = build_cohort([])
        assert result.buckets == []

    def test_single_metric_creates_one_bucket(self) -> None:
        m = make_metric(ts=datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc))
        result = build_cohort([m])
        assert len(result.buckets) == 1
        assert result.buckets[0].count == 1

    def test_metrics_in_same_window_grouped(self) -> None:
        ts1 = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2024, 6, 1, 10, 30, 0, tzinfo=timezone.utc)
        metrics = [make_metric(ts=ts1), make_metric(ts=ts2)]
        result = build_cohort(metrics, CohortConfig(bucket_minutes=60))
        assert len(result.buckets) == 1
        assert result.buckets[0].count == 2

    def test_metrics_in_different_windows_split(self) -> None:
        ts1 = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2024, 6, 1, 11, 0, 0, tzinfo=timezone.utc)
        metrics = [make_metric(ts=ts1), make_metric(ts=ts2)]
        result = build_cohort(metrics, CohortConfig(bucket_minutes=60))
        assert len(result.buckets) == 2

    def test_avg_error_rate_computed(self) -> None:
        ts = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
        metrics = [
            make_metric(failed=2, processed=10, ts=ts),
            make_metric(failed=4, processed=10, ts=ts),
        ]
        result = build_cohort(metrics)
        assert result.buckets[0].avg_error_rate == pytest.approx(0.3)

    def test_min_cohort_size_filters_small_buckets(self) -> None:
        ts1 = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2024, 6, 1, 11, 0, 0, tzinfo=timezone.utc)
        metrics = [make_metric(ts=ts1), make_metric(ts=ts2)]
        result = build_cohort(metrics, CohortConfig(bucket_minutes=60, min_cohort_size=2))
        assert result.buckets == []

    def test_buckets_sorted_by_start(self) -> None:
        ts1 = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
        metrics = [make_metric(ts=ts1), make_metric(ts=ts2)]
        result = build_cohort(metrics)
        assert result.buckets[0].start < result.buckets[1].start

    def test_peak_bucket_returns_highest_error_rate(self) -> None:
        ts1 = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2024, 6, 1, 11, 0, 0, tzinfo=timezone.utc)
        metrics = [
            make_metric(failed=1, processed=10, ts=ts1),
            make_metric(failed=5, processed=10, ts=ts2),
        ]
        result = build_cohort(metrics)
        peak = result.peak_bucket()
        assert peak is not None
        assert peak.avg_error_rate == pytest.approx(0.5)

    def test_peak_bucket_none_for_empty(self) -> None:
        result = build_cohort([])
        assert result.peak_bucket() is None

    def test_to_dict_contains_expected_keys(self) -> None:
        m = make_metric(ts=datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc))
        result = build_cohort([m])
        d = result.to_dict()
        assert "bucket_minutes" in d
        assert "buckets" in d
        assert "label" in d["buckets"][0]
        assert "avg_error_rate" in d["buckets"][0]
