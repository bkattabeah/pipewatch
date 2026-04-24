"""Tests for pipewatch.sampler."""

from datetime import datetime

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.sampler import MetricSampler, SampleEntry, SamplerConfig


def make_metric(
    name: str = "pipeline_a",
    total: int = 100,
    failed: int = 5,
    status: PipelineStatus = PipelineStatus.HEALTHY,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        total_records=total,
        failed_records=failed,
        status=status,
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
    )


class TestSamplerConfig:
    def test_defaults(self):
        cfg = SamplerConfig()
        assert cfg.max_samples == 100
        assert cfg.pipelines is None

    def test_validate_passes(self):
        SamplerConfig(max_samples=1).validate()  # should not raise

    def test_validate_rejects_zero(self):
        with pytest.raises(ValueError, match="max_samples"):
            SamplerConfig(max_samples=0).validate()


class TestMetricSampler:
    def setup_method(self):
        self.sampler = MetricSampler()

    def test_sample_returns_entry(self):
        metric = make_metric()
        entry = self.sampler.sample(metric)
        assert isinstance(entry, SampleEntry)
        assert entry.pipeline_name == "pipeline_a"
        assert entry.total_records == 100
        assert entry.failed_records == 5

    def test_error_rate_stored_correctly(self):
        metric = make_metric(total=200, failed=20)
        entry = self.sampler.sample(metric)
        assert entry.error_rate == pytest.approx(0.1)

    def test_len_increases_with_samples(self):
        self.sampler.sample(make_metric())
        self.sampler.sample(make_metric())
        assert len(self.sampler) == 2

    def test_get_samples_all(self):
        self.sampler.sample(make_metric(name="a"))
        self.sampler.sample(make_metric(name="b"))
        assert len(self.sampler.get_samples()) == 2

    def test_get_samples_filtered_by_name(self):
        self.sampler.sample(make_metric(name="a"))
        self.sampler.sample(make_metric(name="b"))
        result = self.sampler.get_samples(pipeline_name="a")
        assert len(result) == 1
        assert result[0].pipeline_name == "a"

    def test_max_samples_enforced(self):
        sampler = MetricSampler(SamplerConfig(max_samples=3))
        for _ in range(5):
            sampler.sample(make_metric())
        assert len(sampler) == 3

    def test_clear_removes_all_samples(self):
        self.sampler.sample(make_metric())
        self.sampler.clear()
        assert len(self.sampler) == 0

    def test_pipeline_filter_rejects_unlisted(self):
        sampler = MetricSampler(SamplerConfig(pipelines=["allowed"]))
        with pytest.raises(ValueError, match="not in the sampler's watch list"):
            sampler.sample(make_metric(name="other"))

    def test_pipeline_filter_accepts_listed(self):
        sampler = MetricSampler(SamplerConfig(pipelines=["allowed"]))
        entry = sampler.sample(make_metric(name="allowed"))
        assert entry.pipeline_name == "allowed"

    def test_to_dict_has_expected_keys(self):
        metric = make_metric()
        entry = self.sampler.sample(metric)
        d = entry.to_dict()
        assert set(d.keys()) == {
            "pipeline_name",
            "sampled_at",
            "error_rate",
            "total_records",
            "failed_records",
        }
