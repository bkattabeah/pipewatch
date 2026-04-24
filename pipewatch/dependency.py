"""Pipeline dependency tracking and health propagation.

Allows defining upstream/downstream relationships between pipelines
so that failures can be traced through the dependency graph.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

from pipewatch.metrics import PipelineMetric, PipelineStatus


@dataclass
class DependencyConfig:
    """Configuration for dependency graph behaviour."""

    # If True, a CRITICAL upstream causes downstream to be marked DEGRADED
    propagate_critical: bool = True
    # If True, a WARNING upstream causes downstream to be marked DEGRADED
    propagate_warning: bool = False

    def validate(self) -> None:
        """Validate configuration (currently no numeric bounds to check)."""
        pass  # boolean fields are always valid


@dataclass
class DependencyEdge:
    """A directed edge from an upstream pipeline to a downstream pipeline."""

    upstream: str
    downstream: str

    def to_dict(self) -> dict:
        return {"upstream": self.upstream, "downstream": self.downstream}


@dataclass
class DependencyResult:
    """Result of evaluating one pipeline's dependency health."""

    pipeline: str
    degraded_by: List[str] = field(default_factory=list)
    is_degraded: bool = False

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "degraded_by": self.degraded_by,
            "is_degraded": self.is_degraded,
        }


class DependencyGraph:
    """Tracks pipeline dependencies and propagates health status."""

    def __init__(self, config: Optional[DependencyConfig] = None) -> None:
        self._config = config or DependencyConfig()
        # downstream -> set of upstreams
        self._upstreams: Dict[str, Set[str]] = {}
        # upstream -> set of downstreams
        self._downstreams: Dict[str, Set[str]] = {}

    def add_dependency(self, upstream: str, downstream: str) -> None:
        """Register that *downstream* depends on *upstream*."""
        self._upstreams.setdefault(downstream, set()).add(upstream)
        self._downstreams.setdefault(upstream, set()).add(downstream)

    def remove_dependency(self, upstream: str, downstream: str) -> None:
        """Remove a previously registered dependency edge."""
        self._upstreams.get(downstream, set()).discard(upstream)
        self._downstreams.get(upstream, set()).discard(downstream)

    def upstreams_of(self, pipeline: str) -> List[str]:
        """Return all direct upstream pipelines for *pipeline*."""
        return sorted(self._upstreams.get(pipeline, set()))

    def downstreams_of(self, pipeline: str) -> List[str]:
        """Return all direct downstream pipelines for *pipeline*."""
        return sorted(self._downstreams.get(pipeline, set()))

    def all_edges(self) -> List[DependencyEdge]:
        """Return every registered edge in the graph."""
        edges: List[DependencyEdge] = []
        for downstream, upstreams in self._upstreams.items():
            for upstream in sorted(upstreams):
                edges.append(DependencyEdge(upstream=upstream, downstream=downstream))
        return edges

    def evaluate(
        self, metrics: List[PipelineMetric]
    ) -> Dict[str, DependencyResult]:
        """Evaluate dependency health for every pipeline in *metrics*.

        Returns a mapping of pipeline name -> DependencyResult.  A pipeline
        is considered *degraded* when at least one of its upstreams has a
        status that should be propagated according to the config.
        """
        status_by_name: Dict[str, PipelineStatus] = {
            m.pipeline: m.status for m in metrics
        }

        results: Dict[str, DependencyResult] = {}
        for metric in metrics:
            name = metric.pipeline
            degraded_by: List[str] = []

            for upstream in self.upstreams_of(name):
                upstream_status = status_by_name.get(upstream)
                if upstream_status is None:
                    continue
                if self._config.propagate_critical and upstream_status == PipelineStatus.CRITICAL:
                    degraded_by.append(upstream)
                elif self._config.propagate_warning and upstream_status == PipelineStatus.WARNING:
                    degraded_by.append(upstream)

            results[name] = DependencyResult(
                pipeline=name,
                degraded_by=sorted(set(degraded_by)),
                is_degraded=len(degraded_by) > 0,
            )

        return results
