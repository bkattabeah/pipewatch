"""Tests for pipewatch.topology."""
from __future__ import annotations

import pytest
from pipewatch.topology import TopologyGraph, TopologyNode


def make_graph() -> TopologyGraph:
    g = TopologyGraph()
    g.add_edge("ingest", "transform")
    g.add_edge("transform", "aggregate")
    g.add_edge("transform", "validate")
    g.add_edge("aggregate", "report")
    g.add_edge("validate", "report")
    return g


class TestTopologyGraph:
    def setup_method(self) -> None:
        self.graph = make_graph()

    def test_add_node_idempotent(self) -> None:
        self.graph.add_node("ingest")
        assert len([n for n in self.graph.nodes if n == "ingest"]) == 1

    def test_direct_downstream(self) -> None:
        node = self.graph.get("transform")
        assert node is not None
        assert "aggregate" in node.downstream
        assert "validate" in node.downstream

    def test_direct_upstream(self) -> None:
        node = self.graph.get("report")
        assert node is not None
        assert "aggregate" in node.upstream
        assert "validate" in node.upstream

    def test_get_returns_none_for_missing(self) -> None:
        assert self.graph.get("nonexistent") is None

    def test_ancestors_transitive(self) -> None:
        ancestors = self.graph.ancestors("report")
        assert "ingest" in ancestors
        assert "transform" in ancestors
        assert "aggregate" in ancestors
        assert "validate" in ancestors

    def test_ancestors_empty_for_root(self) -> None:
        assert self.graph.ancestors("ingest") == set()

    def test_descendants_transitive(self) -> None:
        descendants = self.graph.descendants("ingest")
        assert "transform" in descendants
        assert "aggregate" in descendants
        assert "report" in descendants

    def test_descendants_empty_for_leaf(self) -> None:
        assert self.graph.descendants("report") == set()

    def test_ancestors_unknown_pipeline(self) -> None:
        assert self.graph.ancestors("ghost") == set()

    def test_descendants_unknown_pipeline(self) -> None:
        assert self.graph.descendants("ghost") == set()

    def test_to_dict_contains_all_nodes(self) -> None:
        d = self.graph.to_dict()
        for name in ["ingest", "transform", "aggregate", "validate", "report"]:
            assert name in d

    def test_node_to_dict_keys(self) -> None:
        node = self.graph.get("transform")
        assert node is not None
        d = node.to_dict()
        assert set(d.keys()) == {"name", "upstream", "downstream", "tags"}

    def test_tags_stored_on_node(self) -> None:
        self.graph.add_node("tagged", tags={"team": "data"})
        node = self.graph.get("tagged")
        assert node is not None
        assert node.tags["team"] == "data"
