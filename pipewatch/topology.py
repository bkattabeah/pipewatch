"""Pipeline topology mapping — track upstream/downstream relationships."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set


@dataclass
class TopologyNode:
    name: str
    upstream: List[str] = field(default_factory=list)
    downstream: List[str] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "upstream": list(self.upstream),
            "downstream": list(self.downstream),
            "tags": dict(self.tags),
        }


@dataclass
class TopologyGraph:
    nodes: Dict[str, TopologyNode] = field(default_factory=dict)

    def add_node(self, name: str, tags: Optional[Dict[str, str]] = None) -> TopologyNode:
        if name not in self.nodes:
            self.nodes[name] = TopologyNode(name=name, tags=tags or {})
        return self.nodes[name]

    def add_edge(self, upstream: str, downstream: str) -> None:
        """Register a directed edge: upstream -> downstream."""
        self.add_node(upstream)
        self.add_node(downstream)
        if downstream not in self.nodes[upstream].downstream:
            self.nodes[upstream].downstream.append(downstream)
        if upstream not in self.nodes[downstream].upstream:
            self.nodes[downstream].upstream.append(upstream)

    def get(self, name: str) -> Optional[TopologyNode]:
        return self.nodes.get(name)

    def ancestors(self, name: str) -> Set[str]:
        """Return all transitive upstream nodes."""
        visited: Set[str] = set()
        stack = list(self.nodes[name].upstream) if name in self.nodes else []
        while stack:
            node = stack.pop()
            if node not in visited:
                visited.add(node)
                if node in self.nodes:
                    stack.extend(self.nodes[node].upstream)
        return visited

    def descendants(self, name: str) -> Set[str]:
        """Return all transitive downstream nodes."""
        visited: Set[str] = set()
        stack = list(self.nodes[name].downstream) if name in self.nodes else []
        while stack:
            node = stack.pop()
            if node not in visited:
                visited.add(node)
                if node in self.nodes:
                    stack.extend(self.nodes[node].downstream)
        return visited

    def to_dict(self) -> dict:
        return {name: node.to_dict() for name, node in self.nodes.items()}
