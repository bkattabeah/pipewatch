"""CLI commands for pipeline topology inspection."""
from __future__ import annotations

import json
import click

from pipewatch.topology import TopologyGraph


def _build_sample_graph() -> TopologyGraph:
    """Build a demo graph; in production this would load from config/store."""
    g = TopologyGraph()
    g.add_edge("ingest", "transform")
    g.add_edge("transform", "aggregate")
    g.add_edge("transform", "validate")
    g.add_edge("aggregate", "report")
    g.add_edge("validate", "report")
    return g


@click.group()
def topology() -> None:
    """Inspect pipeline topology and relationships."""


@topology.command("show")
@click.argument("pipeline")
def cmd_show(pipeline: str) -> None:
    """Show upstream/downstream neighbours for PIPELINE."""
    graph = _build_sample_graph()
    node = graph.get(pipeline)
    if node is None:
        click.echo(f"Pipeline '{pipeline}' not found in topology.", err=True)
        raise SystemExit(1)
    click.echo(f"Pipeline : {node.name}")
    click.echo(f"Upstream : {', '.join(node.upstream) or '(none)'}")
    click.echo(f"Downstream: {', '.join(node.downstream) or '(none)'}")


@topology.command("ancestors")
@click.argument("pipeline")
def cmd_ancestors(pipeline: str) -> None:
    """List all transitive upstream pipelines for PIPELINE."""
    graph = _build_sample_graph()
    if graph.get(pipeline) is None:
        click.echo(f"Pipeline '{pipeline}' not found.", err=True)
        raise SystemExit(1)
    result = sorted(graph.ancestors(pipeline))
    click.echo("\n".join(result) if result else "(no ancestors)")


@topology.command("descendants")
@click.argument("pipeline")
def cmd_descendants(pipeline: str) -> None:
    """List all transitive downstream pipelines for PIPELINE."""
    graph = _build_sample_graph()
    if graph.get(pipeline) is None:
        click.echo(f"Pipeline '{pipeline}' not found.", err=True)
        raise SystemExit(1)
    result = sorted(graph.descendants(pipeline))
    click.echo("\n".join(result) if result else "(no descendants)")


@topology.command("json")
def cmd_json() -> None:
    """Dump full topology as JSON."""
    graph = _build_sample_graph()
    click.echo(json.dumps(graph.to_dict(), indent=2))
