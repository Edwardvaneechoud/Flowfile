"""Reads a FlowGraph into a deterministic, port-aware edge list."""

from collections.abc import Callable
from dataclasses import dataclass

from flowfile_core.configs import logger
from flowfile_core.flowfile.flow_node.input_handles import input_handle_index
from flowfile_core.flowfile.flow_node.multi_output import output_handle_index


@dataclass(frozen=True, slots=True, order=True)
class Edge:
    source: int
    target: int
    source_port: int
    target_port: int


def _handle_index(handle: object, parse: Callable[[str], int]) -> int:
    try:
        return parse(handle)
    except (AttributeError, TypeError, ValueError):
        return 0


def _node_id(candidate: object) -> int | None:
    node_id = getattr(candidate, "node_id", None)
    return node_id if isinstance(node_id, int) else None


def _from_edge_inputs(nodes: list, node_ids: set[int]) -> list[Edge]:
    """The edge set the canvas renders, so the layout optimises what the user sees."""
    edges: list[Edge] = []
    for node in nodes:
        try:
            wire_edges = list(node.get_edge_input())
        except Exception:
            continue
        for wire in wire_edges:
            try:
                source, target = int(wire.source), int(wire.target)
            except (AttributeError, TypeError, ValueError):
                continue
            if source in node_ids and target in node_ids:
                edges.append(
                    Edge(
                        source,
                        target,
                        _handle_index(wire.sourceHandle, output_handle_index),
                        _handle_index(wire.targetHandle, input_handle_index),
                    )
                )
    return edges


def _from_connections(graph, node_ids: set[int]) -> list[Edge]:
    try:
        connections = sorted(graph.node_connections)
    except Exception as exc:
        logger.warning(f"Layout: node_connections unavailable ({exc})")
        return []
    return [Edge(source, target, 0, 0) for source, target in connections if source in node_ids and target in node_ids]


def extract_edges(graph) -> tuple[list[int], list[Edge]]:
    """Node ids and edges, both sorted.

    The canvas edge set leads because it carries the ports and is what the user
    sees; ``node_connections`` backfills any pair it missed (a per-node failure
    above, or forward/reverse links that disagree), so one broken node cannot
    drop a whole pipeline. Sorting is required for reproducible positions --
    ``node_connections`` is set-derived, and node positions feed the undo/redo
    hash.
    """
    nodes = sorted(getattr(graph, "nodes", None) or [], key=lambda node: _node_id(node) or 0)
    node_ids = {node_id for node_id in (_node_id(node) for node in nodes) if node_id is not None}

    edges = _from_edge_inputs(nodes, node_ids)
    pairs = {(edge.source, edge.target) for edge in edges}
    backfill = [edge for edge in _from_connections(graph, node_ids) if (edge.source, edge.target) not in pairs]
    if backfill:
        logger.warning(f"Layout: {len(backfill)} connection(s) missing from the canvas edge set")
    edges.extend(backfill)

    unique = {edge for edge in edges if edge.source != edge.target}
    return sorted(node_ids), sorted(unique)
