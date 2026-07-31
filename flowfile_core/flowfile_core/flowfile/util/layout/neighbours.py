"""Who sits next to whom, and where that puts a node."""

from collections import defaultdict

from flowfile_core.flowfile.util.layout.extract import Edge

Adjacency = dict[int, list[tuple[int, int]]]


def adjacency(edges: list[Edge]) -> tuple[Adjacency, Adjacency]:
    """Incoming and outgoing ``(neighbour, port)`` lists, keyed by node."""
    incoming: Adjacency = defaultdict(list)
    outgoing: Adjacency = defaultdict(list)
    for edge in edges:
        incoming[edge.target].append((edge.source, edge.source_port))
        outgoing[edge.source].append((edge.target, edge.target_port))
    return incoming, outgoing


def median(values: list[float], fallback: float) -> float:
    if not values:
        return fallback
    values.sort()
    middle = len(values) // 2
    if len(values) % 2:
        return values[middle]
    return (values[middle - 1] + values[middle]) / 2.0


def port_bias(port: int) -> float:
    """Strictly increasing and always under one slot, so a port breaks a tie but never outranks a neighbour."""
    return port / (port + 1.0)
