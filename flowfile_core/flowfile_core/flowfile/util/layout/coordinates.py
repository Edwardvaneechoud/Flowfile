"""Turns a column ordering into vertical positions."""

from flowfile_core.flowfile.util.layout.extract import Edge
from flowfile_core.flowfile.util.layout.neighbours import Adjacency, adjacency, median

_ALIGNMENT_PASSES = 5


def _pool_adjacent_violators(targets: list[float]) -> list[float]:
    """The closest non-decreasing sequence to *targets*."""
    blocks: list[tuple[float, int]] = []
    for target in targets:
        total, count = target, 1
        while blocks and blocks[-1][0] / blocks[-1][1] > total / count:
            pooled_total, pooled_count = blocks.pop()
            total += pooled_total
            count += pooled_count
        blocks.append((total, count))
    return [total / count for total, count in blocks for _ in range(count)]


def _fit_column(targets: list[float], spacing: int) -> list[float]:
    """Closest placement to *targets* that keeps the column in order and *spacing* apart.

    Subtracting each seat's minimum offset turns the separation constraint into a
    plain non-decreasing one, which pooling then solves exactly.
    """
    relaxed = _pool_adjacent_violators([target - seat * spacing for seat, target in enumerate(targets)])
    return [value + seat * spacing for seat, value in enumerate(relaxed)]


def _align(layers: list[list[int]], y: dict[int, float], neighbours: Adjacency, indices: range, spacing: int) -> None:
    for index in indices:
        column = layers[index]
        targets = [median([y[other] for other, _ in neighbours[node_id]], y[node_id]) for node_id in column]
        for node_id, value in zip(column, _fit_column(targets, spacing), strict=True):
            y[node_id] = value


def assign_y(layers: list[list[int]], edges: list[Edge], y_spacing: int) -> dict[int, float]:
    """Pulls every node towards the middle of its neighbours, column by column.

    Passes alternate and end going downstream, so a node settles on the median of
    its inputs: that is what puts a join exactly between the two nodes feeding it
    and keeps an unbranched chain on one horizontal line.
    """
    incoming, outgoing = adjacency(edges)
    y = {node_id: float(seat * y_spacing) for column in layers for seat, node_id in enumerate(column)}
    for index in range(_ALIGNMENT_PASSES):
        if index % 2 == 0:
            _align(layers, y, incoming, range(1, len(layers)), y_spacing)
        else:
            _align(layers, y, outgoing, range(len(layers) - 2, -1, -1), y_spacing)
    return y
