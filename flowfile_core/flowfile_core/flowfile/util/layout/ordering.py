"""Orders the nodes inside each column to reduce edge crossings."""

from collections import defaultdict

from flowfile_core.flowfile.util.layout.extract import Edge
from flowfile_core.flowfile.util.layout.neighbours import Adjacency, adjacency, median, port_bias

_SWEEPS = 4
_TRANSPOSE_ROUNDS = 3


def seed_layers(node_ids: list[int], layer: dict[int, int], edges: list[Edge]) -> list[list[int]]:
    """Depth-first from the sources, so the starting order follows the graph rather than node creation order."""
    depth = max((layer[node_id] for node_id in node_ids), default=0)
    layers: list[list[int]] = [[] for _ in range(depth + 1)]
    successors: dict[int, list[int]] = defaultdict(list)
    has_predecessor: set[int] = set()
    for edge in sorted(edges, reverse=True):
        successors[edge.source].append(edge.target)
        has_predecessor.add(edge.target)

    roots = [node_id for node_id in node_ids if node_id not in has_predecessor]
    stack = list(reversed(roots + [node_id for node_id in node_ids if node_id in has_predecessor]))
    seen: set[int] = set()
    while stack:
        node_id = stack.pop()
        if node_id in seen:
            continue
        seen.add(node_id)
        layers[layer[node_id]].append(node_id)
        stack.extend(successors[node_id])
    return layers


def _slots(layers: list[list[int]]) -> dict[int, int]:
    return {node_id: index for column in layers for index, node_id in enumerate(column)}


def _crossings(layers: list[list[int]], outgoing: Adjacency) -> int:
    slots = _slots(layers)
    total = 0
    for column in layers:
        spans = [(slots[node_id], slots[target]) for node_id in column for target, _ in outgoing[node_id]]
        for index, (from_a, to_a) in enumerate(spans):
            for from_b, to_b in spans[index + 1 :]:
                if (from_a - from_b) * (to_a - to_b) < 0:
                    total += 1
    return total


def _sweep(layers: list[list[int]], slots: dict[int, int], neighbours: Adjacency, indices: range) -> None:
    for index in indices:
        measures = {}
        for node_id in layers[index]:
            seats = [slots[other] + port_bias(port) for other, port in neighbours[node_id]]
            measures[node_id] = median(seats, slots[node_id])
        layers[index].sort(key=lambda node_id: (measures[node_id], slots[node_id]))
        for seat, node_id in enumerate(layers[index]):
            slots[node_id] = seat


def _pair_gain(left: int, right: int, slots: dict[int, int], sides: tuple[Adjacency, Adjacency]) -> int:
    """Crossings with *left* on top minus crossings with *right* on top, over their own edges only."""
    keep = swap = 0
    for side in sides:
        for other_left, _ in side[left]:
            for other_right, _ in side[right]:
                if slots[other_left] > slots[other_right]:
                    keep += 1
                elif slots[other_left] < slots[other_right]:
                    swap += 1
    return keep - swap


def _transpose(layers: list[list[int]], slots: dict[int, int], sides: tuple[Adjacency, Adjacency]) -> None:
    for _ in range(_TRANSPOSE_ROUNDS):
        improved = False
        for column in layers:
            for index in range(len(column) - 1):
                left, right = column[index], column[index + 1]
                if _pair_gain(left, right, slots, sides) > 0:
                    column[index], column[index + 1] = right, left
                    slots[left], slots[right] = index + 1, index
                    improved = True
        if not improved:
            return


def order_layers(layers: list[list[int]], edges: list[Edge]) -> list[list[int]]:
    """Alternating median sweeps, each followed by adjacent-swap refinement.

    The best ordering is kept on ``<=`` and the loop never exits early on a zero
    score: a sweep that fixes port order without changing the crossing count is
    still an improvement, and discarding it puts a join's two feeders back the
    wrong way round.
    """
    incoming, outgoing = adjacency(edges)
    current = [list(column) for column in layers]
    slots = _slots(current)
    best, best_score = [list(column) for column in current], _crossings(current, outgoing)

    for sweep in range(_SWEEPS):
        if sweep % 2 == 0:
            _sweep(current, slots, incoming, range(1, len(current)))
        else:
            _sweep(current, slots, outgoing, range(len(current) - 2, -1, -1))
        _transpose(current, slots, (incoming, outgoing))
        score = _crossings(current, outgoing)
        if score <= best_score:
            best, best_score = [list(column) for column in current], score
    return best
