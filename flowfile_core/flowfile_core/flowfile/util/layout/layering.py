"""Assigns each node to a column."""

import heapq
from collections import defaultdict

from flowfile_core.flowfile.util.layout.extract import Edge


def assign_layers(node_ids: list[int], edges: list[Edge]) -> dict[int, int]:
    """Longest-path layering: a node lands one column after its *last* predecessor.

    Nodes left unreached carry a cycle, which connection validation rejects at
    build time; they are appended as one terminal column so a corrupt flow still
    lays out every node.
    """
    successors: dict[int, list[int]] = defaultdict(list)
    in_degree: dict[int, int] = dict.fromkeys(node_ids, 0)
    for edge in edges:
        successors[edge.source].append(edge.target)
        in_degree[edge.target] += 1

    layer = dict.fromkeys(node_ids, 0)
    remaining = dict(in_degree)
    queue = [node_id for node_id in node_ids if in_degree[node_id] == 0]
    heapq.heapify(queue)
    resolved: set[int] = set()
    while queue:
        node_id = heapq.heappop(queue)
        resolved.add(node_id)
        for target in successors[node_id]:
            layer[target] = max(layer[target], layer[node_id] + 1)
            remaining[target] -= 1
            if remaining[target] == 0:
                heapq.heappush(queue, target)

    unreached = [node_id for node_id in node_ids if node_id not in resolved]
    if unreached:
        terminal = max(layer.values(), default=0) + 1
        for node_id in unreached:
            layer[node_id] = terminal
    return layer


def column_index(members: list[int], layer: dict[int, int]) -> dict[int, int]:
    """One component's layers renumbered to gapless columns starting at zero.

    Every pipeline is laid out on its own, so its first column belongs at the left
    edge whatever the layer numbers came out as across the whole graph.
    """
    ranks = {value: index for index, value in enumerate(sorted({layer[node_id] for node_id in members}))}
    return {node_id: ranks[layer[node_id]] for node_id in members}
