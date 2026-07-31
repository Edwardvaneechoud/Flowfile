"""Splits the graph into independent pipelines."""

from collections import defaultdict

from flowfile_core.flowfile.util.layout.extract import Edge


def weak_components(node_ids: list[int], edges: list[Edge]) -> list[list[int]]:
    """Weakly-connected components, each sorted, ordered by their lowest node id.

    Unrelated pipelines get laid out separately so they stack into their own
    bands instead of sharing columns and braiding together.
    """
    parent = {node_id: node_id for node_id in node_ids}

    def find(node_id: int) -> int:
        while parent[node_id] != node_id:
            parent[node_id] = parent[parent[node_id]]
            node_id = parent[node_id]
        return node_id

    for edge in edges:
        left, right = find(edge.source), find(edge.target)
        if left != right:
            parent[max(left, right)] = min(left, right)

    members: dict[int, list[int]] = defaultdict(list)
    for node_id in node_ids:
        members[find(node_id)].append(node_id)
    return [sorted(group) for _, group in sorted(members.items())]
