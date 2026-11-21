"""A step's inputs as a saved flow document stores them (pure, no FlowGraph).

A document stores each edge on both ends: the target holds ``input_ids`` (the ordered main inputs),
``right_input_id``, ``left_input_id`` and, for a node with keyed inputs, ``input_connections``; the source holds
``outputs`` with the parallel ``output_handles``. A static target does not store which output of its source feeds
it, so the handle is read from the source, consumed in order.
"""

from __future__ import annotations

import heapq
from collections import Counter, defaultdict

MAIN, RIGHT, LEFT = "input-0", "input-1", "input-2"
DEFAULT_HANDLE = "output-0"


def source_handles(nodes: dict[int, dict]) -> dict[tuple[int, int], list[str]]:
    """``(source, target) -> [handles]`` in the source's ``outputs`` order."""
    handles: dict[tuple[int, int], list[str]] = defaultdict(list)
    for node in nodes.values():
        outputs = node.get("outputs") or []
        names = node.get("output_handles") or []
        for index, target in enumerate(outputs):
            handles[(node["id"], target)].append(names[index] if index < len(names) else DEFAULT_HANDLE)
    return handles


def incoming_edges(node: dict, nodes: dict[int, dict]) -> dict[str, list[tuple[int, str]]]:
    """``target handle -> [(source, source handle)]`` for one node: main in order, right, left, keyed.

    Static source handles come from the source's ``outputs``/``output_handles`` (consumed in order, so a
    source feeding two slots through two handles keeps both); keyed edges carry their own.
    """
    handles = source_handles(nodes)
    used: Counter = Counter()

    def handle(source: int) -> str:
        options = handles.get((source, node["id"])) or [DEFAULT_HANDLE]
        index = used[source]
        used[source] += 1
        return options[index] if index < len(options) else options[0]

    edges: dict[str, list[tuple[int, str]]] = {}
    main = [(source, handle(source)) for source in node.get("input_ids") or []]
    if main:
        edges[MAIN] = main
    for slot, field in ((LEFT, "left_input_id"), (RIGHT, "right_input_id")):
        if node.get(field) is not None:
            edges[slot] = [(node[field], handle(node[field]))]
    for conn in node.get("input_connections") or []:
        edges.setdefault(conn["input_handle"], []).append(
            (conn["from_id"], conn.get("source_handle") or DEFAULT_HANDLE)
        )
    return edges


def topological(nodes: dict[int, dict]) -> list[int]:
    """Kahn order over the payload's edges with a min-heap on id; ids left in a cycle follow in id order."""
    parents = {
        nid: {s for edges in incoming_edges(n, nodes).values() for s, _ in edges if s in nodes}
        for nid, n in nodes.items()
    }
    children: dict[int, set[int]] = defaultdict(set)
    for nid, sources in parents.items():
        for source in sources:
            children[source].add(nid)
    degree = {nid: len(sources) for nid, sources in parents.items()}
    heap = [nid for nid, d in degree.items() if d == 0]
    heapq.heapify(heap)
    order: list[int] = []
    while heap:
        nid = heapq.heappop(heap)
        order.append(nid)
        for child in sorted(children[nid]):
            degree[child] -= 1
            if degree[child] == 0:
                heapq.heappush(heap, child)
    return order + sorted(set(nodes) - set(order))
