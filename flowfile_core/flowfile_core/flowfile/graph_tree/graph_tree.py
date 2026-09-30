import heapq

from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.flow_node.multi_output import output_handle_index

_MAX_DESCRIPTION = 50

Lane = tuple[int, str] | None


def _label(node: FlowNode) -> str:
    """``Name (id)``, the node's canvas name, plus the first line of its description, truncated."""
    name = getattr(node.node_template, "name", None) or node.node_type.replace("_", " ").title()
    label = f"{name} ({node.node_id})"
    lines = (getattr(node.setting_input, "description", None) or "").strip().splitlines()
    if not lines:
        return label
    description = lines[0] if len(lines[0]) <= _MAX_DESCRIPTION else lines[0][: _MAX_DESCRIPTION - 3] + "..."
    return f"{label}  {description}"


def _exit_label(source: FlowNode, handle: str) -> str:
    """The name of the output an edge leaves from, e.g. a Gate's ``then``/``else``; empty for a single output."""
    names = getattr(source.setting_input, "output_names", None) or []
    index = output_handle_index(handle)
    if len(names) > 1 and index < len(names):
        return names[index]
    return "" if index == 0 else handle


def _flow_order(outgoing: list[list[tuple[int, str]]]) -> list[int]:
    """Topological order in creation order, so code prints in the order it was written; dead ends go first
    among the nodes that are ready, which closes a side branch before the main line continues."""
    waiting = [0] * len(outgoing)
    for edges in outgoing:
        for target, _ in edges:
            waiting[target] += 1
    ready = [(bool(outgoing[i]), i) for i, count in enumerate(waiting) if count == 0]
    heapq.heapify(ready)
    order = []
    while ready:
        _, i = heapq.heappop(ready)
        order.append(i)
        for target, _ in outgoing[i]:
            waiting[target] -= 1
            if waiting[target] == 0:
                heapq.heappush(ready, (bool(outgoing[target]), target))
    return order + sorted(set(range(len(outgoing))) - set(order))


def _connector(lanes: list[Lane], col: int, joined: list[int], corner: str) -> str:
    """A row that merges lanes ``joined`` into column ``col`` (corner ``┘``) or branches them out of it (``┐``)."""
    last = max(joined)
    tee = "┴" if corner == "┘" else "┬"
    cells = []
    for c, lane in enumerate(lanes):
        if c == col:
            mark = "├"
        elif c == last:
            mark = corner
        elif c in joined:
            mark = tee
        elif col < c < last:
            mark = "┼" if lane is not None else "─"
        else:
            mark = "│" if lane is not None else " "
        cells.append(mark + ("─" if col <= c < last else " "))
    return "".join(cells).rstrip()


def render_flow(nodes: list[FlowNode]) -> str:
    """Draw the graph top to bottom, one node per line, with lanes where it branches and merges.

    Reads like ``git log --graph``: each ``●`` is a node, ``│`` links it to the next node in its column,
    ``├─┐`` opens a branch and ``├─┘`` joins one back.
    A node fed from a named output (a Gate's ``then``/``else``) carries that name in brackets.
    """
    index = {str(node.node_id): i for i, node in enumerate(nodes)}
    outgoing: list[list[tuple[int, str]]] = [[] for _ in nodes]
    for i, node in enumerate(nodes):
        for edge in node.get_edge_input():
            source = index[str(edge.source)]
            outgoing[source].append((i, _exit_label(nodes[source], edge.sourceHandle)))
    order = _flow_order(outgoing)
    position = {i: p for p, i in enumerate(order)}
    downstream: list[set[int]] = [set() for _ in nodes]
    for i in reversed(order):
        for target, _ in outgoing[i]:
            downstream[i] |= {target} | downstream[target]

    lanes: list[Lane] = []
    rows = []
    previous_col = None  # the column of the node on the last row, when that row is a node row
    for i in order:
        joined = [c for c, lane in enumerate(lanes) if lane is not None and lane[0] == i]
        reached = bool(joined)
        if not joined:
            joined = [lanes.index(None) if None in lanes else len(lanes)]
            if joined[0] == len(lanes):
                lanes.append(None)
        col = joined[0]
        if len(joined) > 1:
            rows.append(_connector(lanes, col, joined, "┘"))
        elif reached and previous_col == col:
            rows.append("".join(("│" if lane is not None else " ") + " " for lane in lanes))
        exits = sorted({lanes[c][1] for c in joined if lanes[c] is not None and lanes[c][1]})
        for c in joined:
            lanes[c] = None
        while len(lanes) > col + 1 and lanes[-1] is None:
            lanes.pop()
        marks = "".join(("●" if c == col else "│" if lane is not None else " ") + " " for c, lane in enumerate(lanes))
        rows.append(marks + _label(nodes[i]) + (f"  [{', '.join(exits)}]" if exits else ""))
        previous_col = col

        successors = sorted(outgoing[i], key=lambda edge: position[edge[0]])
        # The branch with the most nodes below it keeps this column, so the main line stays straight.
        trunk = max(successors, key=lambda edge: len(downstream[edge[0]]), default=None)
        opened = []
        for lane in successors:
            if lane is trunk:
                lanes[col] = lane
                continue
            free = next((c for c in range(col + 1, len(lanes)) if lanes[c] is None), None)
            if free is None:
                lanes.append(None)
                free = len(lanes) - 1
            lanes[free] = lane
            opened.append(free)
        if opened:
            rows.append(_connector(lanes, col, [col, *opened], "┐"))
            previous_col = None
        while lanes and lanes[-1] is None:
            lanes.pop()
    return "\n".join(row.rstrip() for row in rows)
