"""Left-to-right layered layout for a FlowGraph."""

import math
from typing import TYPE_CHECKING

from flowfile_core.flowfile.util.layout import (
    assign_layers,
    assign_y,
    column_index,
    extract_edges,
    order_layers,
    seed_layers,
    weak_components,
)

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph import FlowGraph


def calculate_layered_layout(
    graph: "FlowGraph",
    x_spacing: int = 250,
    y_spacing: int = 100,
    initial_y: int = 50,
) -> dict[int, tuple[int, int]]:
    """Positions every node of *graph* as a left-to-right layered graph.

    Columns are the dependency depth, so x is the stage. Unconnected pipelines are
    laid out on their own and stacked into separate horizontal bands, each starting
    at x=0, with twice *y_spacing* between bands. Lone nodes advance by *y_spacing*
    alone, so a canvas of unwired nodes stays as compact as a single column.

    Args:
        graph: The FlowGraph instance.
        x_spacing: Horizontal distance between columns.
        y_spacing: Minimum vertical distance between two nodes in a column.
        initial_y: Y position of the topmost node.

    Returns:
        A dictionary mapping node_id to (pos_x, pos_y).
    """
    node_ids, edges = extract_edges(graph)
    if not node_ids:
        return {}

    gutter = 2 * y_spacing
    layer = assign_layers(node_ids, edges)
    positions: dict[int, tuple[int, int]] = {}
    top = float(initial_y)

    for members in weak_components(node_ids, edges):
        member_ids = set(members)
        component_edges = [edge for edge in edges if edge.source in member_ids]
        columns = column_index(members, layer)
        ordered = order_layers(seed_layers(members, columns, component_edges), component_edges)
        y = assign_y(ordered, component_edges, y_spacing)

        offset = top - min(y.values())
        for index, column in enumerate(ordered):
            for node_id in column:
                positions[node_id] = (index * x_spacing, math.floor(y[node_id] + offset + 0.5))
        height = max(y.values()) - min(y.values())
        top += height + (gutter if len(members) > 1 else y_spacing)

    return positions
