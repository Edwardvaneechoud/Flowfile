import logging
import math
from collections import defaultdict, deque
from typing import List, Dict, Set, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    # Make sure this import path is correct for your project structure
    from flowfile_core.flowfile.FlowfileFlow import FlowGraph


def calculate_layered_layout(
    graph: 'FlowGraph',
    x_spacing: int = 250,
    y_spacing: int = 100,
    initial_y: int = 50
) -> Dict[int, Tuple[int, int]]:
    """
    Calculates node positions using a simplified layered approach for a
    LEFT-TO-RIGHT flow. Stages determine horizontal position (X), and
    nodes within a stage are spread vertically (Y).

    Args:
        graph: The FlowGraph instance.
        x_spacing: Horizontal distance between stage centers (X spacing).
        y_spacing: Vertical distance between node centers within a stage (Y spacing).
        initial_y: Reference Y position for vertically centering stages.

    Returns:
        A dictionary mapping node_id to calculated (pos_x, pos_y).
    """
    if not graph._node_db:
        return {}

    nodes = graph.nodes
    node_ids = {node.node_id for node in nodes}
    adj: Dict[int, List[int]] = defaultdict(list)
    rev_adj: Dict[int, List[int]] = defaultdict(list)
    in_degree: Dict[int, int] = defaultdict(int)

    # --- Graph Building Stage ---
    try:
        connections = graph.node_connections
        for u, v in connections:
            if u in node_ids and v in node_ids:
                if v not in adj[u]:
                    adj[u].append(v)
                if u not in rev_adj[v]:
                    rev_adj[v].append(u)
        for node_id in node_ids:
            in_degree[node_id] = len(rev_adj.get(node_id, []))

    except Exception as e:
        logging.warning(e)
        # Fallback graph building
        adj.clear()
        rev_adj.clear()
        in_degree.clear()
        for node in nodes:
            if node.node_id not in in_degree:
                in_degree[node.node_id] = 0
        for node in nodes:
            children = node.leads_to_nodes
            for child_node in children:
                if child_node.node_id in node_ids:
                    if child_node.node_id not in adj[node.node_id]:
                        adj[node.node_id].append(child_node.node_id)
                    # Assuming primary method works or in_degree is handled elsewhere
                    in_degree[child_node.node_id] += 1

    stages: Dict[int, List[int]] = defaultdict(list)
    node_stage: Dict[int, int] = {}
    initial_sources = sorted([node_id for node_id in node_ids if in_degree.get(node_id, 0) == 0])
    queue = deque(initial_sources)
    current_stage = 0
    processed_nodes_count = 0
    current_in_degree = defaultdict(int, in_degree)

    while queue:
        stage_size = len(queue)
        processing_order = sorted(list(queue))
        queue.clear()
        nodes_in_current_stage = []
        for u in processing_order:
            nodes_in_current_stage.append(u)
            node_stage[u] = current_stage
            processed_nodes_count += 1
            for v in sorted(adj.get(u, [])):
                current_in_degree[v] -= 1
                if current_in_degree[v] == 0:
                    queue.append(v)
                elif current_in_degree[v] < 0:
                    # Basic warning for potential issues
                    print(f"[Layout Warning] Node {v} negative in-degree.")
        if nodes_in_current_stage:
            stages[current_stage] = nodes_in_current_stage
            current_stage += 1

    # Handle unprocessed nodes (cycles/disconnected)
    if processed_nodes_count != len(node_ids):
        print(f"[Layout Warning] Cycles or disconnected? Processed {processed_nodes_count}/{len(node_ids)}.")
        unprocessed_nodes = sorted(list(node_ids - set(node_stage.keys())))
        for node_id in unprocessed_nodes:
            node_stage[node_id] = current_stage
            stages[current_stage].append(node_id)
        if unprocessed_nodes:
            stages[current_stage].sort()
            current_stage += 1

    # --- Coordinate Assignment Stage ---
    positions: Dict[int, Tuple[int, int]] = {}
    max_stage_height = 0

    for stage_index, node_ids_in_stage in stages.items():
        if not node_ids_in_stage:
            continue
        sorted_nodes = sorted(node_ids_in_stage)
        stages[stage_index] = sorted_nodes
        stage_height = (len(sorted_nodes) - 1) * y_spacing
        max_stage_height = max(max_stage_height, stage_height)

    center_offset_y = max_stage_height / 2

    for stage_index, node_ids_in_stage in stages.items():
        if not node_ids_in_stage:
            continue

        pos_x = stage_index * x_spacing

        stage_height = (len(node_ids_in_stage) - 1) * y_spacing
        current_center_y = initial_y + center_offset_y
        start_y = current_center_y - (stage_height / 2)

        for i, node_id in enumerate(node_ids_in_stage):
            pos_y = start_y + i * y_spacing
            positions[node_id] = (int(pos_x), int(pos_y))

    return positions
