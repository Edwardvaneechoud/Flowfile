from typing import List, Dict, Set
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.configs import logger
from collections import deque, defaultdict


def determine_execution_order(all_nodes: List[FlowNode], flow_starts: List[FlowNode] = None) -> List[FlowNode]:
    """
    Determines the execution order of nodes using topological sorting based on node dependencies.

    Args:
        all_nodes (List[FlowNode]): A list of all nodes (steps) in the flow.
        flow_starts (List[FlowNode], optional): A list of starting nodes for the flow. If not provided, the function starts with nodes having zero in-degree.

    Returns:
        List[FlowNode]: A list of nodes in the order they should be executed.

    Raises:
        Exception: If a cycle is detected, meaning that a valid execution order cannot be determined.
    """
    node_map = build_node_map(all_nodes)
    in_degree, adjacency_list = compute_in_degrees_and_adjacency_list(all_nodes, node_map)

    queue, visited_nodes = initialize_queue(flow_starts, all_nodes, in_degree)

    execution_order = perform_topological_sort(queue, node_map, in_degree, adjacency_list, visited_nodes)
    if len(execution_order) != len(node_map):
        raise Exception("Cycle detected in the graph. Execution order cannot be determined.")

    logger.info(f"execution order: \n {[node for node in execution_order if node.is_correct]}")

    return execution_order


def build_node_map(all_nodes: List[FlowNode]) -> Dict[str, FlowNode]:
    """
    Creates a mapping from node ID to node object.

    Args:
        all_nodes (List[FlowNode]): A list of all nodes (steps) in the flow.

    Returns:
        Dict[str, FlowNode]: A dictionary mapping node IDs to FlowNode objects.
    """
    return {node.node_id: node for node in all_nodes}


def compute_in_degrees_and_adjacency_list(all_nodes: List[FlowNode],
                                          node_map: Dict[str, FlowNode]) -> (Dict[str, int], Dict[str, List[str]]):
    """
    Computes the in-degree and adjacency list for all nodes.

    Args:
        all_nodes (List[FlowNode]): A list of all nodes (steps) in the flow.
        node_map (Dict[str, FlowNode]): A dictionary mapping node IDs to FlowNode objects.

    Returns:
        (Dict[str, int], Dict[str, List[str]]): A tuple containing:
            - in_degree: A dictionary mapping node IDs to their in-degree count.
            - adjacency_list: A dictionary mapping node IDs to a list of their connected nodes (outgoing edges).
    """
    in_degree = defaultdict(int)
    adjacency_list = defaultdict(list)

    for node in all_nodes:
        for next_node in node.leads_to_nodes:
            adjacency_list[node.node_id].append(next_node.node_id)
            in_degree[next_node.node_id] += 1
            if next_node.node_id not in node_map:
                node_map[next_node.node_id] = next_node

    return in_degree, adjacency_list


def initialize_queue(flow_starts: List[FlowNode], all_nodes: List[FlowNode], in_degree: Dict[str, int]) -> (
deque, Set[str]):
    """
    Initializes the queue with nodes that have zero in-degree or based on specified flow start nodes.

    Args:
        flow_starts (List[FlowNode]): A list of starting nodes for the flow.
        all_nodes (List[FlowNode]): A list of all nodes (steps) in the flow.
        in_degree (Dict[str, int]): A dictionary mapping node IDs to their in-degree count.

    Returns:
        (deque, Set[str]): A tuple containing:
            - queue: A deque containing nodes with zero in-degree or specified start nodes.
            - visited_nodes: A set of visited node IDs to track processing state.
    """
    queue = deque()
    visited_nodes = set()

    if flow_starts and len(flow_starts) > 0:
        for node in flow_starts:
            if in_degree[node.node_id] == 0:
                queue.append(node.node_id)
                visited_nodes.add(node.node_id)
            else:
                logger.warning(f"Flow start node {node.node_id} has non-zero in-degree.")
    else:
        for node in all_nodes:
            if in_degree[node.node_id] == 0:
                queue.append(node.node_id)
                visited_nodes.add(node.node_id)

    return queue, visited_nodes


def perform_topological_sort(queue: deque, node_map: Dict[str, FlowNode], in_degree: Dict[str, int],
                             adjacency_list: Dict[str, List[str]], visited_nodes: Set[str]) -> List[FlowNode]:
    """
    Performs topological sorting to determine the execution order of nodes.

    Args:
        queue (deque): A deque containing nodes with zero in-degree.
        node_map (Dict[str, FlowNode]): A dictionary mapping node IDs to FlowNode objects.
        in_degree (Dict[str, int]): A dictionary mapping node IDs to their in-degree count.
        adjacency_list (Dict[str, List[str]]): A dictionary mapping node IDs to a list of their connected nodes (outgoing edges).
        visited_nodes (Set[str]): A set of visited node IDs.

    Returns:
        List[FlowNode]: A list of nodes in the order they should be executed.
    """
    execution_order = []
    logger.info('Starting topological sort to determine execution order')

    while queue:
        current_node_id = queue.popleft()
        current_node = node_map.get(current_node_id)
        if current_node is None:
            logger.warning(f"Node with ID {current_node_id} not found in the node map.")
            continue
        execution_order.append(current_node)

        for next_node_id in adjacency_list.get(current_node_id, []):
            in_degree[next_node_id] -= 1
            if in_degree[next_node_id] == 0 and next_node_id not in visited_nodes:
                queue.append(next_node_id)
                visited_nodes.add(next_node_id)

    return execution_order
