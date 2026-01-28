from collections import defaultdict, deque
from dataclasses import dataclass

from flowfile_core.configs import logger
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.util.node_skipper import determine_nodes_to_skip


@dataclass(frozen=True)
class ExecutionStage:
    """A group of nodes with no mutual dependencies that can execute in parallel."""

    nodes: list[FlowNode]

    def __len__(self) -> int:
        return len(self.nodes)

    def __iter__(self):
        return iter(self.nodes)


@dataclass(frozen=True)
class ExecutionPlan:
    """Complete execution plan: nodes to skip and ordered stages of parallelizable nodes."""

    skip_nodes: list[FlowNode]
    stages: list[ExecutionStage]

    @property
    def all_nodes(self) -> list[FlowNode]:
        """Flattened list of all nodes across all stages, preserving topological order."""
        return [node for stage in self.stages for node in stage.nodes]

    @property
    def node_count(self) -> int:
        return sum(len(stage) for stage in self.stages)


def compute_execution_plan(nodes: list[FlowNode], flow_starts: list[FlowNode] = None) -> ExecutionPlan:
    """Computes the execution plan: nodes to skip and parallelizable execution stages.

    Args:
        nodes: All nodes in the flow.
        flow_starts: Explicit starting nodes for the flow.

    Returns:
        An ExecutionPlan containing skip_nodes and ordered execution stages.
    """
    skip_nodes = determine_nodes_to_skip(nodes=nodes)
    stages = determine_execution_order(
        all_nodes=[node for node in nodes if node not in skip_nodes], flow_starts=flow_starts
    )
    return ExecutionPlan(skip_nodes=skip_nodes, stages=stages)


def determine_execution_order(
    all_nodes: list[FlowNode], flow_starts: list[FlowNode] = None
) -> list[ExecutionStage]:
    """
    Determines the execution order of nodes using topological sorting based on node dependencies.
    Returns stages of nodes where each stage contains nodes that can execute in parallel.

    Args:
        all_nodes: A list of all nodes in the flow.
        flow_starts: Starting nodes for the flow. If not provided, starts with zero in-degree nodes.

    Returns:
        A list of ExecutionStage objects in dependency order. Nodes within a stage have no
        mutual dependencies and can run concurrently.

    Raises:
        Exception: If a cycle is detected in the graph.
    """
    node_map = build_node_map(all_nodes)
    in_degree, adjacency_list = compute_in_degrees_and_adjacency_list(all_nodes, node_map)

    queue, visited_nodes = initialize_queue(flow_starts, all_nodes, in_degree)

    stages = perform_topological_sort(queue, node_map, in_degree, adjacency_list, visited_nodes)
    total_nodes = sum(len(stage) for stage in stages)
    if total_nodes != len(node_map):
        raise Exception("Cycle detected in the graph. Execution order cannot be determined.")

    all_nodes_flat = [node for stage in stages for node in stage if node.is_correct]
    logger.info(f"execution order: \n {all_nodes_flat}")

    return stages


def build_node_map(all_nodes: list[FlowNode]) -> dict[str, FlowNode]:
    """
    Creates a mapping from node ID to node object.

    Args:
        all_nodes (List[FlowNode]): A list of all nodes (steps) in the flow.

    Returns:
        Dict[str, FlowNode]: A dictionary mapping node IDs to FlowNode objects.
    """
    return {node.node_id: node for node in all_nodes}


def compute_in_degrees_and_adjacency_list(
    all_nodes: list[FlowNode], node_map: dict[str, FlowNode]
) -> (dict[str, int], dict[str, list[str]]):
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


def initialize_queue(
    flow_starts: list[FlowNode], all_nodes: list[FlowNode], in_degree: dict[str, int]
) -> (deque, set[str]):
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


def perform_topological_sort(
    queue: deque,
    node_map: dict[str, FlowNode],
    in_degree: dict[str, int],
    adjacency_list: dict[str, list[str]],
    visited_nodes: set[str],
) -> list[ExecutionStage]:
    """
    Performs a level-based topological sort, grouping nodes into execution stages.
    Nodes within the same stage have no dependencies on each other and can run in parallel.

    Args:
        queue: A deque containing node IDs with zero in-degree.
        node_map: A dictionary mapping node IDs to FlowNode objects.
        in_degree: A dictionary mapping node IDs to their in-degree count.
        adjacency_list: A dictionary mapping node IDs to a list of their outgoing node IDs.
        visited_nodes: A set of visited node IDs.

    Returns:
        A list of ExecutionStage objects. Each stage contains nodes that can execute concurrently.
    """
    stages: list[ExecutionStage] = []
    logger.info("Starting topological sort to determine execution order")

    while queue:
        current_stage_ids = list(queue)
        queue.clear()

        stage_nodes: list[FlowNode] = []
        for node_id in current_stage_ids:
            node = node_map.get(node_id)
            if node is None:
                logger.warning(f"Node with ID {node_id} not found in the node map.")
                continue
            stage_nodes.append(node)

            for next_node_id in adjacency_list.get(node_id, []):
                in_degree[next_node_id] -= 1
                if in_degree[next_node_id] == 0 and next_node_id not in visited_nodes:
                    queue.append(next_node_id)
                    visited_nodes.add(next_node_id)

        if stage_nodes:
            stages.append(ExecutionStage(nodes=stage_nodes))

    return stages
