from typing import Dict, Tuple, Optional, List, Set, Callable
from copy import deepcopy
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection


def combine_flow_graphs_with_mapping(
        *flow_graphs: FlowGraph,
        target_flow_id: Optional[int] = None) -> Tuple[FlowGraph, Dict[Tuple[int, int], int]]:
    # Validate input parameters
    _validate_input(flow_graphs)

    # Generate a unique flow ID if not provided
    if target_flow_id is None:
        target_flow_id = _generate_unique_flow_id(flow_graphs)

    flow_settings = _create_flow_settings(flow_graphs[0], target_flow_id)
    combined_graph = FlowGraph(flow_id=target_flow_id, flow_settings=flow_settings)
    node_id_mapping = _create_node_id_mapping(flow_graphs)
    _add_nodes_to_combined_graph(flow_graphs, combined_graph, node_id_mapping, target_flow_id)
    _add_connections_to_combined_graph(flow_graphs, combined_graph, node_id_mapping)
    return combined_graph, node_id_mapping


def combine_flow_graphs(*flow_graphs: FlowGraph, target_flow_id: Optional[int] = None) -> FlowGraph:
    """
    Combine multiple flow graphs into a single graph, ensuring node IDs don't overlap.

    Args:
        *flow_graphs: Multiple FlowGraph instances to combine
        target_flow_id: Optional ID for the new combined graph. If None, a new ID will be generated.

    Returns:
        A new FlowGraph containing all nodes and edges from the input graphs with remapped IDs

    Raises:
        ValueError: If no flow graphs are provided
    """
    # Validate input parameters
    _validate_input(flow_graphs)

    # Generate a unique flow ID if not provided
    if target_flow_id is None:
        target_flow_id = _generate_unique_flow_id(flow_graphs)

    flow_settings = _create_flow_settings(flow_graphs[0], target_flow_id)
    combined_graph = FlowGraph(flow_id=target_flow_id, flow_settings=flow_settings)
    node_id_mapping = _create_node_id_mapping(flow_graphs)
    _add_nodes_to_combined_graph(flow_graphs, combined_graph, node_id_mapping, target_flow_id)
    _add_connections_to_combined_graph(flow_graphs, combined_graph, node_id_mapping)

    return combined_graph


def _validate_input(flow_graphs: Tuple[FlowGraph, ...]) -> None:
    """
    Validate input parameters.

    Args:
        flow_graphs: Flow graphs to validate

    Raises:
        ValueError: If validation fails
    """
    if not flow_graphs:
        raise ValueError("At least one FlowGraph must be provided")

    # Check for duplicate flow IDs
    flow_ids = [fg.flow_id for fg in flow_graphs]
    if len(flow_ids) != len(set(flow_ids)):
        raise ValueError("Cannot combine flows with duplicate flow IDs")


def _generate_unique_flow_id(flow_graphs: Tuple[FlowGraph, ...]) -> int:
    """
    Generate a unique flow ID based on the input flow graphs.

    Args:
        flow_graphs: Flow graphs to generate ID from

    Returns:
        int: A new unique flow ID
    """
    return abs(hash(tuple(fg.flow_id for fg in flow_graphs))) % 1000000


def _create_flow_settings(base_flow_graph: FlowGraph, target_flow_id: int) -> schemas.FlowSettings:
    """
    Create flow settings for the combined graph based on an existing graph.

    Args:
        base_flow_graph: Flow graph to base settings on
        target_flow_id: The new flow ID

    Returns:
        schemas.FlowSettings: Flow settings for the combined graph
    """
    flow_settings = deepcopy(base_flow_graph.flow_settings)
    flow_settings.flow_id = target_flow_id
    flow_settings.name = f"Combined Flow {target_flow_id}"
    return flow_settings


def _create_node_id_mapping(flow_graphs: Tuple[FlowGraph, ...]) -> Dict[Tuple[int, int], int]:
    """
    Create a mapping from (flow_id, original_node_id) to new unique node IDs.

    Args:
        flow_graphs: Flow graphs to process

    Returns:
        Dict: Mapping from (flow_id, node_id) to new node ID
    """
    node_id_mapping = {}
    next_node_id = _get_next_available_node_id(flow_graphs)

    for fg in flow_graphs:
        for node in fg.nodes:
            node_id_mapping[(fg.flow_id, node.node_id)] = next_node_id
            next_node_id += 1

    return node_id_mapping


def _get_next_available_node_id(flow_graphs: Tuple[FlowGraph, ...]) -> int:
    """
    Find the next available node ID.

    Args:
        flow_graphs: Flow graphs to examine

    Returns:
        int: Next available node ID
    """
    max_id = 0
    for fg in flow_graphs:
        for node in fg.nodes:
            max_id = max(max_id, node.node_id)
    return max_id + 1


def _add_nodes_to_combined_graph(
        flow_graphs: Tuple[FlowGraph, ...],
        combined_graph: FlowGraph,
        node_id_mapping: Dict[Tuple[int, int], int],
        target_flow_id: int
) -> None:
    """
    Add all nodes from source graphs to the combined graph.

    Args:
        flow_graphs: Source flow graphs
        combined_graph: Target combined graph
        node_id_mapping: Mapping of node IDs
        target_flow_id: Target flow ID
    """
    processed_nodes = set()

    for fg in flow_graphs:
        for node in fg.nodes:
            # Skip if already processed
            if (fg.flow_id, node.node_id) in processed_nodes:
                continue

            # Generate new node ID
            new_node_id = node_id_mapping[(fg.flow_id, node.node_id)]

            # Create and update setting input
            setting_input = _create_updated_setting_input(
                node.setting_input,
                new_node_id,
                target_flow_id,
                fg.flow_id,
                node_id_mapping
            )

            # Add node to combined graph
            _add_node_to_graph(combined_graph, new_node_id, target_flow_id, node.node_type, setting_input)

            processed_nodes.add((fg.flow_id, node.node_id))


def _create_updated_setting_input(
        original_setting_input: any,
        new_node_id: int,
        target_flow_id: int,
        source_flow_id: int,
        node_id_mapping: Dict[Tuple[int, int], int]
) -> any:
    """
    Create an updated setting input with new node and flow IDs.

    Args:
        original_setting_input: Original setting input
        new_node_id: New node ID
        target_flow_id: Target flow ID
        source_flow_id: Source flow ID
        node_id_mapping: Mapping of node IDs

    Returns:
        Updated setting input
    """
    setting_input = deepcopy(original_setting_input)

    # Update node ID
    if hasattr(setting_input, 'node_id'):
        setting_input.node_id = new_node_id

    # Update flow ID
    if hasattr(setting_input, 'flow_id'):
        setting_input.flow_id = target_flow_id

    # Update depending_on_id if present
    if hasattr(setting_input, 'depending_on_id') and setting_input.depending_on_id != -1:
        orig_depending_id = setting_input.depending_on_id
        setting_input.depending_on_id = node_id_mapping.get((source_flow_id, orig_depending_id), -1)

    # Update depending_on_ids list if present
    if hasattr(setting_input, 'depending_on_ids'):
        setting_input.depending_on_ids = [
            node_id_mapping.get((source_flow_id, dep_id), -1)
            for dep_id in setting_input.depending_on_ids
            if dep_id != -1
        ]

    return setting_input


def _add_node_to_graph(
        graph: FlowGraph,
        node_id: int,
        flow_id: int,
        node_type: str,
        setting_input: any
) -> None:
    """
    Add a node to the graph.

    Args:
        graph: Target graph
        node_id: Node ID
        flow_id: Flow ID
        node_type: Node type
        setting_input: Setting input
    """
    # Add node promise to graph
    node_promise = input_schema.NodePromise(
        node_id=node_id,
        flow_id=flow_id,
        node_type=node_type,
        is_setup=True,
        pos_x=getattr(setting_input, 'pos_x', 0),
        pos_y=getattr(setting_input, 'pos_y', 0),
        description=getattr(setting_input, 'description', '')
    )
    graph.add_node_promise(node_promise)

    # Get node type-specific add method
    add_method_name = f"add_{node_type}"
    if hasattr(graph, add_method_name):
        add_method = getattr(graph, add_method_name)
        add_method(setting_input)


def _add_connections_to_combined_graph(
        flow_graphs: Tuple[FlowGraph, ...],
        combined_graph: FlowGraph,
        node_id_mapping: Dict[Tuple[int, int], int]
) -> None:
    """
    Add all connections from source graphs to the combined graph.

    Args:
        flow_graphs: Source flow graphs
        combined_graph: Target combined graph
        node_id_mapping: Mapping of node IDs
    """
    for fg in flow_graphs:
        for connection in fg.node_connections:
            source_id, target_id = connection
            new_source_id = node_id_mapping.get((fg.flow_id, source_id))
            new_target_id = node_id_mapping.get((fg.flow_id, target_id))

            if new_source_id is not None and new_target_id is not None:
                input_type = _determine_connection_input_type(fg, source_id, target_id)

                # Create connection in combined graph
                node_connection = input_schema.NodeConnection.create_from_simple_input(
                    from_id=new_source_id,
                    to_id=new_target_id,
                    input_type=input_type
                )
                add_connection(combined_graph, node_connection)


def _determine_connection_input_type(
        flow_graph: FlowGraph,
        source_id: int,
        target_id: int
) -> str:
    """
    Determine the input type for a connection.

    Args:
        flow_graph: Source flow graph
        source_id: Source node ID
        target_id: Target node ID

    Returns:
        str: Input type (main, left, right)
    """
    from_node = flow_graph.get_node(source_id)
    to_node = flow_graph.get_node(target_id)

    if from_node and to_node:
        input_types = to_node.get_input_type(from_node.node_id)
        if input_types:
            return input_types[0]

    return "main"