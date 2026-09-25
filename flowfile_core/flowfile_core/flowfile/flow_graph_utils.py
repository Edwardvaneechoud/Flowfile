from copy import deepcopy

import polars as pl

from flowfile_core.configs.node_store import CUSTOM_NODE_STORE, register_missing_node_template
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE
from flowfile_core.schemas import input_schema, schemas


def combine_flow_graphs_with_mapping(
    *flow_graphs: FlowGraph, target_flow_id: int | None = None
) -> tuple[FlowGraph, dict[tuple[int, int], int]]:
    """Combine flow graphs into one new graph and return it with the node id mapping.

    Every node is rebuilt from its settings (custom nodes included) and every edge keeps its
    source and target handles. Flow parameters are unioned by name (first graph wins), visual
    groups are carried under fresh group ids, and deferred nodes keep their seeded outputs so
    the merged graph never executes them before a flow run.

    Returns:
        The combined graph and a mapping ``(flow_id, original_node_id) -> new_node_id``.
    """
    _validate_input(flow_graphs)

    if target_flow_id is None:
        target_flow_id = _generate_unique_flow_id(flow_graphs)

    flow_settings = _create_flow_settings(flow_graphs[0], target_flow_id)
    _union_flow_parameters(flow_graphs, flow_settings)
    combined_graph = FlowGraph(flow_settings=flow_settings)
    node_id_mapping = _create_node_id_mapping(flow_graphs)
    group_id_mapping = _create_group_id_mapping(flow_graphs)
    _add_nodes_to_combined_graph(flow_graphs, combined_graph, node_id_mapping, target_flow_id, group_id_mapping)
    _add_groups_to_combined_graph(flow_graphs, combined_graph, group_id_mapping)
    _add_connections_to_combined_graph(flow_graphs, combined_graph, node_id_mapping)
    _carry_deferred_seeds(flow_graphs, combined_graph, node_id_mapping)
    return combined_graph, node_id_mapping


def combine_flow_graphs(*flow_graphs: FlowGraph, target_flow_id: int | None = None) -> FlowGraph:
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
    return combine_flow_graphs_with_mapping(*flow_graphs, target_flow_id=target_flow_id)[0]


def _validate_input(flow_graphs: tuple[FlowGraph, ...]) -> None:
    """
    Validate input parameters.

    Args:
        flow_graphs: Flow graphs to validate

    Raises:
        ValueError: If validation fails
    """
    if not flow_graphs:
        raise ValueError("At least one FlowGraph must be provided")

    flow_ids = [fg.flow_id for fg in flow_graphs]
    if len(flow_ids) != len(set(flow_ids)):
        raise ValueError("Cannot combine flows with duplicate flow IDs")


def _generate_unique_flow_id(flow_graphs: tuple[FlowGraph, ...]) -> int:
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


def _union_flow_parameters(flow_graphs: tuple[FlowGraph, ...], flow_settings: schemas.FlowSettings) -> None:
    """Add every other graph's flow parameters to ``flow_settings``; the first graph wins a name."""
    parameters = flow_settings.parameters
    names = {p.name for p in parameters}
    for fg in flow_graphs[1:]:
        for parameter in fg.flow_settings.parameters:
            if parameter.name not in names:
                names.add(parameter.name)
                parameters.append(deepcopy(parameter))


def _create_group_id_mapping(flow_graphs: tuple[FlowGraph, ...]) -> dict[tuple[int, int], int]:
    """Map ``(flow_id, group_id)`` to a fresh group id; every graph numbers its groups from 1."""
    group_id_mapping = {}
    for fg in flow_graphs:
        for group_id in sorted(fg._groups):
            group_id_mapping[(fg.flow_id, group_id)] = len(group_id_mapping) + 1
    return group_id_mapping


def _add_groups_to_combined_graph(
    flow_graphs: tuple[FlowGraph, ...], combined_graph: FlowGraph, group_id_mapping: dict[tuple[int, int], int]
) -> None:
    """Carry the visual groups; node membership rides on each node's remapped ``group_id``."""
    for fg in flow_graphs:
        for group_id, group in fg._groups.items():
            parent_id = group.parent_group_id
            combined_graph._groups[group_id_mapping[(fg.flow_id, group_id)]] = group.model_copy(
                update={
                    "id": group_id_mapping[(fg.flow_id, group_id)],
                    "parent_group_id": group_id_mapping.get((fg.flow_id, parent_id)) if parent_id is not None else None,
                }
            )


def _create_node_id_mapping(flow_graphs: tuple[FlowGraph, ...]) -> dict[tuple[int, int], int]:
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


def _get_next_available_node_id(flow_graphs: tuple[FlowGraph, ...]) -> int:
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
    flow_graphs: tuple[FlowGraph, ...],
    combined_graph: FlowGraph,
    node_id_mapping: dict[tuple[int, int], int],
    target_flow_id: int,
    group_id_mapping: dict[tuple[int, int], int] | None = None,
) -> None:
    """
    Add all nodes from source graphs to the combined graph.

    Args:
        flow_graphs: Source flow graphs
        combined_graph: Target combined graph
        node_id_mapping: Mapping of node IDs
        target_flow_id: Target flow ID
        group_id_mapping: Mapping of visual group IDs, applied to each node's ``group_id``
    """
    processed_nodes = set()

    for fg in flow_graphs:
        for node in fg.nodes:
            if (fg.flow_id, node.node_id) in processed_nodes:
                continue

            new_node_id = node_id_mapping[(fg.flow_id, node.node_id)]

            setting_input = _create_updated_setting_input(
                node.setting_input, new_node_id, target_flow_id, fg.flow_id, node_id_mapping, group_id_mapping
            )

            _add_node_to_graph(
                combined_graph, new_node_id, target_flow_id, node.node_type, setting_input, source_node=node
            )

            processed_nodes.add((fg.flow_id, node.node_id))


def _create_updated_setting_input(
    original_setting_input: any,
    new_node_id: int,
    target_flow_id: int,
    source_flow_id: int,
    node_id_mapping: dict[tuple[int, int], int],
    group_id_mapping: dict[tuple[int, int], int] | None = None,
) -> any:
    """
    Create an updated setting input with new node and flow IDs.

    Args:
        original_setting_input: Original setting input
        new_node_id: New node ID
        target_flow_id: Target flow ID
        source_flow_id: Source flow ID
        node_id_mapping: Mapping of node IDs
        group_id_mapping: Mapping of visual group IDs

    Returns:
        Updated setting input
    """
    setting_input = deepcopy(original_setting_input)

    if hasattr(setting_input, "node_id"):
        setting_input.node_id = new_node_id

    if hasattr(setting_input, "flow_id"):
        setting_input.flow_id = target_flow_id

    if hasattr(setting_input, "depending_on_id") and setting_input.depending_on_id != -1:
        orig_depending_id = setting_input.depending_on_id
        setting_input.depending_on_id = node_id_mapping.get((source_flow_id, orig_depending_id), -1)

    if hasattr(setting_input, "depending_on_ids"):
        setting_input.depending_on_ids = [
            node_id_mapping.get((source_flow_id, dep_id), -1)
            for dep_id in setting_input.depending_on_ids
            if dep_id != -1
        ]

    if getattr(setting_input, "group_id", None) is not None:
        setting_input.group_id = (group_id_mapping or {}).get((source_flow_id, setting_input.group_id))

    return setting_input


def _add_node_to_graph(
    graph: FlowGraph, node_id: int, flow_id: int, node_type: str, setting_input: any, source_node: any = None
) -> None:
    """
    Add a node to the graph.

    Args:
        graph: Target graph
        node_id: Node ID
        flow_id: Flow ID
        node_type: Node type
        setting_input: Setting input
        source_node: The original FlowNode, for node types whose state cannot be
            rebuilt from settings alone
    """
    if node_type == "polars_lazy_frame" and source_node is not None:
        # No add_polars_lazy_frame(settings) exists — the node's function closes
        # over an in-memory LazyFrame that settings can't carry. Recreate it from
        # the source node's frame, or the merged node stays an unrunnable
        # promise whose result is None.
        result = source_node.get_resulting_data()
        if result is not None:
            frame = result.data_frame
            if isinstance(frame, pl.DataFrame):
                frame = frame.lazy()
            graph.add_dependency_on_polars_lazy_frame(frame, node_id)
            return

    node_promise = input_schema.NodePromise(
        node_id=node_id,
        flow_id=flow_id,
        node_type=node_type,
        is_setup=True,
        pos_x=getattr(setting_input, "pos_x", 0),
        pos_y=getattr(setting_input, "pos_y", 0),
        description=getattr(setting_input, "description", ""),
    )
    is_user_defined = isinstance(setting_input, input_schema.UserDefinedNode) and setting_input.is_user_defined
    if is_user_defined and node_type not in CUSTOM_NODE_STORE:
        register_missing_node_template(node_type)
    graph.add_node_promise(node_promise)
    if source_node is not None and source_node.deferred_until_run:
        # Before placement, so a deferred start node's eager schema prefetch never runs its function.
        graph.get_node(node_id).deferred_until_run = True
    if is_user_defined:
        # Same path as the flow loader: a missing type becomes an error-state placeholder.
        graph._place_user_defined_node(node_type, setting_input)
        return

    add_method_name = f"add_{node_type}"
    if hasattr(graph, add_method_name):
        add_method = getattr(graph, add_method_name)
        add_method(setting_input)


def _add_connections_to_combined_graph(
    flow_graphs: tuple[FlowGraph, ...], combined_graph: FlowGraph, node_id_mapping: dict[tuple[int, int], int]
) -> None:
    """
    Rebuild every edge of the source graphs in the combined graph, per target node.

    Each edge keeps its target slot and the source output handle it reads, so a second
    output (a split filter's fail branch, a gate's else exit) and keyed dynamic inputs
    survive the merge.

    Args:
        flow_graphs: Source flow graphs
        combined_graph: Target combined graph
        node_id_mapping: Mapping of node IDs
    """
    for fg in flow_graphs:
        for target in fg.nodes:
            new_target_id = node_id_mapping.get((fg.flow_id, target.node_id))
            if new_target_id is None:
                continue
            for source, input_type, source_handle in _incoming_edges(target):
                new_source_id = node_id_mapping.get((fg.flow_id, source.node_id))
                if new_source_id is None:
                    continue
                node_connection = input_schema.NodeConnection.create_from_simple_input(
                    from_id=new_source_id, to_id=new_target_id, input_type=input_type, output_handle=source_handle
                )
                add_connection(combined_graph, node_connection)


def _incoming_edges(target: FlowNode) -> list[tuple[FlowNode, str, str]]:
    """``(source, input type or target handle, source handle)`` for every edge into ``target``.

    Static targets follow the flow loader's order (main inputs in order, then left, then
    right); dynamic-input targets list their keyed edges by explicit ``input-N`` handle.
    """
    inputs = target.node_inputs
    if target.accepts_dynamic_inputs:
        source_handles = inputs.keyed_source_handles or {}
        return [
            (source, handle, source_handles.get(handle, DEFAULT_OUTPUT_HANDLE))
            for handle, source in inputs.slot_items()
        ]
    wiring = [("main", source) for source in inputs.main_inputs or []]
    wiring.append(("left", inputs.left_input))
    wiring.append(("right", inputs.right_input))
    return [
        (source, input_type, target._input_output_handles.get(source.node_id, DEFAULT_OUTPUT_HANDLE))
        for input_type, source in wiring
        if source is not None
    ]


def _carry_deferred_seeds(
    flow_graphs: tuple[FlowGraph, ...], combined_graph: FlowGraph, node_id_mapping: dict[tuple[int, int], int]
) -> None:
    """Copy each deferred node's seeded outputs onto its rebuilt node, once its edges exist.

    A rebuilt node starts without a result, so the next build step on the merged graph would
    otherwise execute it (a subflow run, a kernel script, a write of zero rows).
    """
    for fg in flow_graphs:
        for source_node in fg.nodes:
            if not source_node.deferred_until_run:
                continue
            node = combined_graph.get_node(node_id_mapping[(fg.flow_id, source_node.node_id)])
            with node._execution_lock_held():
                node.results.resulting_data = source_node.results.resulting_data
                node._named_outputs = dict(source_node._named_outputs)
                node._named_schemas = dict(source_node._named_schemas)
                node.node_schema.result_schema = source_node.node_schema.result_schema
                node.node_schema.predicted_schema = source_node.node_schema.predicted_schema
                node.deferred_until_run = True
