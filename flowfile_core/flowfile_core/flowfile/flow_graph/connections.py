"""Edge helpers: validate, add, splice, restore and delete connections between nodes."""

from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple

from fastapi.exceptions import HTTPException

from flowfile_core.configs import logger
from flowfile_core.configs.node_store.nodes import get_source_node_types, get_source_node_types_str
from flowfile_core.flowfile.flow_node.flow_node import (
    FlowNode,
)
from flowfile_core.flowfile.flow_node.input_handles import input_handle_index
from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE
from flowfile_core.schemas import input_schema, schemas

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph.graph import FlowGraph


def _would_create_cycle(from_node: FlowNode, to_node: FlowNode) -> bool:
    """True if connecting from_node -> to_node would introduce a cycle.

    A cycle exists if from_node is already reachable downstream of to_node via
    existing `leads_to_nodes` edges, or if the caller is trying to create a
    self-loop.
    """
    if from_node.node_id == to_node.node_id:
        return True
    visited: set = {to_node.node_id}
    stack = [to_node]
    while stack:
        current = stack.pop()
        for downstream in current.leads_to_nodes:
            if downstream.node_id == from_node.node_id:
                return True
            if downstream.node_id not in visited:
                visited.add(downstream.node_id)
                stack.append(downstream)
    return False


class ConnectionValidationError(NamedTuple):
    """Rejected connection (reason code + detail), shared by ``add_connection`` and
    the AI connect handler so both speak the same language."""

    reason: str
    detail: str


def format_source_target_detail(node_id: int, node_type: str | None) -> str:
    """Refusal message for wiring INTO a source node. Lives here (not the AI layer)
    so it stays the single source of truth (the AI handler also calls it for
    freshly-staged targets); ``node_type`` is ``None`` when only the id is known.
    """
    label = f"node {node_id} ({node_type})" if node_type else f"node {node_id}"
    sources = get_source_node_types_str()
    return (
        f"{label} is a source node and has no input port, so it cannot receive a "
        f"connection. Source nodes ({sources}) stand alone. To combine two sources, "
        "ADD a join or union node with both sources as its inputs (the join/union "
        "node is the combine step and wires its own inputs — do not connect the "
        "sources directly); otherwise pick a transformation/output node as the "
        "connection target."
    )


def node_is_source(node: FlowNode) -> bool:
    """True when ``node`` is a source-only node (no input port), via the
    resolved template (``input == 0``), else the registry by ``node_type``.
    """
    template = node.node_template
    if template is not None:
        return template.input == 0
    return node.node_type in get_source_node_types()


def live_source_handle(to_node: FlowNode, from_node_id: int) -> str | None:
    """The output handle a live edge from ``from_node_id`` into ``to_node`` reads.

    Existence comes from the target's actual current inputs, so a bookkeeping
    entry left behind by a deleted edge never answers. Dynamic-input targets
    key their handles per input slot (``keyed_source_handles``) and are exempt.
    """
    if to_node.accepts_dynamic_inputs:
        return None
    if not any(node.node_id == from_node_id for node in to_node.all_inputs):
        return None
    return to_node._input_output_handles.get(from_node_id, DEFAULT_OUTPUT_HANDLE)


def validate_connection(
    from_node: FlowNode,
    to_node: FlowNode,
    output_handle: str | None = None,
) -> ConnectionValidationError | None:
    """Non-mutating topology check for from_node -> to_node; None when valid.

    ``output_handle`` is the source handle the new edge would read. Pass it to
    also refuse double-wiring two different exits of one source into the same
    target: ``_input_output_handles`` keys one handle per source node, so the
    second edge silently re-keys the first and both then read one exit (a
    closed gate side losing its data, or the live side being read twice).
    """
    if node_is_source(to_node):
        return ConnectionValidationError(
            "target_is_source",
            format_source_target_detail(to_node.node_id, to_node.node_type),
        )
    if _would_create_cycle(from_node, to_node):
        return ConnectionValidationError(
            "would_create_cycle",
            f"Connecting node {from_node.node_id} -> {to_node.node_id} would create a cycle",
        )
    if output_handle is not None:
        existing_handle = live_source_handle(to_node, from_node.node_id)
        if existing_handle is not None and existing_handle != output_handle:
            return ConnectionValidationError(
                "source_handle_conflict",
                f"Node {to_node.node_id} already reads node {from_node.node_id} on {existing_handle}, "
                f"so it cannot also read {output_handle} of that node: one target reads a single "
                "output handle per source. Disconnect the existing connection first, or send this "
                "output to a different node.",
            )
    return None


def add_connection(flow: FlowGraph, node_connection: input_schema.NodeConnection) -> None:
    """Adds a connection between two nodes in the flow graph.

    Args:
        flow: The FlowGraph instance to modify.
        node_connection: An object defining the source and target of the connection.
    """
    logger.info("adding a connection")
    flow._note_graph_write()
    from_node = flow.get_node(node_connection.output_connection.node_id)
    to_node = flow.get_node(node_connection.input_connection.node_id)
    logger.info(f"from_node={from_node}, to_node={to_node}")
    if not (from_node and to_node):
        missing = [
            str(nc.node_id)
            for nc, n in (
                (node_connection.output_connection, from_node),
                (node_connection.input_connection, to_node),
            )
            if not n
        ]
        raise HTTPException(404, f"Node(s) not found: {', '.join(missing)}")
    error = validate_connection(from_node, to_node, node_connection.output_connection.connection_class)
    if error is not None:
        raise HTTPException(422, error.detail)
    if to_node.accepts_dynamic_inputs:
        _add_keyed_connection_validated(to_node, from_node, node_connection)
        return
    try:
        insert_type = node_connection.input_connection.get_node_input_connection_type()
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    to_node.add_node_connection(
        from_node,
        insert_type,
        output_handle=node_connection.output_connection.connection_class,
    )


def insert_node_on_edge(flow: FlowGraph, node_id: int, node_connection: input_schema.NodeConnection) -> None:
    """Splice node ``node_id`` into the existing edge A -> B described by ``node_connection``.

    A feeds the node's ``input-0`` through the edge's output handle, and the node's ``output-0``
    takes A's place in B's inputs: the same slot and the same position among B's inputs (see
    ``FlowNode.replace_input_source``). Everything is validated before anything changes.
    """
    flow._note_graph_write()
    source_id = node_connection.output_connection.node_id
    target_id = node_connection.input_connection.node_id
    slot = node_connection.input_connection.connection_class
    source_handle = node_connection.output_connection.connection_class
    node = flow.get_node(node_id)
    if node is None:
        raise HTTPException(404, f"Node {node_id} does not exist")
    from_node, to_node = flow.get_node(source_id), flow.get_node(target_id)
    if from_node is None or to_node is None or to_node.input_edge_handle(source_id, slot) != source_handle:
        raise HTTPException(422, "Connection does not exist on the input node")
    error = validate_connection(node, to_node, DEFAULT_OUTPUT_HANDLE)
    if error is not None:
        raise HTTPException(422, error.detail)
    add_connection(
        flow, input_schema.NodeConnection.create_from_simple_input(source_id, node_id, output_handle=source_handle)
    )
    to_node.replace_input_source(from_node, node, slot, DEFAULT_OUTPUT_HANDLE)


def restore_dynamic_input_connections(graph: FlowGraph, flow_info: schemas.FlowInformation) -> None:
    """Rebuild keyed edges of dynamic-input nodes from serialized ``input_connections``.

    Shared by ``open_flow`` and ``restore_from_snapshot``; the generic wiring pass
    skips dynamic-input targets. Malformed entries are skipped with a warning so a
    hand-edited or stale file degrades to missing edges instead of failing to open.
    """
    for node_id, node_info in flow_info.data.items():
        connections = getattr(node_info, "input_connections", None)
        if not connections:
            continue
        to_node = graph.get_node(node_id)
        if to_node is None or not to_node.accepts_dynamic_inputs:
            continue
        slot_count = len(getattr(to_node.setting_input, "input_slots", None) or [])
        for connection in connections:
            from_node = graph.get_node(connection.from_id)
            if from_node is None:
                logger.warning(f"Node {node_id}: dropping keyed edge from missing node {connection.from_id}")
                continue
            try:
                handle_index = input_handle_index(connection.input_handle)
            except ValueError:
                logger.warning(f"Node {node_id}: dropping keyed edge with invalid handle {connection.input_handle!r}")
                continue
            if handle_index > slot_count:
                # No lead for this never-restored edge; delete_lead_to_node would strip a valid keyed edge.
                logger.warning(
                    f"Node {node_id}: dropping keyed edge on {connection.input_handle} "
                    f"(only {slot_count} input slots)"
                )
                continue
            to_node.add_node_connection(
                from_node,
                "main",
                output_handle=connection.source_handle,
                target_handle=connection.input_handle,
            )


def _add_keyed_connection_validated(
    to_node: FlowNode, from_node: FlowNode, node_connection: input_schema.NodeConnection
) -> None:
    """Connect-API validation for dynamic-input targets: one edge per existing handle."""
    handle = node_connection.input_connection.connection_class
    if isinstance(to_node.setting_input, input_schema.NodePromise):
        raise HTTPException(422, "Configure the node before connecting inputs")
    slot_count = len(getattr(to_node.setting_input, "input_slots", None) or [])
    if input_handle_index(handle) > slot_count:
        raise HTTPException(422, f"Handle {handle} is not available on node {to_node.node_id}")
    if to_node.node_inputs.keyed_inputs and handle in to_node.node_inputs.keyed_inputs:
        raise HTTPException(422, f"Handle {handle} already has a connection")
    to_node.add_node_connection(
        from_node,
        "main",
        output_handle=node_connection.output_connection.connection_class,
        target_handle=handle,
    )


def delete_connection(graph, node_connection: input_schema.NodeConnection):
    """Deletes a connection between two nodes in the flow graph.

    Args:
        graph: The FlowGraph instance to modify.
        node_connection: An object defining the connection to be removed.
    """
    graph._note_graph_write()
    from_node = graph.get_node(node_connection.output_connection.node_id)
    to_node = graph.get_node(node_connection.input_connection.node_id)
    # A stale delete must not raise: an AttributeError 500 drops CORS headers and reads as a CORS error.
    if from_node is None or to_node is None:
        raise HTTPException(422, "Connection does not exist on the input node")
    if to_node.accepts_dynamic_inputs:
        handle = node_connection.input_connection.connection_class
        if not to_node.node_inputs.keyed_connection_exists(handle, from_node.node_id):
            raise HTTPException(422, "Connection does not exist on the input node")
        from_node.delete_lead_to_node(to_node.node_id)
        to_node.delete_input_node(from_node.node_id, connection_type=handle)
        return
    try:
        connection_name = node_connection.input_connection.get_node_input_connection_type()
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    connection_valid = to_node.node_inputs.validate_if_input_connection_exists(
        node_input_id=from_node.node_id,
        connection_name=connection_name,
    )
    if not connection_valid:
        raise HTTPException(422, "Connection does not exist on the input node")
    from_node.delete_lead_to_node(node_connection.input_connection.node_id)
    to_node.delete_input_node(
        node_connection.output_connection.node_id,
        connection_type=node_connection.input_connection.connection_class,
    )
