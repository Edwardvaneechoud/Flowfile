"""Connection ops — connect, delete_connection, delete_node.

Three tightly-coupled handlers + the flat-shape NodeConnection builder
they share. Kept together so the connection-shape contract lives in one
place.
"""

from __future__ import annotations

from typing import Any, Final

from pydantic import ValidationError

from flowfile_core.configs.node_store.nodes import get_source_node_types_str
from flowfile_core.flowfile.flow_graph import (
    add_connection,
    delete_connection,
    format_source_target_detail,
    node_is_source,
    validate_connection,
)
from flowfile_core.schemas import input_schema

from .._internal import (
    ExecutionMode,
    ToolExecutionResult,
    _record_event,
    _reject_and_audit,
)
from ..coercions import _coerce_connection_id_to_flat, _coerce_to_int_or_none
from ..refusals import _detect_sink_upstreams, _format_sink_upstream_refusal

_CONNECTION_FIELD_REWRITES: Final[tuple[tuple[str, str], ...]] = (
    ("input_connection.connection_class", "input_class"),
    ("output_connection.connection_class", "output_class"),
    ("input_connection.node_id", "to_node_id"),
    ("output_connection.node_id", "from_node_id"),
    ("input_connection", "input_class/to_node_id"),
    ("output_connection", "output_class/from_node_id"),
)


def _build_node_connection_from_flat(tool_args: dict[str, Any]) -> input_schema.NodeConnection:
    """Construct a ``NodeConnection`` from the flat shape advertised by the connect ToolSpec.

    The tool spec exposes ``{from_node_id, to_node_id, input_class?, output_class?}``,
    but ``NodeConnection`` is nested. We validate the nested dict at the top level
    (rather than constructing each child independently) so Pydantic emits full
    field paths like ``input_connection.connection_class`` — :func:`_format_connection_validation_error`
    rewrites those back to the flat tool-spec names.

    We avoid ``create_from_simple_input`` because it silently downgrades unrecognised
    input_type values to ``"input-0"`` (input_schema.py: ``create_from_simple_input``).

    Runs ``_coerce_connection_id_to_flat`` first so an LLM emitting
    ``{"connection_id": "5→6"}`` is rewritten to the structured shape
    before validation, instead of burning a retry round on the
    "missing required field" refusal.
    """
    tool_args = _coerce_connection_id_to_flat(tool_args)
    try:
        from_node_id = tool_args["from_node_id"]
        to_node_id = tool_args["to_node_id"]
    except KeyError as exc:
        raise ValueError(f"missing required field: {exc.args[0]}") from None

    return input_schema.NodeConnection.model_validate(
        {
            "input_connection": {
                "node_id": to_node_id,
                "connection_class": tool_args.get("input_class", "input-0"),
            },
            "output_connection": {
                "node_id": from_node_id,
                "connection_class": tool_args.get("output_class", "output-0"),
            },
        }
    )


def _format_connection_validation_error(exc: ValidationError) -> str:
    """Translate Pydantic field paths from internal ``NodeConnection``
    field names back to the flat tool-spec field names (``input_class``,
    ``output_class``, ``from_node_id``, ``to_node_id``). Otherwise the
    retry loop hands the LLM error messages referring to fields it never
    sees in the tool schema.

    Appends a concrete example payload when any error names an integer
    field — when the LLM JSON-string-encodes ids, raw Pydantic prose
    isn't enough; the example shape teaches the corrected call in one
    retry instead of cascading-retry exhaustion.
    """
    parts = []
    int_field_error = False
    for err in exc.errors():
        loc = ".".join(str(p) for p in err["loc"])
        for internal, external in _CONNECTION_FIELD_REWRITES:
            if loc == internal or loc.startswith(internal + "."):
                loc = loc.replace(internal, external, 1)
                break
        # Any int-typed field at the flat-tool surface triggers the example
        # nudge — covers both ``unable to parse string as integer`` and
        # ``Input should be a valid integer``.
        if loc in {"from_node_id", "to_node_id", "flow_id"}:
            int_field_error = True
        parts.append(f"{loc}: {err['msg']}")
    detail = "; ".join(parts)
    if int_field_error:
        detail += (
            ". Example payload: "
            '{"flow_id": 1, "from_node_id": 3, "to_node_id": 5}. '
            "Pass node ids as integers, not JSON-encoded strings."
        )
    return detail


def format_missing_connection_target_detail(node_id: int) -> str:
    """Refusal message for ``connect`` whose target exists nowhere — not in
    the live graph and not among the nodes staged earlier this session.

    The classic failure mode (see the join dogfood): the planner narrates a
    join node it never added (*"connect node 1 → node 4"* where node 4 was
    never created) and tries to wire the sources into that phantom id. The
    message steers toward ADDING the join/union node first — that node *is*
    the combine step, and it wires its own inputs.
    """
    return (
        f"connect: node {node_id} does not exist — it is neither in the live "
        f"graph nor among the nodes you staged earlier this session, so it "
        f"cannot receive a connection. You cannot wire data into a node that "
        f"has not been created yet. If you are combining or joining data "
        f"sources, the join (or union) node IS the combine step: ADD it as a "
        f"NEW node with the two nodes as its inputs "
        f"(``upstream_node_ids`` + ``right_input_node_id`` for a join) — its "
        f"input wiring is created automatically, so do NOT invent a target "
        f"id and connect into it. Otherwise, target an existing node id from "
        f"the live graph."
    )


def _handle_connect(
    tool_name: str,
    tool_args: dict[str, Any],
    redacted_args: dict[str, Any],
    flow,
    session_id: str,
    user_id: int,
    mode: ExecutionMode,
    *,
    audit_meta: dict[str, Any] | None = None,
) -> ToolExecutionResult:
    """Wire a connection between two existing nodes."""
    try:
        connection = _build_node_connection_from_flat(tool_args)
    except ValidationError as exc:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"connection validation failed: {_format_connection_validation_error(exc)}",
        )
    except ValueError as exc:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"connection validation failed: {exc}",
        )

    # Same-id ``connect`` is a self-loop the runtime cycle check at
    # ``flow_graph.add_connection`` would catch later — but only at
    # apply-time (after the diff bundle ships and rolls back). Reject
    # at staging so the LLM gets immediate feedback on the same round
    # and re-emits with valid distinct ids. Mirrors the join-shaped
    # self-loop posture above. Without this guard, an LLM-emitted
    # ``connect 7→7`` aborts the whole diff at apply-time with
    # *"422: Connecting node 7 -> 7 would create a cycle"* and the
    # user has to re-prompt from scratch.
    from_id = connection.output_connection.node_id
    to_id = connection.input_connection.node_id
    if from_id == to_id:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason="self_loop_connection",
            refusal_detail=(
                f"connect: ``from_node_id`` and ``to_node_id`` must be "
                f"different (got both = {from_id}). A node cannot connect "
                "to itself; that would create a cycle. Pick a different "
                "downstream node id for ``to_node_id``."
            ),
        )

    # Refuse LLM-emitted ``connect`` that wires a source-only node
    # staged in this session into a pre-existing live node. The user
    # did not explicitly ask for that wire — narrations like *"so the
    # user can see the new data alongside …"* are rationalisations,
    # not user intent. Example failure mode: planner stages
    # ``add_manual_input`` (id 11) AND ``connect 11→4`` where node 4
    # is a pre-existing ``explore_data``; the wire was never
    # requested and would silently re-route node 4's input.
    # Refusal is per-session and source-only-scoped: legitimate
    # transform-chained wires (e.g. add_filter then connect) and
    # live→live wires are unaffected. Once the user accepts the diff
    # the staged id transitions to live (via
    # ``revalidate_staged_results_against_live``) and this guard
    # naturally stops firing.
    staged_source_ids_meta = (audit_meta or {}).get("staged_source_node_ids_at_stage") or []
    staged_source_ids = {sid for sid in staged_source_ids_meta if isinstance(sid, int)}
    staged_ids_meta = (audit_meta or {}).get("staged_node_ids_at_stage") or []
    staged_ids = {sid for sid in staged_ids_meta if isinstance(sid, int)}
    if staged_source_ids and from_id in staged_source_ids and to_id not in staged_ids:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason="unrequested_wire_to_live",
            refusal_detail=(
                f"connect: refusing to wire freshly-staged source node "
                f"{from_id} into pre-existing live node {to_id}. The user "
                f"did not explicitly ask for this connection — narrations "
                f"like \"so the user can see the new data alongside …\" "
                f"are rationalisations, not user intent. Source-only "
                f"types ({get_source_node_types_str()}) are stand-"
                f"alone by default. If the user explicitly asked for "
                f"this wiring (e.g. \"connect the new node to node "
                f"{to_id}\"), restate that intent in your next assistant "
                f"message and re-emit; otherwise leave the new node "
                f"stand-alone and end the turn."
            ),
            refusal_detail_short=(
                f"Refused: the user did not ask to wire node {from_id} "
                f"into node {to_id}. Source-only nodes stand alone; do "
                f"not re-emit this connect. Either end the turn via "
                f"``classify_intent(op_kind=\"other\")`` or stage the "
                f"next user-requested op."
            ),
        )

    # Refuse explicit ``connect`` calls whose upstream side is a sink.
    # ``output_connection.node_id`` is the FROM (upstream) side; if it
    # has no output port, the connection is a static error.
    upstream_id = connection.output_connection.node_id
    sink_upstreams = _detect_sink_upstreams(flow, [upstream_id])
    if sink_upstreams:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason="upstream_is_sink",
            refusal_detail=_format_sink_upstream_refusal(flow, sink_upstreams),
        )

    # Refuse wiring INTO a source-only node (read/manual_input/...) and reject
    # live-graph cycles BEFORE staging — mirroring how ``_handle_add_node``
    # validates settings before its own ``if mode == "stage"``. The backend
    # ``add_connection`` enforces both, but stage mode never calls it, so without
    # this the LLM only learns of a ``read → read`` (or cycle) at apply-time, after
    # the planner loop ended and can no longer self-correct.
    #
    # The source-target check must fire whether the target is freshly-staged
    # (no live FlowNode yet — use the ``staged_source_ids`` set above) OR a live
    # node, and independent of whether the FROM side is live: a source has no
    # input port either way. The cycle check, by contrast, needs both endpoints
    # live (a staged→staged cycle is an acceptable gap, caught at apply-time by
    # add_connection).
    to_node = flow.get_node(to_id)
    if to_id in staged_source_ids or (to_node is not None and node_is_source(to_node)):
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason="target_is_source",
            refusal_detail=format_source_target_detail(
                to_id, to_node.node_type if to_node is not None else None
            ),
        )

    # Refuse ``connect`` whose target exists nowhere — not live, not staged
    # this session. The dogfood failure: the planner narrates a join node it
    # never added (``connect 1 → 4`` where node 4 was never created) and
    # wires the sources into that phantom id. Without this guard, stage mode
    # silently stages a dangling edge that only blows up at apply_diff, and
    # apply mode surfaces a raw ``404 Node not found`` the LLM reads as "I
    # can't create nodes" (then gives up). Reject early, after the
    # source-target check (so a staged-source target still reports
    # ``target_is_source``), with an actionable steer toward ADDING the join.
    if to_node is None and to_id not in staged_ids:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason="target_not_found",
            refusal_detail=format_missing_connection_target_detail(to_id),
        )

    from_node = flow.get_node(from_id)
    if from_node is not None and to_node is not None:
        connection_error = validate_connection(from_node, to_node)
        if connection_error is not None:
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow.flow_id,
                refusal_reason=connection_error.reason,
                refusal_detail=connection_error.detail,
            )

    if mode == "stage":
        audit_row = _record_event(
            session_id=session_id,
            user_id=user_id,
            tool_name=tool_name,
            flow_id=flow.flow_id,
            tool_args=redacted_args,
            result_status="success",
        )
        return ToolExecutionResult(
            status="staged",
            tool_name=tool_name,
            audit_id=audit_row.id if audit_row is not None else None,
            executed_args=redacted_args,
            staged_node_payload={"connection": connection.model_dump()},
        )

    # LLM-redundant-op tolerance: if the wire is already present (e.g. an
    # ``update_node_settings`` in the same diff already implicitly rewired
    # via ``add_node_step``'s ``input_node_ids`` derivation), treat as
    # applied no-op. Core ``add_connection`` is strict for non-AI callers;
    # tolerance lives here at the AI seam.
    from_node = flow.get_node(from_id)
    to_node = flow.get_node(to_id)
    already_present = False
    if from_node is not None and to_node is not None:
        try:
            already_present = to_node.node_inputs.validate_if_input_connection_exists(
                node_input_id=from_node.node_id,
                connection_name=connection.input_connection.get_node_input_connection_type(),
            )
        except Exception:
            already_present = False
    if not already_present:
        try:
            add_connection(flow, connection)
        except Exception as exc:
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow.flow_id,
                refusal_reason=None,
                refusal_detail=f"connect failed: {exc}",
            )

    audit_row = _record_event(
        session_id=session_id,
        user_id=user_id,
        tool_name=tool_name,
        flow_id=flow.flow_id,
        tool_args=redacted_args,
        result_status="success",
    )
    return ToolExecutionResult(
        status="applied",
        tool_name=tool_name,
        audit_id=audit_row.id if audit_row is not None else None,
        executed_args=redacted_args,
    )


def _handle_delete_node(
    tool_name: str,
    tool_args: dict[str, Any],
    redacted_args: dict[str, Any],
    flow,
    session_id: str,
    user_id: int,
    mode: ExecutionMode,
) -> ToolExecutionResult:
    node_id_raw = tool_args.get("node_id")
    node_id = _coerce_to_int_or_none(node_id_raw)
    if node_id is None:
        got_type = type(node_id_raw).__name__
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=(
                f"delete_node requires an integer node_id (got {got_type}). "
                'Example payload: {"flow_id": 1, "node_id": 5}. '
                "Pass node ids as integers, not JSON-encoded strings."
            ),
        )

    if mode == "stage":
        audit_row = _record_event(
            session_id=session_id,
            user_id=user_id,
            tool_name=tool_name,
            flow_id=flow.flow_id,
            tool_args=redacted_args,
            result_status="success",
        )
        return ToolExecutionResult(
            status="staged",
            tool_name=tool_name,
            audit_id=audit_row.id if audit_row is not None else None,
            executed_args=redacted_args,
            staged_node_payload={"delete_node_id": node_id},
        )

    try:
        flow.delete_node(node_id)
    except Exception as exc:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"delete_node failed: {exc}",
        )

    audit_row = _record_event(
        session_id=session_id,
        user_id=user_id,
        tool_name=tool_name,
        flow_id=flow.flow_id,
        tool_args=redacted_args,
        result_status="success",
    )
    return ToolExecutionResult(
        status="applied",
        tool_name=tool_name,
        node_id=node_id,
        audit_id=audit_row.id if audit_row is not None else None,
        executed_args=redacted_args,
    )


def _handle_delete_connection(
    tool_name: str,
    tool_args: dict[str, Any],
    redacted_args: dict[str, Any],
    flow,
    session_id: str,
    user_id: int,
    mode: ExecutionMode,
) -> ToolExecutionResult:
    try:
        connection = _build_node_connection_from_flat(tool_args)
    except ValidationError as exc:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"connection validation failed: {_format_connection_validation_error(exc)}",
        )
    except ValueError as exc:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"connection validation failed: {exc}",
        )

    if mode == "stage":
        audit_row = _record_event(
            session_id=session_id,
            user_id=user_id,
            tool_name=tool_name,
            flow_id=flow.flow_id,
            tool_args=redacted_args,
            result_status="success",
        )
        return ToolExecutionResult(
            status="staged",
            tool_name=tool_name,
            audit_id=audit_row.id if audit_row is not None else None,
            executed_args=redacted_args,
            staged_node_payload={"delete_connection": connection.model_dump()},
        )

    from fastapi import HTTPException

    try:
        delete_connection(flow, connection)
    except HTTPException as exc:
        # LLM-redundant-op tolerance: swallow only the "Connection does not
        # exist" 422 (an ``update_node_settings`` in the same diff already
        # implicitly rewired, so this delete targets a wire that's already
        # gone). Other HTTPExceptions and any non-HTTPException propagate
        # to the rejection path below. Core stays strict for non-AI callers.
        if exc.status_code == 422 and "Connection does not exist" in str(exc.detail):
            audit_row = _record_event(
                session_id=session_id,
                user_id=user_id,
                tool_name=tool_name,
                flow_id=flow.flow_id,
                tool_args=redacted_args,
                result_status="success",
            )
            return ToolExecutionResult(
                status="applied",
                tool_name=tool_name,
                audit_id=audit_row.id if audit_row is not None else None,
                executed_args=redacted_args,
            )
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"delete_connection failed: {exc}",
        )
    except Exception as exc:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"delete_connection failed: {exc}",
        )

    audit_row = _record_event(
        session_id=session_id,
        user_id=user_id,
        tool_name=tool_name,
        flow_id=flow.flow_id,
        tool_args=redacted_args,
        result_status="success",
    )
    return ToolExecutionResult(
        status="applied",
        tool_name=tool_name,
        audit_id=audit_row.id if audit_row is not None else None,
        executed_args=redacted_args,
    )
