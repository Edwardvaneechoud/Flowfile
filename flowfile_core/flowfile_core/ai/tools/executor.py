"""Tool executor with prospective schema validation — W31.

Per plan §6.3, ``execute_tool_call`` is the single entry point for the LLM's
typed tool calls. The executor:

1. Parses the MCP-shaped tool name (``flowfile.<domain>.<op>``).
2. Resolves the target ``FlowGraph`` via ``flow_file_handler.get_flow``.
3. Dispatches per-domain (``graph`` / ``schema`` / ``codegen`` / ``meta``).
4. For ``graph.add_<node_type>``: validates settings via the Pydantic class,
   refuses on network egress (W25 §9.6), resolves upstream schemas via the
   D011 tier handler, validates column refs, predicts output schema (mirror
   for static/source/passthrough; kernel dry-run for dynamic — D003), then
   either applies via ``getattr(flow, f"add_{node_type}")(settings)`` or
   stages the payload for W41 to compose into a GraphDiff.
5. Emits one :class:`AuditEvent` per call (W15) with secrets redacted.

The executor does NOT do its own ``pl.scan_*`` calls — per the project rule
"the collect of polars data only takes place in the worker — use nodes
already", all data-touching paths go through the existing ``add_<node_type>``
infrastructure (which is worker-aware) or through ``kernel_runtime``.

Coordination with W41: ``mode="stage"`` returns ``staged_node_payload`` with
the validated settings + predicted schema; W41 composes a list of those into
a ``GraphDiff`` and wires the accept path through
``HistoryManager.capture_if_changed`` for a single undo point. W31 does NOT
call ``audit.update_diff_action`` — that's W41's contract.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Final, Literal

from pydantic import BaseModel, Field, ValidationError

from flowfile_core.ai import audit, safety
from flowfile_core.ai.tools.classification import classify_node_type
from flowfile_core.ai.tools.dry_run import DryRunCache, dry_run_code
from flowfile_core.ai.tools.node_docs import NODE_AGENT_PAYLOAD_EXAMPLES
from flowfile_core.ai.tools.predictor import (
    _resolve_upstream_schemas,
    collect_column_refs,
    predict_schema_via_mirror,
    schema_to_dict_list,
)
from flowfile_core.ai.tools.meta_ops import OP_KIND_NAMES
from flowfile_core.ai.tools.registry import _inline_ref_schema
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.schemas import NODE_TYPE_TO_SETTINGS_CLASS, get_settings_class_for_node_type

logger = logging.getLogger(__name__)

ExecutionMode = Literal["apply", "stage"]
ResultStatus = Literal["applied", "staged", "warned", "rejected"]

_TOOL_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^flowfile\.(graph|schema|codegen|meta)\.(.+)$")

#: Settings field paths for code-bearing nodes — used for the network-egress
#: check (§9.6). Each tuple is ``(node_type, attr_path)``.
_CODE_BEARING: Final[dict[str, tuple[str, ...]]] = {
    "polars_code": ("polars_code_input", "polars_code"),
    "python_script": ("python_script_input", "code"),
    "sql_query": ("sql_query_input", "sql_code"),
}


#: Layout offsets used by :func:`_resolve_insertion_position` (W62) when
#: ``InsertionContext.pos_x`` / ``pos_y`` are unset. Mirrors the canonical
#: spacings of :func:`flowfile_core.flowfile.util.calculate_layout.calculate_layered_layout`
#: (``x_spacing=250, y_spacing=100, initial_y=50``) so AI-staged nodes lay out
#: with the same density the auto-layout helper would produce.
_AUTO_LAYOUT_X_SPACING: Final[float] = 250.0
_AUTO_LAYOUT_Y_SPACING: Final[float] = 100.0
_AUTO_LAYOUT_FALLBACK_X: Final[float] = 50.0
_AUTO_LAYOUT_FALLBACK_Y: Final[float] = 50.0


class InsertionContext(BaseModel):
    """Where a new node attaches to the existing graph.

    ``upstream_node_ids`` are connected to ``input-0`` (main); the optional
    ``right_input_node_id`` is connected to ``input-1`` (right) for joins.

    ``pos_x`` / ``pos_y`` may be ``None`` to ask the executor to derive a
    layout position from the upstream's canvas coordinates (W62). When the
    caller wants (0, 0) literally, it must pass ``0.0`` explicitly — the
    sentinel-vs-default distinction is the whole point of the ``None`` shape.
    """

    upstream_node_ids: list[int] = Field(default_factory=list)
    right_input_node_id: int | None = None
    pos_x: float | None = None
    pos_y: float | None = None


def _resolve_insertion_position(
    flow,
    upstream_node_ids: list[int],
    *,
    staged_offset_index: int = 0,
    extra_upstream_positions: dict[int, tuple[float, float]] | None = None,
) -> tuple[float, float]:
    """Derive ``(pos_x, pos_y)`` for an AI-staged node from the live graph.

    The most-recent upstream node anchors the new node; the helper offsets
    horizontally by :data:`_AUTO_LAYOUT_X_SPACING` and vertically by
    ``staged_offset_index * _AUTO_LAYOUT_Y_SPACING``. ``staged_offset_index``
    is the count of prior in-batch staged adds anchored at the same upstream;
    callers (planner / Cmd+K) thread it so fan-outs from one upstream stack
    instead of overlapping.

    ``extra_upstream_positions`` is a caller-supplied lookup
    ``{node_id: (pos_x, pos_y)}`` consulted before the live graph. The
    planner threads its in-batch staged-but-unapplied adds through here
    because chained transformations (filter → sort) anchor on the prior
    staged add, which by definition hasn't been applied to ``flow.nodes``
    yet.

    Cold flow (``upstream_node_ids`` empty or no live upstream resolves):
    fall back to ``(_AUTO_LAYOUT_FALLBACK_X, _AUTO_LAYOUT_FALLBACK_Y)``
    plus the staged-offset y-stack so multiple cold-flow adds in one batch
    don't collapse onto each other either.

    The helper reads ``setting_input.pos_x`` / ``pos_y`` off
    :class:`flowfile_core.flowfile.flow_node.flow_node.FlowNode` because that
    is the persistent canvas position carried on every node settings model
    (``NodeBase.pos_x`` / ``pos_y``). Live ``node_information`` mirrors the
    same value but only as ``int``.
    """
    upstream_pos: tuple[float, float] | None = None
    for uid in reversed(upstream_node_ids):
        if uid is None:
            continue
        if extra_upstream_positions and uid in extra_upstream_positions:
            cand = extra_upstream_positions[uid]
            if (
                isinstance(cand, tuple)
                and len(cand) == 2
                and isinstance(cand[0], int | float)
                and isinstance(cand[1], int | float)
            ):
                upstream_pos = (float(cand[0]), float(cand[1]))
                break
        node = flow.get_node(uid)
        if node is None:
            continue
        setting_input = getattr(node, "setting_input", None)
        if setting_input is None:
            continue
        ux = getattr(setting_input, "pos_x", None)
        uy = getattr(setting_input, "pos_y", None)
        if isinstance(ux, int | float) and isinstance(uy, int | float):
            upstream_pos = (float(ux), float(uy))
            break

    if upstream_pos is None:
        return (
            _AUTO_LAYOUT_FALLBACK_X,
            _AUTO_LAYOUT_FALLBACK_Y + staged_offset_index * _AUTO_LAYOUT_Y_SPACING,
        )

    base_x, base_y = upstream_pos
    return (
        base_x + _AUTO_LAYOUT_X_SPACING,
        base_y + staged_offset_index * _AUTO_LAYOUT_Y_SPACING,
    )


class ToolExecutionResult(BaseModel):
    """Outcome of one ``execute_tool_call``.

    * ``applied`` — node was added/wired to the real graph (mode=apply, no
      D011 warnings).
    * ``staged`` — settings + predicted schema captured for W41 (mode=stage),
      no real-graph mutation.
    * ``warned`` — tool ran but produced D011 warnings (e.g. upstream schema
      unknown; column-ref validation deferred until run).
    * ``rejected`` — refusal (Pydantic validation, network egress, unknown
      columns, ...).
    """

    status: ResultStatus
    tool_name: str
    node_id: int | None = None
    predicted_output_schema: list[dict[str, Any]] | None = None
    refusal_reason: safety.RefusalReason | None = None
    refusal_detail: str | None = None
    warnings: list[str] = Field(default_factory=list)
    audit_id: int | None = None
    executed_args: dict[str, Any] | None = None
    staged_node_payload: dict[str, Any] | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


def execute_tool_call(
    *,
    flow_id: int,
    tool_name: str,
    tool_args: dict[str, Any],
    insertion_context: InsertionContext,
    session_id: str,
    user_id: int,
    mode: ExecutionMode = "apply",
    flow=None,
    dry_run_cache: DryRunCache | None = None,
    llm_provided_node_id: int | None = None,
    audit_meta: dict[str, Any] | None = None,
    staged_offset_index: int = 0,
    extra_upstream_positions: dict[int, tuple[float, float]] | None = None,
    extra_upstream_schemas: dict[int, Any] | None = None,
) -> ToolExecutionResult:
    """Validate, predict, and dispatch a single LLM tool call.

    ``flow`` is optional — if not provided, looked up via
    ``flow_file_handler.get_flow(flow_id)``. Tests can pass an explicit flow
    to avoid touching the global handler.

    ``dry_run_cache`` is the per-session :class:`DryRunCache`. If ``None``, a
    fresh cache is created (no cross-call hit). Long-running planner sessions
    should reuse one cache so identical proposals don't re-pay the kernel cost.

    ``llm_provided_node_id`` and ``audit_meta`` are W54 surfaces. The planner
    sets ``llm_provided_node_id`` when the LLM emitted ``node_id`` itself
    (instead of letting the planner allocate); the executor then validates
    the id is fresh + non-self-looping. ``audit_meta`` rides on every
    ``add_*`` audit row under ``tool_args["__planner_meta__"]`` so future
    self-loops are diagnosable from the audit row alone (the existing
    ``AuditEvent.extra`` field is dropped before persistence — see
    plan §6).

    ``staged_offset_index`` is W62 — the count of prior in-batch staged adds
    anchored at the same upstream. Callers (planner / Cmd+K) thread it so
    fan-outs from one upstream stack vertically instead of overlapping. Only
    consulted when ``insertion_context.pos_x`` / ``pos_y`` are both ``None``.

    ``extra_upstream_positions`` is W62 — a caller-supplied
    ``{node_id: (pos_x, pos_y)}`` map merged into the upstream lookup before
    the live graph. The planner uses this to anchor chained adds onto prior
    in-batch staged-but-unapplied upstreams (which by definition aren't in
    ``flow.nodes`` yet).
    """
    # W71 v1.4 — universal lenient JSON-string unwrap. Smaller open-weights
    # models (llama-3.3-70b, in particular) routinely pass structured
    # tool args as JSON-encoded strings rather than native objects /
    # arrays / ints. ``upstream_node_ids: "[3]"``, ``groupby_input:
    # "{\"agg_cols\": ...}"``, ``node_id: "5"`` — Pydantic strict mode
    # rejects each of these and burns retry budget on a recoverable
    # type-wrap mistake. Apply the unwrap pass at the top of dispatch so
    # every handler (add_*, update_node_settings, the meta ops, schema
    # ops) gets the same forgiveness uniformly. See
    # :func:`_unwrap_json_string_values` for the heuristic + safety
    # guards (free-form code bodies / Polars expressions are never
    # mangled because they don't start with ``[`` / ``{`` / a digit).
    if tool_args:
        tool_args = _unwrap_json_string_values(tool_args)
    redacted_args = safety.redact_secrets(tool_args) if tool_args else {}

    match = _TOOL_NAME_RE.match(tool_name)
    if match is None:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow_id,
            refusal_reason=None,
            refusal_detail=f"invalid tool name: {tool_name!r}",
        )

    domain, op = match.group(1), match.group(2)

    if flow is None:
        flow = _resolve_flow(flow_id)
        if flow is None:
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow_id,
                refusal_reason=None,
                refusal_detail=f"flow {flow_id} not found",
            )

    if dry_run_cache is None:
        dry_run_cache = DryRunCache()

    if domain == "graph":
        return _handle_graph(
            op=op,
            tool_name=tool_name,
            tool_args=tool_args,
            redacted_args=redacted_args,
            insertion_context=insertion_context,
            flow=flow,
            session_id=session_id,
            user_id=user_id,
            mode=mode,
            dry_run_cache=dry_run_cache,
            llm_provided_node_id=llm_provided_node_id,
            audit_meta=audit_meta,
            staged_offset_index=staged_offset_index,
            extra_upstream_positions=extra_upstream_positions,
            extra_upstream_schemas=extra_upstream_schemas,
        )

    if domain == "schema":
        return _handle_schema(
            op=op,
            tool_name=tool_name,
            tool_args=tool_args,
            redacted_args=redacted_args,
            flow=flow,
            session_id=session_id,
            user_id=user_id,
        )

    if domain == "codegen":
        return _handle_codegen(
            op=op,
            tool_name=tool_name,
            redacted_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
        )

    if domain == "meta":
        return _handle_meta(
            op=op,
            tool_name=tool_name,
            tool_args=tool_args,
            redacted_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
        )

    return _reject_and_audit(
        tool_name=tool_name,
        tool_args=redacted_args,
        session_id=session_id,
        user_id=user_id,
        flow_id=flow.flow_id,
        refusal_reason=None,
        refusal_detail=f"unknown domain: {domain!r}",
    )


def _resolve_flow(flow_id: int):
    """Look up a ``FlowGraph`` via the global handler. Lazy import to keep the
    executor's module-level imports light (the handler pulls in DB session
    machinery which other AI tests don't need)."""
    from flowfile_core.flowfile.handler import flow_file_handler

    return flow_file_handler.get_flow(flow_id)


def _handle_graph(
    *,
    op: str,
    tool_name: str,
    tool_args: dict[str, Any],
    redacted_args: dict[str, Any],
    insertion_context: InsertionContext,
    flow,
    session_id: str,
    user_id: int,
    mode: ExecutionMode,
    dry_run_cache: DryRunCache,
    llm_provided_node_id: int | None = None,
    audit_meta: dict[str, Any] | None = None,
    staged_offset_index: int = 0,
    extra_upstream_positions: dict[int, tuple[float, float]] | None = None,
    extra_upstream_schemas: dict[int, Any] | None = None,
) -> ToolExecutionResult:
    if op.startswith("add_"):
        node_type = op[len("add_") :]
        return _handle_add_node(
            node_type=node_type,
            tool_name=tool_name,
            tool_args=tool_args,
            redacted_args=redacted_args,
            insertion_context=insertion_context,
            flow=flow,
            session_id=session_id,
            user_id=user_id,
            mode=mode,
            dry_run_cache=dry_run_cache,
            llm_provided_node_id=llm_provided_node_id,
            audit_meta=audit_meta,
            staged_offset_index=staged_offset_index,
            extra_upstream_positions=extra_upstream_positions,
            extra_upstream_schemas=extra_upstream_schemas,
        )

    if op == "connect":
        return _handle_connect(tool_name, tool_args, redacted_args, flow, session_id, user_id, mode)
    if op == "delete_node":
        return _handle_delete_node(tool_name, tool_args, redacted_args, flow, session_id, user_id, mode)
    if op == "delete_connection":
        return _handle_delete_connection(tool_name, tool_args, redacted_args, flow, session_id, user_id, mode)
    if op == "update_node_settings":
        return _handle_update_node_settings(
            tool_name=tool_name,
            tool_args=tool_args,
            redacted_args=redacted_args,
            flow=flow,
            session_id=session_id,
            user_id=user_id,
            mode=mode,
            dry_run_cache=dry_run_cache,
            extra_upstream_schemas=extra_upstream_schemas,
        )

    # ``run_node`` / ``propose_subgraph`` were removed from the catalog in
    # W46 (graph_ops.py 2026-05-05) and stay out — autonomous run-node is
    # unsafe (worker collects, user code, external systems); propose_subgraph
    # is redundant with W40's per-step staging. This rejection branch stays
    # as defence-in-depth in case a future workstream re-adds either before
    # wiring an implementation.
    return _reject_and_audit(
        tool_name=tool_name,
        tool_args=redacted_args,
        session_id=session_id,
        user_id=user_id,
        flow_id=flow.flow_id,
        refusal_reason=None,
        refusal_detail=f"graph op {op!r} is not in the agent's catalog",
    )


def _navigate_schema(schema: dict[str, Any], loc_parts: list[str]) -> dict[str, Any] | None:
    """Walk into a JSON Schema following a Pydantic ``loc`` path.

    Pydantic ``loc`` is a tuple like ``("raw_data_format", "columns", 0, "name")``.
    Schema navigation: property names dive into ``properties[name]``; integer
    indices indicate the failing array element so we step into ``items``;
    schema branches under ``anyOf`` are flattened by picking the first
    object-typed branch.
    """
    node: dict[str, Any] | None = schema
    for part in loc_parts:
        if node is None:
            return None
        if "anyOf" in node:
            object_branches = [b for b in node["anyOf"] if isinstance(b, dict) and b.get("type") == "object"]
            if object_branches:
                node = object_branches[0]
        if isinstance(part, int):
            node = node.get("items") if isinstance(node, dict) else None
            continue
        if isinstance(part, str):
            properties = node.get("properties") if isinstance(node, dict) else None
            if properties and part in properties:
                node = properties[part]
                continue
            return None
        return None
    if node is not None and "anyOf" in node:
        object_branches = [b for b in node["anyOf"] if isinstance(b, dict) and b.get("type") == "object"]
        if object_branches:
            node = object_branches[0]
    return node


def _summarize_expected_shape(field_schema: dict[str, Any] | None) -> str:
    """Render a JSON Schema fragment as a short human-readable shape summary."""
    if not field_schema:
        return "the value documented in the catalog"
    title = field_schema.get("title")
    type_ = field_schema.get("type")
    if type_ == "object":
        return f"an object ({title})" if title else "an object"
    if type_ == "array":
        items = field_schema.get("items") or {}
        items_type = items.get("type")
        if items_type == "object":
            items_title = items.get("title")
            return f"an array of objects ({items_title})" if items_title else "an array of objects"
        if items_type:
            return f"an array of {items_type}"
        return "an array"
    if isinstance(type_, str):
        return f"a {type_}"
    return "the value documented in the catalog"


def _expects_object(field_schema: dict[str, Any] | None) -> bool:
    """Return True iff the field expects an object (top-level or via array items)."""
    if not field_schema:
        return False
    if field_schema.get("type") == "object":
        return True
    if field_schema.get("type") == "array":
        items = field_schema.get("items") or {}
        return items.get("type") == "object"
    return False


_PRIMITIVE_DEFAULTS: Final[dict[str, Any]] = {
    "string": "",
    "integer": 0,
    "number": 0.0,
    "boolean": False,
    "null": None,
}


def _synthesize_example_from_schema(
    schema: dict[str, Any] | None,
    *,
    depth: int = 0,
    max_depth: int = 5,
) -> Any:
    """Synthesize a structurally-faithful placeholder for a JSON-Schema fragment.

    Used in W67 settings-validation refusals to give the LLM a template payload
    it can pattern-match on. Required object fields are filled; optional fields
    are skipped to keep the example minimal. Cycle / depth bound prevents
    runaway recursion on self-referential schemas.
    """
    if schema is None or depth >= max_depth:
        return None

    if "anyOf" in schema:
        object_branches = [b for b in schema["anyOf"] if isinstance(b, dict) and b.get("type") == "object"]
        if object_branches:
            return _synthesize_example_from_schema(object_branches[0], depth=depth, max_depth=max_depth)
        for branch in schema["anyOf"]:
            if isinstance(branch, dict) and branch.get("type") not in (None, "null"):
                return _synthesize_example_from_schema(branch, depth=depth, max_depth=max_depth)
        return None

    if "enum" in schema:
        enum_values = schema["enum"]
        if enum_values:
            return enum_values[0]

    if "default" in schema:
        return schema["default"]

    type_ = schema.get("type")
    if type_ == "object":
        properties = schema.get("properties") or {}
        required = schema.get("required") or list(properties.keys())[:2]
        result: dict[str, Any] = {}
        for key in required:
            sub = properties.get(key)
            value = _synthesize_example_from_schema(sub, depth=depth + 1, max_depth=max_depth)
            if value is not None or (sub and sub.get("type") == "null"):
                result[key] = value
            else:
                result[key] = ""
        return result
    if type_ == "array":
        items = schema.get("items")
        if isinstance(items, dict) and items.get("type") == "object":
            return [_synthesize_example_from_schema(items, depth=depth + 1, max_depth=max_depth)]
        return []
    if isinstance(type_, str) and type_ in _PRIMITIVE_DEFAULTS:
        return _PRIMITIVE_DEFAULTS[type_]
    return None


def _example_from_payload(node_type: str, loc_parts: list[str]) -> Any:
    """Try to extract the failing-field fragment from
    :data:`NODE_AGENT_PAYLOAD_EXAMPLES` (per the spec's preferred cascade).

    Returns ``None`` if no payload is registered for this node type or if the
    loc path doesn't resolve cleanly inside it.
    """
    payload_json = NODE_AGENT_PAYLOAD_EXAMPLES.get(node_type)
    if not payload_json:
        return None
    try:
        payload = json.loads(payload_json)
    except json.JSONDecodeError:
        return None
    node: Any = payload
    for part in loc_parts:
        if isinstance(part, str) and isinstance(node, dict) and part in node:
            node = node[part]
            continue
        if isinstance(part, int) and isinstance(node, list) and 0 <= part < len(node):
            node = node[part]
            continue
        return None
    return node


def _coerce_to_int_or_none(value: Any) -> int | None:
    """Best-effort coercion of a ``node_id``-shaped value to ``int``.

    The Pydantic validator at the ``add_*`` and ``connect`` paths runs in
    lax mode and silently coerces ``"5"`` → ``5``; the manually-validated
    paths (``delete_node`` / ``update_node_settings`` / ``read_node_*``)
    historically refused outright on any non-``int`` shape, which made the
    cross-tool experience inconsistent — the LLM would correct on one
    tool and immediately repeat the same mistake on another. This helper
    closes that gap: numeric-looking strings get coerced; truly bogus
    inputs (``"node_5"``, ``None``, lists) return ``None`` so the caller
    can refuse with a structured detail. ``bool`` is rejected explicitly
    because Python treats it as ``int`` subclass and ``True`` / ``False``
    would otherwise sneak through as ``1`` / ``0``.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _detect_sink_upstreams(flow, upstream_ids: list[int]) -> list[tuple[int, str]]:
    """Return ``(node_id, node_type)`` for any upstream id that resolves to a
    sink in ``flow.nodes`` (i.e. ``NodeTemplate.output == 0``).

    Sinks (``explore_data`` / ``output`` / ``database_writer`` /
    ``cloud_storage_writer`` / ``catalog_writer``) consume data and have no
    output port, so wiring a downstream node to one is a static error. The
    LLM occasionally proposes this when chat history doesn't disambiguate
    and the resolver Tier-6 fallback (post-2026-05-07 fix) skips sinks —
    this guard catches the explicit-Tier-1 case where the LLM names a sink
    in ``upstream_node_ids`` directly.

    Ids absent from ``flow.nodes`` (i.e. staged-this-session or invalid)
    are silently skipped here; the missing-id case is handled by downstream
    refusal stages, not this guard.
    """
    sinks: list[tuple[int, str]] = []
    for uid in upstream_ids:
        upstream_node = flow.get_node(uid)
        if upstream_node is None:
            continue
        template = getattr(upstream_node, "node_template", None)
        if template is not None and getattr(template, "output", 1) == 0:
            sinks.append((uid, upstream_node.node_type))
    return sinks


def _format_sink_upstream_refusal(flow, sink_upstreams: list[tuple[int, str]]) -> str:
    """Build the refusal detail string for a sink-upstream rejection.

    Lists each offending id + its node type and proposes the live non-sink
    candidates so the LLM can retry with a corrected ``upstream_node_ids``
    on the next turn (planner re-feeds refusal_detail back as a tool message).
    """
    sink_str = ", ".join(f"{nid} ({nt})" for nid, nt in sink_upstreams)
    non_sinks: list[int] = []
    for live in flow.nodes:
        live_template = getattr(live, "node_template", None)
        if live_template is not None and getattr(live_template, "output", 1) > 0:
            try:
                non_sinks.append(int(live.node_id))
            except (TypeError, ValueError, AttributeError):
                continue
    candidates = sorted(set(non_sinks))
    return (
        f"upstream node(s) {sink_str} are sinks (no output port) and cannot "
        f"have downstream nodes — sink types consume data, they don't produce "
        f"it. Non-sink candidates available: {candidates}. Pick a "
        f"transformation node and retry."
    )


def _format_settings_validation_refusal(
    *,
    exc: ValidationError,
    settings_cls: type[BaseModel],
    node_type: str,
) -> str:
    """Translate a Pydantic ``ValidationError`` on a settings class into a
    course-correctable refusal detail.

    Rationale (W67 Defect 2): the bare ``str(exc)`` is a stack-shaped string
    the LLM treats as opaque. Live transcript 2026-05-06 showed the agent
    looping 3× on ``raw_data_format`` because it never learned the field is
    structured. The translated message names the failing field, the expected
    shape (from the inlined catalog schema), the received Python type, and
    embeds a concrete example payload — same field-and-shape contract W53
    landed for the connection-validation site.
    """
    errors = exc.errors()
    if not errors:
        return f"settings validation failed: {exc}"

    first = errors[0]
    raw_loc = first.get("loc", ())
    loc_parts: list = list(raw_loc)
    loc_str = ".".join(str(p) for p in loc_parts) if loc_parts else "<root>"
    received = first.get("input")
    received_type = type(received).__name__

    full_schema = _inline_ref_schema(dict(settings_cls.model_json_schema()))
    field_schema = _navigate_schema(full_schema, loc_parts)
    expected_summary = _summarize_expected_shape(field_schema)

    example = _example_from_payload(node_type, loc_parts)
    if example is None:
        example = _synthesize_example_from_schema(field_schema)

    # W71 v1.13B — FunctionInput-specific disambiguation. The naming
    # collision (outer ``function`` parameter holding a FunctionInput
    # object whose inner ``function`` field is a string) trips small
    # models into reading *"got str"* as *"send a str"* — the LLM
    # inverts the constraint on the second retry. Detect the case
    # narrowly (FunctionInput summary + received str) and emit a
    # rewritten refusal that names the OUTER vs INNER ``function``
    # references explicitly, dropping the misread-prone *"not as a
    # JSON-encoded string"* clause.
    if (
        node_type == "formula"
        and isinstance(received, str)
        and "FunctionInput" in expected_summary
    ):
        truncated = received if len(received) <= 80 else received[:77] + "..."
        return (
            "formula's `function` parameter is an OBJECT with two keys:\n"
            "  - `field`: the new column descriptor, e.g. "
            '{"name": "full_name", "data_type": "String"}\n'
            "  - `function`: the row-wise expression STRING in "
            "[column_name] syntax, e.g. \"[first] + ' ' + [last]\"\n"
            f"Your call sent `function` as a single string ({truncated!r}). "
            'Re-emit as: {"field": {"name": "<col>", "data_type": '
            '"<type>"}, "function": "<your-expression>"}.'
        )

    parts = [
        f"Field `{loc_str}` expects {expected_summary} matching the schema in tool "
        f"`flowfile.graph.add_{node_type}` (see catalog); got {received_type}.",
    ]
    if example is not None:
        try:
            example_json = json.dumps(example)
            parts.append(f"Example payload for `{loc_str}`: {example_json}.")
        except (TypeError, ValueError):
            pass
    if isinstance(received, str) and _expects_object(field_schema):
        parts.append("Pass the structured object directly, not as a JSON-encoded string.")

    return " ".join(parts)


def _handle_add_node(
    *,
    node_type: str,
    tool_name: str,
    tool_args: dict[str, Any],
    redacted_args: dict[str, Any],
    insertion_context: InsertionContext,
    flow,
    session_id: str,
    user_id: int,
    mode: ExecutionMode,
    dry_run_cache: DryRunCache,
    llm_provided_node_id: int | None = None,
    audit_meta: dict[str, Any] | None = None,
    staged_offset_index: int = 0,
    extra_upstream_positions: dict[int, tuple[float, float]] | None = None,
    extra_upstream_schemas: dict[int, Any] | None = None,
) -> ToolExecutionResult:
    # W54 — every audit row this function emits piggybacks on tool_args
    # under the namespaced ``__planner_meta__`` key. Rebind ``redacted_args``
    # once at the top so all downstream rejection / record_event sites pick
    # it up automatically. (Why tool_args and not AuditEvent.extra: the
    # extra field is silently dropped by record_event — the AiAuditEvent
    # ORM has no ``extra`` column. See plan §6.)
    if audit_meta is not None:
        redacted_args = {**redacted_args, "__planner_meta__": audit_meta}

    # W62 — auto-layout: when the caller didn't supply pos_x/pos_y (both
    # ``None``), derive them from the upstream's canvas position so AI-staged
    # nodes don't pile up at (0, 0). When the caller passed explicit floats —
    # including ``0.0`` — they win verbatim.
    if insertion_context.pos_x is None and insertion_context.pos_y is None:
        resolved_x, resolved_y = _resolve_insertion_position(
            flow,
            insertion_context.upstream_node_ids,
            staged_offset_index=staged_offset_index,
            extra_upstream_positions=extra_upstream_positions,
        )
        insertion_context = insertion_context.model_copy(update={"pos_x": resolved_x, "pos_y": resolved_y})

    # W71 v2.1 — agent surfaces are not allowed to stage writer-shaped
    # node types (output / database_writer / cloud_storage_writer /
    # catalog_writer). The catalog filter in registry.build_tool_catalog
    # already hides them from the LLM-facing tool list, but the LLM can
    # hallucinate a call name (or a future regression could re-expose
    # the tool); refuse here so the safety property holds regardless of
    # how the call reached us.
    if node_type in safety.AGENT_BLOCKED_NODE_TYPES:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason="writer_blocked",
            refusal_detail=(
                f"node_type {node_type!r} writes to an external destination "
                "(file / database / cloud / catalog). The AI agent is not "
                "allowed to stage writer nodes — the user adds these "
                "manually. Suggest the writer to the user instead, or pick "
                "a transformation node (filter, sort, group_by, formula, "
                "select, …) for the next step."
            ),
        )

    settings_cls = get_settings_class_for_node_type(node_type)
    if settings_cls is None:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"unknown node type: {node_type!r}",
        )

    # --- W54 stage 0: LLM-provided node_id validation ---
    # Only fires when the planner observed the LLM emit ``node_id`` itself
    # rather than letting the planner allocate. The id must be fresh
    # (not in the live graph), not a duplicate of another agent-staged id,
    # and not equal to any of its own upstream / right_input — that last
    # rule overlaps with the planner-side guard in defence-in-depth.
    if llm_provided_node_id is not None:
        live_ids: set[int] = set()
        for node in flow.nodes:
            try:
                live_ids.add(int(node.node_id))
            except (TypeError, ValueError, AttributeError):
                continue
        staged_ids_meta = (audit_meta or {}).get("staged_node_ids_at_stage") or []
        staged_ids = {sid for sid in staged_ids_meta if isinstance(sid, int)}
        upstream_ids_set = {uid for uid in insertion_context.upstream_node_ids if isinstance(uid, int)}
        if insertion_context.right_input_node_id is not None:
            upstream_ids_set.add(insertion_context.right_input_node_id)

        violation: str | None = None
        if llm_provided_node_id in live_ids:
            violation = f"already exists in the live graph (live_node_ids={sorted(live_ids)})"
        elif llm_provided_node_id in staged_ids:
            violation = f"already staged this session (staged_node_ids={sorted(staged_ids)})"
        elif llm_provided_node_id in upstream_ids_set:
            violation = (
                f"equals one of its own upstream / right_input ids "
                f"({sorted(upstream_ids_set)}) — would create a self-loop"
            )
        if violation is not None:
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow.flow_id,
                refusal_reason="self_loop_prevented",
                refusal_detail=f"LLM-provided node_id {llm_provided_node_id} is invalid: {violation}",
            )

    # --- Refusal stage 1: Pydantic shape ---
    try:
        settings = settings_cls.model_validate(tool_args)
    except ValidationError as exc:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason="settings_validation",
            refusal_detail=_format_settings_validation_refusal(exc=exc, settings_cls=settings_cls, node_type=node_type),
        )

    # --- Refusal stage 1.5: upstream sink validation (2026-05-07) ---
    # The Tier-6 resolver fallback already filters sinks (see planner.py).
    # This guard catches the explicit case where the LLM named a sink in
    # ``upstream_node_ids`` directly (Tier 1) — the resolver respects
    # explicit intent, so the only place to refuse is here.
    upstream_check_ids = [uid for uid in (insertion_context.upstream_node_ids or []) if isinstance(uid, int)]
    if isinstance(insertion_context.right_input_node_id, int):
        upstream_check_ids.append(insertion_context.right_input_node_id)
    sink_upstreams = _detect_sink_upstreams(flow, upstream_check_ids)
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

    # --- Refusal stage 1.6: join-shaped wire validation (W71 v1.16) ---
    # Defense-in-depth: the v1.15A spec REQUIRES left_input_node_id +
    # right_input_node_id for join-shaped types, and the planner's
    # post-pick_upstream check enforces shape. But providers
    # (OpenRouter, Groq) don't always validate ``required`` strictly,
    # and the LLM occasionally ignores the spec and emits the legacy
    # ``upstream_node_ids: [left, right]`` shape with right_input_node_id
    # null — both ids in the list. The downstream
    # ``flow_node.add_node_connection`` then silently OVERWRITES
    # ``main_inputs`` on the second main-port call (line 638-641 has
    # ``input <= 2 → main_inputs = [from_node]`` which clobbers
    # previous), leaving the join with only one wire (last write wins)
    # and the LLM gets *"applied"* without any error. That's the
    # 2026-05-08 cross_join dogfood failure mode. Refuse here so the
    # error reaches the LLM and the retry path produces correct shape.
    from flowfile_core.ai.tools.meta_ops import JOIN_SHAPED_NODE_TYPES
    if node_type in JOIN_SHAPED_NODE_TYPES:
        upstream_ids_for_check = list(insertion_context.upstream_node_ids or [])
        right_id_for_check = insertion_context.right_input_node_id
        violation: str | None = None
        if right_id_for_check is None:
            violation = (
                f"join-shaped node `{node_type}` requires a RIGHT input. Pick the "
                "right upstream via ``right_input_node_id`` (the spec for "
                f"{node_type} exposes ``left_input_node_id`` + ``right_input_node_id``, "
                "both required scalars). The LEFT input goes in "
                "``upstream_node_ids[0]``; do NOT put both ids in "
                "``upstream_node_ids`` — they go in separate fields."
            )
        elif len(upstream_ids_for_check) != 1:
            violation = (
                f"join-shaped node `{node_type}` requires exactly ONE LEFT upstream "
                f"(``upstream_node_ids`` must have one element); got "
                f"{upstream_ids_for_check!r}. The right input goes in "
                "``right_input_node_id`` (separate scalar field), not as a second "
                "entry in ``upstream_node_ids``."
            )
        elif upstream_ids_for_check[0] == right_id_for_check:
            violation = (
                f"join-shaped node `{node_type}` cannot use the same id "
                f"({right_id_for_check}) for both LEFT and RIGHT inputs — a node "
                "cannot join to itself. Pick two different upstream ids."
            )
        if violation is not None:
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow.flow_id,
                refusal_reason="join_wire_invalid",
                refusal_detail=violation,
            )

    # W62 — stamp the resolved layout coordinates onto the settings object.
    # The apply path (``_apply_add_node`` → ``flow.add_<node_type>(settings)``)
    # reads ``settings.pos_x`` / ``settings.pos_y`` (via
    # ``set_node_information``) when stamping the canvas position; the
    # ``InsertionContext`` itself is only consulted for connection wiring.
    # Only stamp when the LLM did NOT include pos_x / pos_y in its tool_args
    # — explicit caller intent (even ``0.0``) wins. Detect via key presence
    # in ``tool_args`` rather than ``settings.pos_x == 0`` because
    # ``NodeBase`` defaults pos_x / pos_y to 0 and we can't distinguish
    # "LLM said 0" from "LLM omitted it" once Pydantic has run.
    settings_pos_x_explicit = "pos_x" in tool_args and tool_args["pos_x"] is not None
    settings_pos_y_explicit = "pos_y" in tool_args and tool_args["pos_y"] is not None
    if (
        insertion_context.pos_x is not None
        and insertion_context.pos_y is not None
        and hasattr(settings, "pos_x")
        and hasattr(settings, "pos_y")
        and not settings_pos_x_explicit
        and not settings_pos_y_explicit
    ):
        try:
            settings.pos_x = insertion_context.pos_x
            settings.pos_y = insertion_context.pos_y
        except (TypeError, ValueError, AttributeError):
            # Defensive — settings classes are Pydantic so assignment should
            # always succeed, but if a future class freezes the field we don't
            # want to fail the whole tool call over a layout cosmetic.
            logger.debug("could not stamp pos_x/pos_y on settings for %s", node_type)

    # --- Refusal stage 2: network egress (code-bearing nodes only) ---
    code = _extract_code(node_type, settings)
    if code is not None:
        egress_labels = safety.detect_network_egress(code)
        if egress_labels:
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow.flow_id,
                refusal_reason="network_egress",
                refusal_detail=f"blocked: {', '.join(egress_labels)}",
            )

    # --- Resolve upstream schemas (D011 tiers 0-1, warn on tier 2) ---
    upstream_ids = list(insertion_context.upstream_node_ids)
    if insertion_context.right_input_node_id is not None and insertion_context.right_input_node_id not in upstream_ids:
        upstream_ids.append(insertion_context.right_input_node_id)
    upstream_schemas, warnings = _resolve_upstream_schemas(
        flow, upstream_ids, staged_schemas=extra_upstream_schemas
    )

    # --- Refusal stage 3: column refs ---
    refs = collect_column_refs(node_type, settings)
    if refs and upstream_schemas:
        available: list[str] = []
        seen: set[str] = set()
        for cols in upstream_schemas.values():
            for col in cols:
                if col.column_name not in seen:
                    seen.add(col.column_name)
                    available.append(col.column_name)
        missing = safety.validate_column_references(refs, available)
        if missing:
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow.flow_id,
                refusal_reason="unknown_columns",
                refusal_detail=f"missing columns: {', '.join(missing)}",
                extra={"missing_columns": missing, "available_columns": available},
            )

    # --- Predict output schema ---
    node_class = classify_node_type(node_type)
    predicted: list[Any] | None = None
    if node_class == "dynamic":
        if code is None:
            warnings.append(f"dynamic node {node_type} has no code-bearing field; output schema cannot be predicted")
        elif not upstream_schemas and insertion_context.upstream_node_ids:
            warnings.append("upstream schema unresolved — kernel dry-run skipped; output schema deferred until run")
        elif not insertion_context.upstream_node_ids:
            warnings.append(f"dynamic node {node_type} has no upstream; output schema cannot be predicted")
        else:
            try:
                predicted = dry_run_code(
                    flow=flow,
                    node_id=settings.node_id,
                    upstream_node_ids=insertion_context.upstream_node_ids,
                    code=code,
                    output_names=_resolve_output_names(node_type, settings),
                    cache=dry_run_cache,
                    upstream_schemas=upstream_schemas,
                )
            except Exception as exc:
                logger.warning("dry-run failed for %s/%s: %s", node_type, settings.node_id, exc)
                warnings.append(f"kernel dry-run failed: {exc}; output schema deferred")
    else:
        predicted = predict_schema_via_mirror(
            node_type,
            settings,
            upstream_schemas,
            right_input_node_id=insertion_context.right_input_node_id,
        )

    # --- Apply or stage ---
    if mode == "stage":
        payload = {
            "node_type": node_type,
            "settings": settings.model_dump(mode="json"),
            "insertion_context": insertion_context.model_dump(),
            "predicted_output_schema": schema_to_dict_list(predicted),
        }
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
            predicted_output_schema=schema_to_dict_list(predicted),
            warnings=warnings,
            audit_id=audit_row.id if audit_row is not None else None,
            executed_args=redacted_args,
            staged_node_payload=payload,
        )

    # mode == "apply"
    try:
        _apply_add_node(flow, node_type, settings, insertion_context)
    except Exception as exc:
        logger.warning("apply failed for %s/%s: %s", node_type, settings.node_id, exc)
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"apply failed: {exc}",
        )

    audit_row = _record_event(
        session_id=session_id,
        user_id=user_id,
        tool_name=tool_name,
        flow_id=flow.flow_id,
        tool_args=redacted_args,
        result_status="success",
    )
    status: ResultStatus = "warned" if warnings else "applied"
    return ToolExecutionResult(
        status=status,
        tool_name=tool_name,
        node_id=settings.node_id,
        predicted_output_schema=schema_to_dict_list(predicted),
        warnings=warnings,
        audit_id=audit_row.id if audit_row is not None else None,
        executed_args=redacted_args,
    )


def _apply_add_node(flow, node_type: str, settings: BaseModel, ctx: InsertionContext) -> None:
    """Real-graph mutation: dispatch ``add_<node_type>`` then wire connections."""
    from flowfile_core.flowfile.flow_graph import add_connection

    add_method = getattr(flow, f"add_{node_type}")
    add_method(settings)

    target_id = settings.node_id
    main_ids = [uid for uid in ctx.upstream_node_ids if uid != ctx.right_input_node_id]
    # W71 v1.16 — ``NodeConnection.create_from_simple_input`` takes the
    # SEMANTIC ``input_type`` ("main" / "right" / "left"), NOT the
    # connection-class string ("input-0" / "input-1"). The pre-v1.16
    # code passed ``input_type="input-0"`` / ``"input-1"`` which fell
    # through the match block to the default ``_ → "input-0"``, so
    # BOTH the main and right wires got connection_class "input-0" —
    # `add_node_connection` then routed both to ``main_inputs`` and the
    # second call silently overwrote the first (the ``input <= 2``
    # branch in flow_node.py:638 clobbers main_inputs each time).
    # Pass the semantic names so the connection class lands correctly.
    for uid in main_ids:
        connection = input_schema.NodeConnection.create_from_simple_input(
            from_id=uid, to_id=target_id, input_type="main", output_handle="output-0"
        )
        add_connection(flow, connection)
    if ctx.right_input_node_id is not None:
        right_connection = input_schema.NodeConnection.create_from_simple_input(
            from_id=ctx.right_input_node_id, to_id=target_id, input_type="right", output_handle="output-0"
        )
        add_connection(flow, right_connection)


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
    """
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
    """Translate Pydantic field paths from internal ``NodeConnection`` field names
    back to the flat tool-spec field names (``input_class``, ``output_class``,
    ``from_node_id``, ``to_node_id``). Otherwise the W53 retry loop hands the LLM
    error messages referring to fields it never sees in the tool schema.

    2026-05-07 — appends a concrete example payload when any error names an
    integer field (mirrors W67's ``_format_settings_validation_refusal``: when
    the LLM JSON-string-encodes ids, raw Pydantic prose isn't enough — the
    example shape teaches the corrected call in one retry instead of the
    cascading-retry exhaustion the dogfood trace showed).
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


def _handle_connect(
    tool_name: str,
    tool_args: dict[str, Any],
    redacted_args: dict[str, Any],
    flow,
    session_id: str,
    user_id: int,
    mode: ExecutionMode,
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

    # 2026-05-07 — refuse explicit ``connect`` calls whose upstream side is a
    # sink. ``output_connection.node_id`` is the FROM (upstream) side; if it
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

    from flowfile_core.flowfile.flow_graph import add_connection

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

    from flowfile_core.flowfile.flow_graph import delete_connection

    try:
        delete_connection(flow, connection)
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


def _handle_update_node_settings(
    *,
    tool_name: str,
    tool_args: dict[str, Any],
    redacted_args: dict[str, Any],
    flow,
    session_id: str,
    user_id: int,
    mode: ExecutionMode,
    dry_run_cache: DryRunCache,
    extra_upstream_schemas: dict[int, Any] | None = None,
) -> ToolExecutionResult:
    """W47 — modify an existing node's settings.

    Mirrors :func:`_handle_add_node`'s shape: validates new settings via the
    Pydantic class for the live node's type, runs the network-egress check
    for code-bearing settings, resolves upstream schemas via the D011 tier
    handler from the existing wiring, validates column references, predicts
    the new output schema (mirror for static, kernel dry-run for dynamic),
    then either stages the modification for a :class:`GraphDiff` or
    re-fires the production ``add_<node_type>`` path so the live node
    inherits the new settings.

    The stage payload carries ``kind="modification"`` plus old and new
    settings dicts so the diff preview can render an old-vs-new view; the
    old-settings capture happens at stage time (not apply time) so a
    rejected diff reverts to what the user was looking at when reviewing.
    """
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
                f"update_node_settings requires an integer node_id (got {got_type}). "
                'Example payload: {"flow_id": 1, "node_id": 5, "settings": {...}}. '
                "Pass node ids as integers, not JSON-encoded strings."
            ),
        )

    settings_payload = tool_args.get("settings")
    if not isinstance(settings_payload, dict):
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail="update_node_settings requires a settings object",
        )

    target_node = flow.get_node(node_id)
    if target_node is None:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"node {node_id} not found in flow {flow.flow_id}",
        )

    node_type = getattr(target_node, "node_type", None)
    if not isinstance(node_type, str) or not node_type:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"node {node_id} has no resolvable node_type",
        )

    settings_cls = get_settings_class_for_node_type(node_type)
    if settings_cls is None:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"unknown node type: {node_type!r}",
        )

    # Capture the live (pre-change) settings BEFORE validation. This is the
    # truth the diff-preview UI renders against and the value Reject would
    # restore — we want it stable even if the new settings later fail.
    old_settings_dict: dict[str, Any] = {}
    current_settings = getattr(target_node, "setting_input", None)
    if isinstance(current_settings, BaseModel):
        try:
            old_settings_dict = current_settings.model_dump(mode="json")
        except Exception:  # noqa: BLE001 — defensive serialisation
            logger.warning("could not serialise existing settings for node %s", node_id)

    # --- Refusal stage 1: Pydantic shape ---
    try:
        new_settings = settings_cls.model_validate(settings_payload)
    except ValidationError as exc:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason="settings_validation",
            refusal_detail=_format_settings_validation_refusal(exc=exc, settings_cls=settings_cls, node_type=node_type),
        )

    # --- Refusal stage 2: network egress (code-bearing nodes only) ---
    code = _extract_code(node_type, new_settings)
    if code is not None:
        egress_labels = safety.detect_network_egress(code)
        if egress_labels:
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow.flow_id,
                refusal_reason="network_egress",
                refusal_detail=f"blocked: {', '.join(egress_labels)}",
            )

    # --- Resolve upstream schemas from the live node's existing wiring ---
    # Modifications never rewire the topology — the LLM must use connect /
    # delete_connection for that — so we read upstream ids off the live
    # node rather than from the new settings (which might carry a stale
    # ``depending_on_id`` we don't want to honour for schema resolution).
    main_input_ids: list[int] = []
    main_inputs = getattr(target_node.node_inputs, "main_inputs", None) or []
    for upstream in main_inputs:
        try:
            main_input_ids.append(int(upstream.node_id))
        except (TypeError, ValueError, AttributeError):
            continue
    right_input_obj = getattr(target_node.node_inputs, "right_input", None)
    right_input_id: int | None = None
    if right_input_obj is not None:
        try:
            right_input_id = int(right_input_obj.node_id)
        except (TypeError, ValueError, AttributeError):
            right_input_id = None

    upstream_ids = list(main_input_ids)
    if right_input_id is not None and right_input_id not in upstream_ids:
        upstream_ids.append(right_input_id)
    upstream_schemas, warnings = _resolve_upstream_schemas(
        flow, upstream_ids, staged_schemas=extra_upstream_schemas
    )

    # --- Refusal stage 3: column refs ---
    refs = collect_column_refs(node_type, new_settings)
    if refs and upstream_schemas:
        available: list[str] = []
        seen: set[str] = set()
        for cols in upstream_schemas.values():
            for col in cols:
                if col.column_name not in seen:
                    seen.add(col.column_name)
                    available.append(col.column_name)
        missing = safety.validate_column_references(refs, available)
        if missing:
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow.flow_id,
                refusal_reason="unknown_columns",
                refusal_detail=f"missing columns: {', '.join(missing)}",
                extra={"missing_columns": missing, "available_columns": available},
            )

    # --- Predict output schema ---
    node_class = classify_node_type(node_type)
    predicted: list[Any] | None = None
    if node_class == "dynamic":
        if code is None:
            warnings.append(f"dynamic node {node_type} has no code-bearing field; output schema cannot be predicted")
        elif not upstream_schemas and main_input_ids:
            warnings.append("upstream schema unresolved — kernel dry-run skipped; output schema deferred until run")
        elif not main_input_ids:
            warnings.append(f"dynamic node {node_type} has no upstream; output schema cannot be predicted")
        else:
            try:
                predicted = dry_run_code(
                    flow=flow,
                    node_id=node_id,
                    upstream_node_ids=main_input_ids,
                    code=code,
                    output_names=_resolve_output_names(node_type, new_settings),
                    cache=dry_run_cache,
                    upstream_schemas=upstream_schemas,
                )
            except Exception as exc:
                logger.warning("dry-run failed for %s/%s on update: %s", node_type, node_id, exc)
                warnings.append(f"kernel dry-run failed: {exc}; output schema deferred")
    else:
        predicted = predict_schema_via_mirror(
            node_type,
            new_settings,
            upstream_schemas,
            right_input_node_id=right_input_id,
        )

    # --- Apply or stage ---
    if mode == "stage":
        payload = {
            "kind": "modification",
            "node_id": node_id,
            "node_type": node_type,
            "old_settings": old_settings_dict,
            "new_settings": new_settings.model_dump(mode="json"),
            "predicted_output_schema": schema_to_dict_list(predicted),
        }
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
            node_id=node_id,
            predicted_output_schema=schema_to_dict_list(predicted),
            warnings=warnings,
            audit_id=audit_row.id if audit_row is not None else None,
            executed_args=redacted_args,
            staged_node_payload=payload,
        )

    # mode == "apply"
    try:
        _apply_update_node_settings(flow, node_type, new_settings)
    except Exception as exc:
        logger.warning("apply failed for update_node_settings %s/%s: %s", node_type, node_id, exc)
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"apply failed: {exc}",
        )

    audit_row = _record_event(
        session_id=session_id,
        user_id=user_id,
        tool_name=tool_name,
        flow_id=flow.flow_id,
        tool_args=redacted_args,
        result_status="success",
    )
    status: ResultStatus = "warned" if warnings else "applied"
    return ToolExecutionResult(
        status=status,
        tool_name=tool_name,
        node_id=node_id,
        predicted_output_schema=schema_to_dict_list(predicted),
        warnings=warnings,
        audit_id=audit_row.id if audit_row is not None else None,
        executed_args=redacted_args,
    )


def _apply_update_node_settings(flow, node_type: str, settings: BaseModel) -> None:
    """Real-graph mutation: re-fire ``add_<node_type>`` so the existing node
    inherits the new settings.

    The same path the production UI takes from
    ``POST /editor/update_settings/`` (``routes.py:887``) — ``add_node_step``
    detects the existing node id and routes through ``existing_node.update_node``
    rather than constructing a fresh :class:`FlowNode`. Wiring is preserved
    (``existing_node.all_inputs`` is kept verbatim — the modification never
    rewires upstream / right-input).
    """
    add_method = getattr(flow, f"add_{node_type}", None)
    if add_method is None:
        raise ValueError(f"flow has no add_{node_type} method")
    add_method(settings)


def _handle_schema(
    *,
    op: str,
    tool_name: str,
    tool_args: dict[str, Any],
    redacted_args: dict[str, Any],
    flow,
    session_id: str,
    user_id: int,
) -> ToolExecutionResult:
    """Read-only introspection ops — never mutate the graph."""
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
                f"{op} requires an integer node_id (got {got_type}). "
                'Example payload: {"flow_id": 1, "node_id": 5}. '
                "Pass node ids as integers, not JSON-encoded strings."
            ),
        )

    node = flow.get_node(node_id)
    if node is None:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"node {node_id} not found in flow {flow.flow_id}",
        )

    if op == "read_node_schema":
        try:
            schema = node.get_predicted_schema(force=False)
        except Exception as exc:
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow.flow_id,
                refusal_reason=None,
                refusal_detail=f"schema read failed: {exc}",
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
            predicted_output_schema=schema_to_dict_list(schema),
            audit_id=audit_row.id if audit_row is not None else None,
            executed_args=redacted_args,
        )

    if op == "read_node_preview":
        # Preview is metadata-only in W31 — actual sample-row return belongs
        # to a later workstream that wires the existing GET /node?get_data
        # path through the audit pipeline.
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail="schema.read_node_preview not implemented in W31; use the existing GET /node?get_data=true",
        )

    return _reject_and_audit(
        tool_name=tool_name,
        tool_args=redacted_args,
        session_id=session_id,
        user_id=user_id,
        flow_id=flow.flow_id,
        refusal_reason=None,
        refusal_detail=f"unknown schema op: {op!r}",
    )


def _handle_codegen(
    *,
    op: str,
    tool_name: str,
    redacted_args: dict[str, Any],
    session_id: str,
    user_id: int,
    flow_id: int,
) -> ToolExecutionResult:
    """Codegen tools are LLM-facing proposals — the LLM should call
    ``flowfile.graph.add_polars_code`` etc. directly with its generated code.
    W31 returns a friendly hint; W40's planner replaces this stub with actual
    LLM-driven code generation."""
    audit_row = _record_event(
        session_id=session_id,
        user_id=user_id,
        tool_name=tool_name,
        flow_id=flow_id,
        tool_args=redacted_args,
        result_status="success",
    )
    return ToolExecutionResult(
        status="staged",
        tool_name=tool_name,
        audit_id=audit_row.id if audit_row is not None else None,
        executed_args=redacted_args,
        staged_node_payload={
            "codegen_op": op,
            "hint": (
                "Codegen tools are not directly executed in W31. Generate the code "
                f"yourself and call flowfile.graph.add_{op.replace('generate_', '').replace('_code', '_code')} "
                "with the proposed snippet."
            ),
        },
    )


def _unwrap_json_string_values(value: Any) -> Any:
    """W71 v1.4 — recursively unwrap JSON-encoded string values in tool
    args.

    Smaller open-weights models routinely emit structured tool args as
    JSON-encoded strings rather than native objects / arrays / ints.
    Three failure modes seen in dogfood:

    * ``upstream_node_ids: "[3, 4]"`` (the field is array<int>; model
      emits a string).
    * ``groupby_input: "{\\"agg_cols\\": [...]}"`` (an object field
      delivered as a JSON-string).
    * ``node_id: "5"`` (an int field delivered as a digit-string).

    Pydantic strict mode rejects each of these and the planner spends
    its retry budget re-asking the model to fix shape, which it usually
    does only after several attempts (or never). Pre-unwrapping at the
    executor seam means the rest of the pipeline (Pydantic validation,
    custom handlers) sees the native types it expects.

    The heuristic is conservative: only attempt to JSON-parse strings
    that **start with ``{``, ``[``, a digit, or ``-``**. This protects
    free-form code bodies (``polars_code``, ``python_script``,
    ``sql_query``), Polars expressions ("``pl.col('x') > 5``"), and
    SQL queries ("``SELECT …``") from being parsed accidentally —
    none of those start with a JSON-shape character.

    A parsed value replaces the original string ONLY when it parses to
    a ``dict``, ``list``, or ``int``. Floats / bools / null parse-results
    leave the original string in place so Pydantic can decide; this
    avoids rewriting a literal ``"true"`` (where the model truly meant
    the string) into ``True``.

    Walks dicts and lists recursively so partially-encoded payloads
    (e.g. ``{"join_input": {"join_mapping": "[{...}]"}}``) get fully
    unwrapped in one pass.
    """
    if isinstance(value, dict):
        return {k: _unwrap_json_string_values(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_unwrap_json_string_values(item) for item in value]
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return value
        first = stripped[0]
        # Free-form strings (code, SQL, prose, identifiers) never start
        # with a JSON-shape character, so this guard makes the unwrap
        # pass safe to apply universally without a per-tool allowlist.
        if first not in "{[" and not first.isdigit() and first != "-":
            return value
        try:
            parsed = json.loads(stripped)
        except (TypeError, ValueError):
            return value
        # Recurse so nested JSON-strings inside the parsed value also
        # get unwrapped. Booleans are passed through; floats/null land
        # back at the original string.
        if isinstance(parsed, dict):
            return _unwrap_json_string_values(parsed)
        if isinstance(parsed, list):
            return _unwrap_json_string_values(parsed)
        if isinstance(parsed, bool):
            return value
        if isinstance(parsed, int):
            return parsed
        return value
    return value


def _coerce_to_int_or_self(value: Any) -> Any:
    """W71 v1.3 — small helper for lenient ``int | null`` parsing.

    Returns the parsed int when ``value`` is a stringified int (``"4"``)
    or None when it's an empty string. Otherwise returns the original
    ``value`` unchanged so the caller's type check fires on the
    structurally-wrong shape rather than on a recoverable string. Booleans
    are passed through (caller rejects them — Python's ``isinstance(True, int)``
    is True so we explicitly avoid swallowing booleans here).
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or stripped.lower() in ("null", "none"):
            return None
        try:
            return int(stripped)
        except (TypeError, ValueError):
            return value
    return value


def _coerce_to_int_list_or_self(value: Any) -> Any:
    """W71 v1.3 — coerce common llama-70b mis-shapes back to ``list[int]``.

    Accepts:

    * Native ``list`` of ints — returned unchanged (each element passes
      through ``_coerce_to_int_or_self`` so stringified ints inside the
      list are also recovered).
    * Single ``int`` — wrapped as ``[int]`` (the model emitted a scalar
      where the schema expected an array).
    * ``str`` parseable as JSON to a list or int — parsed and wrapped.
    * ``str`` of comma-separated ints (``"4, 5"``) — parsed.
    * Anything else — returned unchanged so the caller's type check
      surfaces the actual shape problem in the refusal_detail.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return [value]
    if isinstance(value, list):
        return [_coerce_to_int_or_self(item) for item in value]
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        # Try JSON first — covers '[4]', '[4, 5]', '4', and '"4"'.
        try:
            parsed = json.loads(stripped)
        except (TypeError, ValueError):
            parsed = None
        if isinstance(parsed, list):
            return [_coerce_to_int_or_self(item) for item in parsed]
        if isinstance(parsed, int) and not isinstance(parsed, bool):
            return [parsed]
        # Fallback: comma-separated integers.
        try:
            return [int(part.strip()) for part in stripped.split(",") if part.strip()]
        except (TypeError, ValueError):
            return value
    return value


def _handle_meta(
    *,
    op: str,
    tool_name: str,
    tool_args: dict[str, Any],
    redacted_args: dict[str, Any],
    session_id: str,
    user_id: int,
    flow_id: int,
) -> ToolExecutionResult:
    """Dispatch the W71 staged meta ops.

    * ``classify_intent`` — stage 0: returns ``extra["op_kind"]``.
    * ``pick_node_type`` — stage 1: returns ``extra["node_type"]``.
    * ``pick_upstream`` — stage 2: returns
      ``extra["upstream_node_ids"]`` and ``extra["right_input_node_id"]``.

    All three follow the same shape: validate the LLM-provided value
    against the schema (enum / type), emit one ``AuditEvent``, return
    ``status="applied"`` with the chosen value(s) on ``extra`` so the
    planner can mutate session state.

    W71 v1.10 — ``pick_category`` (legacy two-stage agent) was removed.
    """
    if op == "emit_plan":
        plan = tool_args.get("plan")
        rationale = tool_args.get("rationale", "")
        if not isinstance(plan, str) or not plan.strip():
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow_id,
                refusal_reason=None,
                refusal_detail=(
                    "emit_plan: ``plan`` must be a non-empty string "
                    "containing the markdown plan (numbered list of "
                    "≤6 steps; each step names a node_type and a "
                    "one-sentence description)."
                ),
            )
        audit_row = _record_event(
            session_id=session_id,
            user_id=user_id,
            tool_name=tool_name,
            flow_id=flow_id,
            tool_args=redacted_args,
            result_status="success",
        )
        return ToolExecutionResult(
            status="applied",
            tool_name=tool_name,
            audit_id=audit_row.id if audit_row is not None else None,
            executed_args=redacted_args,
            extra={"plan": plan, "rationale": str(rationale or "")},
        )

    if op == "classify_intent":
        op_kind = tool_args.get("op_kind")
        rationale = tool_args.get("rationale", "")
        if not isinstance(op_kind, str) or op_kind not in OP_KIND_NAMES:
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow_id,
                refusal_reason=None,
                refusal_detail=(
                    f"classify_intent: op_kind {op_kind!r} not one of "
                    f"{list(OP_KIND_NAMES)}"
                ),
            )
        audit_row = _record_event(
            session_id=session_id,
            user_id=user_id,
            tool_name=tool_name,
            flow_id=flow_id,
            tool_args=redacted_args,
            result_status="success",
        )
        return ToolExecutionResult(
            status="applied",
            tool_name=tool_name,
            audit_id=audit_row.id if audit_row is not None else None,
            executed_args=redacted_args,
            extra={"op_kind": op_kind, "rationale": str(rationale or "")},
        )

    if op == "pick_node_type":
        node_type = tool_args.get("node_type")
        rationale = tool_args.get("rationale", "")
        if not isinstance(node_type, str) or get_settings_class_for_node_type(node_type) is None:
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow_id,
                refusal_reason=None,
                refusal_detail=(
                    f"pick_node_type: node_type {node_type!r} is not a registered "
                    f"Flowfile node type. Known: {sorted(NODE_TYPE_TO_SETTINGS_CLASS.keys())}"
                ),
            )
        audit_row = _record_event(
            session_id=session_id,
            user_id=user_id,
            tool_name=tool_name,
            flow_id=flow_id,
            tool_args=redacted_args,
            result_status="success",
        )
        return ToolExecutionResult(
            status="applied",
            tool_name=tool_name,
            audit_id=audit_row.id if audit_row is not None else None,
            executed_args=redacted_args,
            extra={"node_type": node_type, "rationale": str(rationale or "")},
        )

    if op == "pick_upstream":
        # W71 v1.15A — for join-shaped node types the spec uses a
        # symmetric scalar pair (``left_input_node_id`` +
        # ``right_input_node_id``). When the LLM emits that shape,
        # translate to the legacy list+scalar representation BEFORE
        # the coercion / validation runs so the downstream consumers
        # (planner session state, ``_handle_add_node`` insertion
        # context) see exactly what they did pre-v1.15.
        raw_left_input = tool_args.get("left_input_node_id")
        if raw_left_input is not None:
            raw_left_coerced = _coerce_to_int_or_self(raw_left_input)
            if isinstance(raw_left_coerced, int) and not isinstance(raw_left_coerced, bool):
                tool_args = dict(tool_args)  # copy, then translate
                tool_args["upstream_node_ids"] = [raw_left_coerced]
                tool_args.pop("left_input_node_id", None)
                redacted_args = safety.redact_secrets(tool_args)
        raw_upstream = tool_args.get("upstream_node_ids")
        raw_right = tool_args.get("right_input_node_id")
        rationale = tool_args.get("rationale", "")

        # W71 v1.3 — coerce common llama-70b mis-shapes back to the
        # expected list[int]. Without this, every dogfood run on
        # llama-3.3-70b spends its retry budget on the same predictable
        # type errors. Three mistakes seen in practice:
        #   1. ``upstream_node_ids: 4`` (single int, not wrapped in a list)
        #   2. ``upstream_node_ids: "4"`` (JSON-encoded as string)
        #   3. ``upstream_node_ids: "[4]"`` or ``"4,5"`` (CSV string)
        # Each gets coerced to ``[4]`` / ``[4, 5]`` rather than rejected.
        # We only coerce — if the result still doesn't match list[int]
        # the original rejection still fires below.
        raw_upstream = _coerce_to_int_list_or_self(raw_upstream)
        raw_right = _coerce_to_int_or_self(raw_right)

        if not isinstance(raw_upstream, list):
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow_id,
                refusal_reason=None,
                refusal_detail=(
                    "pick_upstream: upstream_node_ids must be a list of integers, "
                    f"got {type(raw_upstream).__name__}. Pass a list like [3] "
                    "or [3, 4]; not a string, not a single integer."
                ),
            )
        upstream_ids: list[int] = []
        for uid in raw_upstream:
            if isinstance(uid, int) and not isinstance(uid, bool):
                upstream_ids.append(uid)
            else:
                return _reject_and_audit(
                    tool_name=tool_name,
                    tool_args=redacted_args,
                    session_id=session_id,
                    user_id=user_id,
                    flow_id=flow_id,
                    refusal_reason=None,
                    refusal_detail=(
                        f"pick_upstream: upstream_node_ids contains non-integer {uid!r}. "
                        "Each entry must be one of the live node ids in the enum."
                    ),
                )

        right_input_id: int | None = None
        if raw_right is not None:
            if isinstance(raw_right, int) and not isinstance(raw_right, bool):
                right_input_id = raw_right
            else:
                return _reject_and_audit(
                    tool_name=tool_name,
                    tool_args=redacted_args,
                    session_id=session_id,
                    user_id=user_id,
                    flow_id=flow_id,
                    refusal_reason=None,
                    refusal_detail=(
                        "pick_upstream: right_input_node_id must be an integer or null, "
                        f"got {type(raw_right).__name__}"
                    ),
                )

        audit_row = _record_event(
            session_id=session_id,
            user_id=user_id,
            tool_name=tool_name,
            flow_id=flow_id,
            tool_args=redacted_args,
            result_status="success",
        )
        return ToolExecutionResult(
            status="applied",
            tool_name=tool_name,
            audit_id=audit_row.id if audit_row is not None else None,
            executed_args=redacted_args,
            extra={
                "upstream_node_ids": upstream_ids,
                "right_input_node_id": right_input_id,
                "rationale": str(rationale or ""),
            },
        )

    return _reject_and_audit(
        tool_name=tool_name,
        tool_args=redacted_args,
        session_id=session_id,
        user_id=user_id,
        flow_id=flow_id,
        refusal_reason=None,
        refusal_detail=f"unknown meta op: {op!r}",
    )


def _extract_code(node_type: str, settings: BaseModel) -> str | None:
    path = _CODE_BEARING.get(node_type)
    if path is None:
        return None
    obj: Any = settings
    for attr in path:
        obj = getattr(obj, attr, None)
        if obj is None:
            return None
    return obj if isinstance(obj, str) else None


def _resolve_output_names(node_type: str, settings: BaseModel) -> list[str]:
    """Return the output names the kernel dry-run should produce.

    ``python_script`` and ``user_defined`` declare ``output_names`` explicitly;
    everything else has a single ``main`` output.
    """
    if node_type in ("python_script", "user_defined"):
        names = getattr(settings, "output_names", None) or ["main"]
        return list(names)
    return ["main"]


def _record_event(
    *,
    session_id: str,
    user_id: int,
    tool_name: str,
    flow_id: int,
    tool_args: dict[str, Any] | None,
    result_status: audit.ResultStatus,
    error: str | None = None,
):
    """Persist an audit event; swallow audit-side exceptions so a DB hiccup
    doesn't take down a tool call. Returns the row or ``None``."""
    try:
        return audit.record_event(
            audit.AuditEvent(
                session_id=session_id,
                user_id=user_id,
                tool_name=tool_name,
                result_status=result_status,
                flow_id=flow_id,
                tool_args=tool_args,
                error=error,
            )
        )
    except Exception as exc:
        logger.warning("audit.record_event failed for %s: %s", tool_name, exc)
        return None


def _reject_and_audit(
    *,
    tool_name: str,
    tool_args: dict[str, Any],
    session_id: str,
    user_id: int,
    flow_id: int,
    refusal_reason: safety.RefusalReason | None,
    refusal_detail: str,
    extra: dict[str, Any] | None = None,
) -> ToolExecutionResult:
    audit_row = _record_event(
        session_id=session_id,
        user_id=user_id,
        tool_name=tool_name,
        flow_id=flow_id,
        tool_args=tool_args,
        result_status="rejected",
        error=refusal_detail,
    )
    return ToolExecutionResult(
        status="rejected",
        tool_name=tool_name,
        refusal_reason=refusal_reason,
        refusal_detail=refusal_detail,
        audit_id=audit_row.id if audit_row is not None else None,
        executed_args=tool_args,
        extra=extra or {},
    )


__all__ = [
    "execute_tool_call",
    "ToolExecutionResult",
    "InsertionContext",
    "ExecutionMode",
    "ResultStatus",
]
