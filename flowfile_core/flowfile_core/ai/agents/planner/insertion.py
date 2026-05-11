"""Insertion-context resolution + node id allocation.

The 7-tier resolver in :func:`_resolve_insertion_context` is the
canonical place that turns the LLM's emitted args into the
``InsertionContext`` the executor expects. Tier order is documented
inline; see also the planner system prompt which explains the tier
priorities to the LLM.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from flowfile_core.ai import sessions
from flowfile_core.ai.providers.base import ToolCall
from flowfile_core.ai.tools.classification import classify_node_type
from flowfile_core.ai.tools.executor import InsertionContext

from ._internal import (
    _ADD_PREFIX,
    _SETTINGS_DEPENDENCY_FIELDS,
    _STAGED_STATE_MACHINE_SURFACES,
)

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph import FlowGraph

logger = logging.getLogger(__name__)


def _allocate_node_id(flow: FlowGraph, session: sessions.AgentSession) -> int:
    """Pick the next free node_id, considering live nodes + in-batch additions.

    The flow's ``_node_db`` is keyed by id; every add_* tool dispatch
    reserves a slot in the staged session as well. Allocating here keeps
    the LLM out of the id-management business — it just emits settings.
    """
    used: set[int] = set()
    for node in flow.nodes:
        try:
            used.add(int(node.node_id))
        except (TypeError, ValueError, AttributeError):
            continue
    for entry in session.staged_results:
        if not isinstance(entry.staged_node_payload, dict):
            continue
        settings = entry.staged_node_payload.get("settings")
        if isinstance(settings, dict):
            nid = settings.get("node_id")
            if isinstance(nid, int):
                used.add(nid)
    return (max(used) + 1) if used else 1


def _read_settings_dependency_field(args: dict[str, Any]) -> int | None:
    """Read a primary-upstream settings field out of ``args``.

    The LLM's tool ``arguments`` for ``add_<node_type>`` follow the per-node
    Pydantic settings schema. Most surfaces emit fields at the root of
    ``args`` (e.g. ``{"depending_on_id": 2, "filter_input": {...}, ...}``);
    a few wrap them under ``settings_input``. Mirrors the dual-lookup
    pattern in :func:`_arg_summary_for_add`.

    Returns the int id when a known dependency field is present and parseable,
    else ``None``. See :data:`_SETTINGS_DEPENDENCY_FIELDS` for the canonical
    set.
    """
    nested = args.get("settings_input")
    sources: list[dict[str, Any]] = [args]
    if isinstance(nested, dict):
        sources.append(nested)
    for source in sources:
        for field in _SETTINGS_DEPENDENCY_FIELDS:
            raw = source.get(field)
            if isinstance(raw, int) and raw >= 0:
                # ``-1`` is the NodeSingleInput default sentinel meaning
                # "no dependency configured"; treat as absent.
                return raw
    return None


def _format_ambiguous_insertion_detail(flow: FlowGraph) -> str:
    """Build a refusal_detail string listing the live candidates.

    Used by the ambiguous-insertion refusal so the LLM can retry with
    explicit ``upstream_node_ids``. Walks ``flow.nodes`` once; defensive
    against a node missing ``node_id`` / ``node_type``.
    """
    parts: list[str] = []
    for node in flow.nodes:
        try:
            nid = int(node.node_id)
        except (TypeError, ValueError, AttributeError):
            continue
        node_type = getattr(node, "node_type", None) or "unknown"
        parts.append(f"{nid} ({node_type})")
    candidates = ", ".join(parts) if parts else "(none)"
    return (
        "multiple live nodes and no user selection / pin / explicit "
        f"upstream_node_ids — choose one and retry. Candidates: {candidates}"
    )


def _resolve_insertion_context(
    session: sessions.AgentSession,
    tc: ToolCall,
    flow: FlowGraph,
) -> tuple[InsertionContext, str | None]:
    """Build an :class:`InsertionContext` for a tool call.

    Returns ``(insertion_context, ambiguous_detail_or_none)``. The second
    element is always ``None`` — the ambiguity-refusal tier was reverted
    after live UX showed it too aggressive. Tuple shape preserved so
    existing destructuring at call sites keeps working.

    Tier order (7 tiers):

    1. LLM-provided ``args["upstream_node_ids"]`` (explicit override always
       wins).
    2. Settings-field dependency hint in ``args`` — see
       :data:`_SETTINGS_DEPENDENCY_FIELDS`. The catalog generator exposes
       legacy connection-state fields (``depending_on_id``) AND the planner
       param (``upstream_node_ids``) to the LLM; when only the legacy field
       is set, the resolver canonicalises.
    3. Most-recent in-batch staged ``add_*`` from ``session.staged_results``
       (chained transformations: add_filter → add_sort).
    4. ``session.selected_node_ids`` — the user's canvas selection at start
       time.
    5. ``session.pinned_node_ids`` — ``@``-mention targets at start time.
    6. Most-recently-added live node whose ``NodeTemplate.output > 0``
       — i.e. skip terminal / sink types (``explore_data``, ``output``,
       ``database_writer``, ``cloud_storage_writer``, ``catalog_writer``)
       in reverse order. The walk-in-reverse-and-skip-sinks rule prevents
       attaching a new node downstream of a sink on flows that end in
       ``explore_data`` / writer nodes.
    7. Empty (truly cold flow — no nodes, OR all live nodes are terminals).
       The executor handles this (sources don't need an upstream;
       most node types refuse).

    Source-only adds (``manual_input`` / ``read`` / ``database_reader`` /
    ``cloud_storage_reader`` / ``catalog_reader`` / ``kafka_source`` /
    ``google_analytics_reader`` / ``external_source``) bypass every tier
    and return empty upstream / right_input — they have no input port.
    """
    args: dict[str, Any] = tc.arguments or {}

    upstream_ids: list[int] = []
    ambiguous_detail: str | None = None

    node_type: str | None = None
    if tc.name and tc.name.startswith(_ADD_PREFIX):
        node_type = tc.name.removeprefix(_ADD_PREFIX)
    is_source_only = bool(node_type) and classify_node_type(node_type) == "source"

    # Tier 0 — staged stage 3: session state is canonical. The LLM at
    # ``fill_settings`` doesn't see the upstream fields in its tool
    # schema (they're stripped by ``build_staged_fill_tool_spec``); the
    # upstream picker at stage 2 already resolved the choice and stored
    # it on the session. Skip every other tier — these picks cannot be
    # overridden by an LLM that didn't see the schema.
    if session.surface in _STAGED_STATE_MACHINE_SURFACES and session.stage == "fill_settings":
        upstream_ids = [uid for uid in (session.picked_upstream_ids or []) if isinstance(uid, int)]

    # Tier 1 — explicit planner param.
    if not upstream_ids:
        raw_upstream = args.get("upstream_node_ids")
        if isinstance(raw_upstream, list):
            for uid in raw_upstream:
                if isinstance(uid, int):
                    upstream_ids.append(uid)

    if is_source_only:
        if upstream_ids:
            logger.debug(
                "stripping upstream_node_ids=%s from source-only add %s",
                upstream_ids,
                node_type,
            )
            upstream_ids = []
    else:
        # Tier 2 — settings-field dependency hint.
        if not upstream_ids:
            settings_dep = _read_settings_dependency_field(args)
            if settings_dep is not None:
                upstream_ids = [settings_dep]

        # Tier 3 — most-recent in-batch staged add_*.
        if not upstream_ids:
            for entry in reversed(session.staged_results):
                if not entry.tool_name.startswith(_ADD_PREFIX):
                    continue
                payload = entry.staged_node_payload if isinstance(entry.staged_node_payload, dict) else {}
                settings = payload.get("settings") if isinstance(payload.get("settings"), dict) else {}
                nid = settings.get("node_id") if isinstance(settings, dict) else None
                if isinstance(nid, int):
                    upstream_ids = [nid]
                    break

        # Tier 4 — session selection.
        if not upstream_ids and session.selected_node_ids:
            upstream_ids = [uid for uid in session.selected_node_ids if isinstance(uid, int)]

        # Tier 5 — session pinned (@-mention).
        if not upstream_ids and session.pinned_node_ids:
            upstream_ids = [uid for uid in session.pinned_node_ids if isinstance(uid, int)]

        # Tiers 6-7 — live-graph fallback. Tier 6 walks live nodes in reverse
        # and falls back to the most-recently-added node whose template advertises
        # an output port (``NodeTemplate.output > 0``). Sink types (``output=0``
        # — explore_data, output, database_writer, cloud_storage_writer,
        # catalog_writer) are skipped: attaching a downstream node to a sink is
        # always wrong (the sink consumes data, doesn't produce it). Tier 7
        # is the truly-empty case (cold flow OR all live nodes are sinks).
        # Selection / pin tiers above still take precedence when set; LLM override
        # + settings-field hint always win.
        if not upstream_ids:
            live_nodes = flow.nodes
            if live_nodes:
                for candidate in reversed(live_nodes):
                    template = getattr(candidate, "node_template", None)
                    # ``NodeTemplate.output`` is an int port count. Default to 1
                    # when the template is missing or the attribute isn't set —
                    # treating an unknown node type as non-terminal preserves the
                    # pre-fix behaviour for any node missing a registered template.
                    if template is not None and getattr(template, "output", 1) == 0:
                        continue
                    try:
                        upstream_ids = [int(candidate.node_id)]
                        break
                    except (TypeError, ValueError, AttributeError):
                        continue
            # else / no non-sink found: Tier 7 — leave upstream_ids empty.

    # At fill_settings, right input is also session-canonical.
    if is_source_only:
        right_input_node_id = None
    elif session.surface in _STAGED_STATE_MACHINE_SURFACES and session.stage == "fill_settings":
        right_input_node_id = session.picked_right_input_id
    else:
        raw_right = args.get("right_input_node_id")
        right_input_node_id = raw_right if isinstance(raw_right, int) else None

    pos_x = args.get("pos_x")
    pos_y = args.get("pos_y")
    # Leave pos_x / pos_y as ``None`` when the LLM didn't supply numbers
    # so the executor's auto-layout resolver kicks in. The LLM never
    # invents screen coordinates in practice.
    pos_x_val = float(pos_x) if isinstance(pos_x, int | float) else None
    pos_y_val = float(pos_y) if isinstance(pos_y, int | float) else None

    ctx = InsertionContext(
        upstream_node_ids=upstream_ids,
        right_input_node_id=right_input_node_id,
        pos_x=pos_x_val,
        pos_y=pos_y_val,
    )
    return ctx, ambiguous_detail


def _count_prior_staged_with_same_upstream(
    session: sessions.AgentSession,
    upstream_node_ids: list[int],
) -> int:
    """Count prior in-batch staged ``add_*`` entries anchored at the same
    upstream as ``upstream_node_ids``. Threaded into the executor as
    ``staged_offset_index`` so fan-outs from one upstream stack vertically
    instead of overlapping. Chained adds (each with a different upstream)
    naturally see 0 here and lay out as a straight horizontal chain.
    """
    target = list(upstream_node_ids)
    count = 0
    for entry in session.staged_results:
        if not entry.tool_name.startswith(_ADD_PREFIX):
            continue
        payload = entry.staged_node_payload if isinstance(entry.staged_node_payload, dict) else {}
        ic = payload.get("insertion_context") if isinstance(payload, dict) else None
        if not isinstance(ic, dict):
            continue
        prior_upstream = ic.get("upstream_node_ids")
        if isinstance(prior_upstream, list) and list(prior_upstream) == target:
            count += 1
    return count


def _collect_staged_upstream_positions(
    session: sessions.AgentSession,
) -> dict[int, tuple[float, float]]:
    """Build ``{node_id: (pos_x, pos_y)}`` for every prior in-batch staged
    ``add_*`` so the executor's auto-layout resolver can anchor chained
    adds onto staged-but-unapplied upstreams. Without this the second add
    in a multi-step plan can't find its upstream in ``flow.nodes`` (the
    prior add is only staged, not applied), and the resolver falls back
    to the cold-flow seed at (50, 50).
    """
    out: dict[int, tuple[float, float]] = {}
    for entry in session.staged_results:
        if not entry.tool_name.startswith(_ADD_PREFIX):
            continue
        payload = entry.staged_node_payload if isinstance(entry.staged_node_payload, dict) else {}
        if not isinstance(payload, dict):
            continue
        settings = payload.get("settings")
        if not isinstance(settings, dict):
            continue
        nid = settings.get("node_id")
        sx = settings.get("pos_x")
        sy = settings.get("pos_y")
        if not isinstance(nid, int):
            continue
        if not (isinstance(sx, int | float) and isinstance(sy, int | float)):
            # Fall back to the staged insertion_context if settings didn't
            # carry coords (defence-in-depth — the executor stamps both).
            ic = payload.get("insertion_context")
            if isinstance(ic, dict):
                sx = ic.get("pos_x")
                sy = ic.get("pos_y")
        if isinstance(sx, int | float) and isinstance(sy, int | float):
            out[int(nid)] = (float(sx), float(sy))
    return out
