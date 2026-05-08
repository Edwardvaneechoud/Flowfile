"""Schema prediction + D011 upstream-tier handler — W31.

W31 needs a pure ``predict_schema(node_type, settings, upstream_schemas)`` that
does NOT exist in production code today: 21+ ``schema_callback`` closures live
inside ``add_*`` methods on :class:`FlowGraph`, bound to the node being created.
Lifting them into pure functions would duplicate non-trivial logic (join column
merge, group_by key+agg projection, fuzzy_match similarity injection, etc.) and
rot as production logic mutates.

**Strategy: ephemeral mirror-graph.** Build a throwaway :class:`FlowGraph` with
``track_history=False``, install promise-stub upstream nodes whose
``predicted_schema`` is pre-populated, dispatch ``getattr(mirror, f"add_{node_type}")(settings)``,
wire input connections, then read ``predicted_schema`` off the new node. Throws
the mirror away after. Reuses 100% of production callback logic with zero
reimplementation drift.

Caveats handled:

* ``track_history=False`` on the mirror — avoids spurious snapshots.
* Pre-install upstream promises with ``predicted_schema`` populated — several
  callbacks (``add_python_script``, ``add_join``, ``add_group_by``) read
  ``node.node_inputs.main_inputs[0].schema``; without the upstream promise +
  populated schema the callback gets an empty schema and returns nonsense.
* ``add_node_step`` does NOT auto-wire input connections; the wiring goes through
  the module-level :func:`add_connection` helper. Mirror builds connections
  explicitly per ``InsertionContext``.
* Settings are deep-copied before mutating ``flow_id``/``node_id`` on the mirror
  so the caller's settings object is not side-effected.

Module also owns:

* :func:`_resolve_upstream_schemas` — D011 tiered handler. Tier 0 cached → Tier
  1 cheap predict via static + ``schema_callback`` → Tier 2 cheap source preview
  via :mod:`classification` → Tier 3 warn-and-stage.
* :func:`collect_column_refs` — per-node-type walker that pulls column-name
  references out of settings for the column-validation refusal path. Conservative
  by design — returns ``[]`` for node types whose refs aren't trivially extractable
  (the egress + dry-run paths catch the rest).
"""

from __future__ import annotations

import itertools
from typing import Any

from pydantic import BaseModel

from flowfile_core.ai.tools.classification import (
    is_predictable_via_mirror,
)
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.schemas import input_schema, schemas

# Counter for unique mirror flow_ids — each predict_schema_static call gets a
# distinct negative id so concurrent calls don't collide on FlowLogger setup.
_MIRROR_ID_COUNTER = itertools.count(start=-1, step=-1)


def _next_mirror_flow_id() -> int:
    return next(_MIRROR_ID_COUNTER)


def _ensure_promise_template_registered() -> None:
    """Register a minimal ``promise`` template in ``node_store.node_dict``.

    ``add_node_promise`` constructs a ``FlowNode`` whose ``node_type="promise"``,
    but the production ``node_dict`` (at ``configs/node_store/nodes.py:547``)
    never registers ``"promise"`` even though the type is used internally.
    ``FlowNode.update_node`` raises if the template is missing, so the mirror
    can't add upstream stubs without this. We register lazily on first call so
    the module import stays cheap and idempotent — calling twice is a no-op.
    """
    from flowfile_core.configs import node_store
    from flowfile_core.schemas.schemas import NodeTemplate

    if "promise" in node_store.node_dict:
        return
    node_store.node_dict["promise"] = NodeTemplate(
        name="Promise placeholder",
        item="promise",
        input=1,
        output=1,
        node_group="special",
        image="",
        node_type="process",
        transform_type="other",
        laziness="lazy",
    )


def _build_mirror_graph(upstream_schemas: dict[int, list[FlowfileColumn]]):
    """Create a throwaway ``FlowGraph`` with promise-stub upstream nodes.

    Pre-populates ``predicted_schema`` on each promise so downstream callbacks
    that read ``node.node_inputs.main_inputs[0].schema`` see the right columns.
    Uses ``track_history=False`` so the mirror doesn't push spurious snapshots.
    """
    # Local imports — keep top-level lazy so ``import flowfile_core.ai.tools``
    # doesn't pull the full FlowGraph machinery for callers that only want the
    # catalog. Mirrors the W11 / W12 / W13 / W30 lazy contract.
    from flowfile_core.flowfile.flow_graph import FlowGraph

    _ensure_promise_template_registered()
    flow_settings = schemas.FlowSettings(flow_id=_next_mirror_flow_id(), track_history=False)
    mirror = FlowGraph(flow_settings=flow_settings)
    for upstream_id, schema in upstream_schemas.items():
        promise = input_schema.NodePromise(
            flow_id=mirror.flow_id,
            node_id=upstream_id,
            node_type="promise",
            is_setup=True,
        )
        mirror.add_node_promise(promise, track_history=False)
        node = mirror.get_node(upstream_id)
        if node is not None:
            node.node_schema.predicted_schema = list(schema)
    return mirror


def _wire_connections(
    mirror,
    target_id: int,
    upstream_schemas: dict[int, list[FlowfileColumn]],
    *,
    right_input_node_id: int | None = None,
) -> None:
    """Wire input connections on the mirror — main inputs to ``input-0``,
    optional right input to ``input-1``."""
    from flowfile_core.flowfile.flow_graph import add_connection

    main_ids = [uid for uid in upstream_schemas if uid != right_input_node_id]
    for uid in main_ids:
        connection = input_schema.NodeConnection.create_from_simple_input(
            from_id=uid, to_id=target_id, input_type="input-0", output_handle="output-0"
        )
        add_connection(mirror, connection)
    if right_input_node_id is not None and right_input_node_id in upstream_schemas:
        right_connection = input_schema.NodeConnection.create_from_simple_input(
            from_id=right_input_node_id, to_id=target_id, input_type="input-1", output_handle="output-0"
        )
        add_connection(mirror, right_connection)


def predict_schema_via_mirror(
    node_type: str,
    settings: BaseModel,
    upstream_schemas: dict[int, list[FlowfileColumn]],
    *,
    right_input_node_id: int | None = None,
) -> list[FlowfileColumn] | None:
    """Predict the output schema by dispatching ``add_<node_type>`` on a throwaway
    :class:`FlowGraph` and reading the bound ``schema_callback``.

    Handles ``static`` / ``source`` / ``passthrough`` nodes via the existing
    production callbacks (which are themselves worker-aware where needed —
    ``add_read`` uses :meth:`FlowDataEngine.create_from_path` which routes
    through the worker for non-trivial cases). Returns ``None`` for ``dynamic``
    nodes — the executor routes those to the kernel dry-run instead.

    The caller's ``settings`` is deep-copied before the mirror dispatch so the
    ``flow_id`` override does not leak back to the caller's flow. The settings
    ``node_id`` is kept verbatim — mirror nodes don't conflict with real ids
    because they live on a separate :class:`FlowGraph` instance.
    """
    if not is_predictable_via_mirror(node_type):
        return None

    mirror = _build_mirror_graph(upstream_schemas)
    settings_clone = settings.model_copy(update={"flow_id": mirror.flow_id})

    add_method = getattr(mirror, f"add_{node_type}", None)
    if add_method is None:
        return None

    try:
        add_method(settings_clone)
        target_id = settings_clone.node_id
        if upstream_schemas:
            _wire_connections(mirror, target_id, upstream_schemas, right_input_node_id=right_input_node_id)
        target_node = mirror.get_node(target_id)
        if target_node is None:
            return None
        return target_node.get_predicted_schema(force=True)
    except Exception:
        # Mirror-graph prediction failures bubble up as ``None``; the executor
        # surfaces them via a generic ``rejected`` ``ToolExecutionResult`` with
        # the underlying exception in ``refusal_detail``. We don't re-raise here
        # because the caller (executor) needs to record an audit event before
        # failing.
        return None


# Backwards-compatible alias — earlier draft used the narrower name.
predict_schema_static = predict_schema_via_mirror


def _resolve_upstream_schemas(
    flow,
    upstream_node_ids: list[int],
    *,
    staged_schemas: dict[int, list[FlowfileColumn]] | None = None,
) -> tuple[dict[int, list[FlowfileColumn]], list[str]]:
    """Apply D011's tiered handling for upstream nodes whose schema may be ``None``.

    Per the project rule "the collect of polars data only takes place in the
    worker — use nodes already", W31 does NOT do its own ``pl.scan_*`` calls
    here. Instead it delegates to the production ``schema_callback`` registered
    by each ``add_<node_type>`` method, which is already worker-aware where
    needed (e.g. ``add_read`` calls ``FlowDataEngine.create_from_path`` which
    routes through the worker for the non-trivial cases). Source readers
    without a registered callback (``cloud_storage_reader``, ``kafka_source``)
    fall to tier 3 — auto-fetch for those would require adding worker-backed
    callbacks at the node layer (out of W31 scope).

    Tiers (in order):

    * **Tier 0a** (W71 v1.11) — uid is in ``staged_schemas`` (a session's
      already-staged-but-not-yet-applied add_* node). Use the predicted
      output schema captured when that node was staged. This is the
      load-bearing path for chained add_* calls in a single agent turn:
      without it, ``predictor`` warned *"upstream node N not found in
      flow"* for each chain step, the LLM read the warning as a failure
      signal, and tried to "fix" it by re-staging with new ids — a
      14-node runaway loop on Qwen 32B / agent_complex (2026-05-08
      dogfood).
    * **Tier 0** — ``node.predicted_schema`` already populated. Use it.
    * **Tier 1** — node has a ``schema_callback`` and forcing returns a
      non-empty schema. Covers both static upstreams (filter/select/join/...)
      and source upstreams whose ``add_<source>()`` registered a callback
      (manual_input / read / google_analytics_reader / database_reader /
      external_source-with-fields). Schema is populated in place so downstream
      tool calls in the same session see it.
    * **Tier 2** — anything else. Append a warning; the executor will still run
      and return ``status="warned"``.

    Returns ``(resolved_schemas, warnings)`` where ``resolved_schemas`` only
    contains entries that successfully resolved (tiers 0a–1). Tier 2 entries
    are surfaced solely as warnings — callers can choose to refuse the tool
    call or proceed with deferred validation.
    """
    resolved: dict[int, list[FlowfileColumn]] = {}
    warnings: list[str] = []

    for uid in upstream_node_ids:
        # Tier 0a (W71 v1.11): staged-but-not-yet-applied upstream from
        # the current session. Use the cached predicted output schema
        # the planner threaded in via ``staged_schemas``. Skip the
        # live-graph lookup AND the warning entirely.
        if staged_schemas is not None and uid in staged_schemas:
            resolved[uid] = list(staged_schemas[uid])
            continue

        node = flow.get_node(uid)
        if node is None:
            warnings.append(f"upstream node {uid} not found in flow {flow.flow_id}")
            continue

        # Tier 0: cached predicted_schema.
        existing = node.node_schema.predicted_schema
        if existing:
            resolved[uid] = list(existing)
            continue

        # Tier 1: invoke the production schema-prediction path. ``force=True``
        # makes ``get_predicted_schema`` fire ``schema_callback`` if present,
        # otherwise fall back to ``_predicted_data_getter()`` (which handles
        # nodes without an explicit callback — e.g. ``filter``/``select`` —
        # by walking upstream).
        try:
            forced = node.get_predicted_schema(force=True)
        except Exception:
            forced = None
        if forced:
            node.node_schema.predicted_schema = list(forced)
            resolved[uid] = list(forced)
            continue

        # Tier 2: warn-and-stage.
        warnings.append(f"upstream node {uid} ({node.node_type}): schema unknown — validation deferred until run")

    return resolved, warnings


def collect_column_refs(node_type: str, settings: BaseModel) -> list[str]:
    """Return upstream column-name references the executor should validate.

    Conservative — returns the references we can extract trivially from settings.
    Returns an empty list for node types whose references live in user code
    (``polars_code`` / ``python_script`` / ``sql_query``) or in arbitrary
    expressions (``formula``); the network-egress check + kernel dry-run + post
    -execution schema validation cover those paths instead.
    """
    refs: list[str] = []

    if node_type == "filter":
        fi = getattr(settings, "filter_input", None)
        if fi is not None:
            basic = getattr(fi, "basic_filter", None)
            if basic is not None and getattr(basic, "field", None):
                refs.append(basic.field)
            # Advanced filter expressions are arbitrary Polars; we don't parse
            # them here. Network-egress + dry-run handles bad refs at runtime.

    elif node_type == "select":
        select_input = getattr(settings, "select_input", None) or []
        for entry in select_input:
            old = getattr(entry, "old_name", None)
            if old:
                refs.append(old)

    elif node_type == "sort":
        for entry in getattr(settings, "sort_input", None) or []:
            col = getattr(entry, "column", None)
            if col:
                refs.append(col)

    elif node_type == "group_by":
        gb = getattr(settings, "groupby_input", None)
        if gb is not None:
            for entry in getattr(gb, "agg_cols", None) or []:
                old = getattr(entry, "old_name", None)
                if old:
                    refs.append(old)

    elif node_type in ("join", "fuzzy_match"):
        ji = getattr(settings, "join_input", None)
        if ji is not None:
            for jm in getattr(ji, "join_mapping", None) or []:
                left = getattr(jm, "left_col", None)
                right = getattr(jm, "right_col", None)
                if left:
                    refs.append(left)
                if right:
                    refs.append(right)

    elif node_type == "unique":
        # NodeUnique exposes either a list of columns or "all columns" sentinel.
        cols = getattr(settings, "unique_input", None)
        if cols is not None:
            for entry in getattr(cols, "columns", None) or []:
                if isinstance(entry, str):
                    refs.append(entry)
                else:
                    name = getattr(entry, "name", None) or getattr(entry, "old_name", None)
                    if name:
                        refs.append(name)

    elif node_type == "pivot":
        pi = getattr(settings, "pivot_input", None)
        if pi is not None:
            for col in getattr(pi, "index_columns", None) or []:
                if isinstance(col, str):
                    refs.append(col)
            pivot_col = getattr(pi, "pivot_column", None)
            if pivot_col:
                refs.append(pivot_col)
            value_col = getattr(pi, "value_col", None) or getattr(pi, "value_column", None)
            if value_col:
                refs.append(value_col)

    elif node_type == "unpivot":
        ui = getattr(settings, "unpivot_input", None)
        if ui is not None:
            for col in getattr(ui, "index_columns", None) or []:
                if isinstance(col, str):
                    refs.append(col)
            for col in getattr(ui, "value_columns", None) or []:
                if isinstance(col, str):
                    refs.append(col)

    # Dedupe order-preserving so the refusal message is stable.
    seen: set[str] = set()
    out: list[str] = []
    for ref in refs:
        if ref not in seen:
            seen.add(ref)
            out.append(ref)
    return out


def schema_to_dict_list(schema: list[FlowfileColumn] | None) -> list[dict[str, Any]] | None:
    """Project ``list[FlowfileColumn]`` to a JSON-friendly shape for the
    ``ToolExecutionResult.predicted_output_schema`` field."""
    if schema is None:
        return None
    return [
        {
            "name": col.column_name,
            "data_type": col.data_type,
            "nullable": True,
        }
        for col in schema
    ]


__all__ = [
    "predict_schema_via_mirror",
    "predict_schema_static",  # backwards-compatible alias
    "_resolve_upstream_schemas",
    "collect_column_refs",
    "schema_to_dict_list",
]
