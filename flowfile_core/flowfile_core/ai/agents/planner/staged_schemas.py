"""Staged-result → mirror-graph upstream schema bridge.

The predictor's mirror walk consults ``flow.get_node(uid)`` to fetch an
upstream's predicted schema. Within a single agent turn, the planner can
emit chained adds (filter → sort → group_by) where the second add's
upstream is the just-staged-but-not-yet-applied first add — by
definition not in ``flow.nodes``. These helpers serialise the staged
``predicted_output_schema`` back into ``FlowfileColumn``-shaped objects
so the predictor's Tier 0a lookup can resolve them.
"""

from __future__ import annotations

from typing import Any

from flowfile_core.ai import sessions

from ._internal import _ADD_PREFIX


def _staged_dict_to_flowfile_column(col: dict[str, Any]) -> Any:
    """Reconstruct a ``FlowfileColumn``-shaped object from a serialized
    predicted_output_schema dict.

    ``predictor.schema_to_dict_list`` projects a ``FlowfileColumn`` to
    ``{"name": ..., "data_type": ..., "nullable": True}`` for the wire
    payload (and disk persistence). To feed that back into the predictor's
    mirror-graph as an upstream ``predicted_schema``, we need objects
    that quack like ``FlowfileColumn`` — at minimum ``column_name`` and
    ``data_type`` attributes. The mirror's schema_callback closures only
    read those two; the rest of the dataclass fields are populated with
    safe defaults so reflection / repr / private attributes don't trip.

    Uses ``__new__`` to bypass ``FlowfileColumn.__init__`` (which
    requires a heavy ``PlType`` argument we don't have on the staging
    path).
    """
    from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn

    obj = FlowfileColumn.__new__(FlowfileColumn)
    obj.column_name = str(col.get("name") or "")
    obj.data_type = str(col.get("data_type") or "Unknown")
    obj.size = 0
    obj.max_value = ""
    obj.min_value = ""
    obj.col_index = 0
    obj.number_of_empty_values = 0
    obj.number_of_unique_values = 0
    obj.example_values = ""
    obj.average_value = None
    # Name-mangled private dataclass fields. Setting them to ``None``
    # matches what the regular ``__init__`` does so any access path
    # (e.g. ``__str__``) doesn't AttributeError.
    obj._FlowfileColumn__has_values = None
    obj._FlowfileColumn__nullable = None
    obj._FlowfileColumn__is_unique = None
    obj._FlowfileColumn__sql_type = None
    obj._FlowfileColumn__perc_unique = None
    try:
        obj.data_type_group = obj.get_readable_datatype_group()
    except Exception:
        obj.data_type_group = None
    return obj


def _collect_staged_upstream_schemas(
    session: sessions.AgentSession,
) -> dict[int, list[Any]]:
    """Build ``{node_id: list[FlowfileColumn-like]}`` for every prior
    in-batch staged ``add_*`` whose predicted output schema is known.

    Mirrors :func:`_collect_staged_upstream_positions`'s shape (same
    walk over ``session.staged_results``) so the executor receives a
    parallel ``extra_upstream_schemas`` lookup. The predictor consults
    this BEFORE the live-graph ``flow.get_node(uid)`` lookup at Tier
    0a (see ``predictor._resolve_upstream_schemas``). Without it,
    chained add_* calls in a single agent turn produce *"upstream node
    N not found in flow"* warnings on every step — which the LLM reads
    as a failure signal and tries to "fix" by re-staging with new ids,
    leading to runaway loops on smaller models.
    """
    out: dict[int, list[Any]] = {}
    for entry in session.staged_results:
        if not entry.tool_name.startswith(_ADD_PREFIX):
            continue
        payload = entry.staged_node_payload if isinstance(entry.staged_node_payload, dict) else None
        if payload is None:
            continue
        settings = payload.get("settings") if isinstance(payload.get("settings"), dict) else {}
        nid = settings.get("node_id") if isinstance(settings, dict) else None
        if not isinstance(nid, int):
            continue
        preds = payload.get("predicted_output_schema")
        if not isinstance(preds, list):
            continue
        cols = [_staged_dict_to_flowfile_column(c) for c in preds if isinstance(c, dict)]
        if cols:
            out[int(nid)] = cols
    return out
