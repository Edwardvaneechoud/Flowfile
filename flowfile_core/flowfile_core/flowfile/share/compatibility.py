"""Decide, per node, whether it can travel in a share link as itself.

A node either travels *as itself* (the browser runs it and gets the same rows)
or as a **placeholder** — same identity, position and edges, no settings at all.
Placeholders exist for three reasons at once:

* the browser has no implementation (locked / absent node types);
* it has one, but it reads the settings differently and would run **green and
  silently wrong** (a non-inner join, ``keep_missing``, a per-group record id);
* the settings are executable (``polars_code``, an advanced filter). Those are
  ``exec``/``eval``'d during schema propagation, before anyone clicks Run, so
  they must never reach a recipient's browser regardless of node support.

These predicates read the settings that will actually travel, so an advanced
filter ``filter_translation`` already rewrote into a basic one arrives here as
a basic filter and passes.

Every predicate below was checked against the browser builder it protects
(``flowfile_wasm/src/pyodide/engine/``); when the two engines agree the node
travels, when they disagree it is a placeholder.
"""

from dataclasses import dataclass
from typing import Literal

from flowfile_core.flowfile.share import support

# Placeholders travel under a type no build can execute. A supported type with
# stripped settings would no-op green on the deployed browser app; an unknown
# type fails loudly there and renders properly on the new one.
PLACEHOLDER_SENTINEL_SUFFIX = "__unsupported"

_LOCKED_REASON = "Runs only in the full Flowfile app"
_ABSENT_REASON = "Not available in the browser version"
_CATALOG_REASON = "The browser catalog is a separate, per-browser store"
_CODE_REASON = "Custom Python code does not travel in share links"
_FLOW_INPUT_REASON = "Subflow inputs have no data source in the browser"

_ALWAYS_PLACEHOLDER = {
    "catalog_reader": _CATALOG_REASON,
    "catalog_writer": _CATALOG_REASON,
    "polars_code": _CODE_REASON,
    "flow_input": _FLOW_INPUT_REASON,
}

_GROUP_BY_PASSTHROUGH_AGGS = frozenset({"groupby"})


@dataclass(frozen=True)
class NodeShareStatus:
    """How one node fares in a share link."""

    node_id: int
    node_type: str
    status: Literal["supported", "placeholder"]
    reason: str | None = None


def placeholder_type(node_type: str) -> str:
    """The sentinel type a placeholder for ``node_type`` travels under."""
    return f"{node_type}{PLACEHOLDER_SENTINEL_SUFFIX}"


def _dict(value: object) -> dict:
    return value if isinstance(value, dict) else {}


def _is_identity_select(entries: object) -> bool:
    """Whether a join select list leaves every column exactly as it arrived.

    The browser join applies no select list at all, so anything other than
    "keep everything under its own name, unchanged" runs differently there.
    """
    for entry in entries if isinstance(entries, list) else []:
        entry = _dict(entry)
        if not entry.get("keep", True):
            return False
        if entry.get("data_type_change"):
            return False
        new_name = entry.get("new_name")
        if new_name not in (None, "", entry.get("old_name")):
            return False
    return True


def _join_reason(settings: dict) -> str | None:
    join_input = _dict(settings.get("join_input"))
    if join_input.get("how", "inner") != "inner":
        return "Only inner joins run identically in the browser"
    for side in ("left_select", "right_select"):
        renames = _dict(join_input.get(side)).get("renames")
        if not _is_identity_select(renames):
            return "The browser join keeps every column; this join renames or drops some"
    return None


def _select_reason(settings: dict) -> str | None:
    # Data-type changes are fine: the browser select casts exactly like core
    # (non-strict, before the rename — see flowfile_wasm build_select).
    if settings.get("keep_missing") is True:
        return "The browser select drops columns it does not list; this one keeps them"
    return None


def _filter_reason(settings: dict) -> str | None:
    filter_input = _dict(settings.get("filter_input"))
    if filter_input.get("mode") == "advanced":
        # The browser runs these correctly now (same parser core uses), but the
        # parser still executes Python for a crafted formula — see the xfail in
        # flowfile_wasm/tests/python/test_build_helpers.py. A sender-authored
        # expression must not reach a recipient until that is fixed.
        return "Advanced filter expressions are executable code and do not travel in share links"
    if settings.get("split_mode"):
        return "Two-output (pass/fail) filters are not available in the browser version"
    return None


def _group_by_reason(settings: dict) -> str | None:
    allowed = support.group_by_aggs() | _GROUP_BY_PASSTHROUGH_AGGS
    for entry in _dict(settings.get("groupby_input")).get("agg_cols") or []:
        agg = _dict(entry).get("agg")
        if agg not in allowed:
            return f"The browser group by has no '{agg}' aggregation"
    return None


def _pivot_reason(settings: dict) -> str | None:
    allowed = support.pivot_aggs()
    for agg in _dict(settings.get("pivot_input")).get("aggregations") or []:
        if agg not in allowed:
            return f"The browser pivot has no '{agg}' aggregation"
    return None


def _read_reason(settings: dict) -> str | None:
    received_file = _dict(settings.get("received_file"))
    file_type = received_file.get("file_type")
    if file_type not in support.read_file_types():
        return f"The browser version cannot read {file_type} files"
    if received_file.get("scan_mode") == "directory":
        return "The browser version cannot read a whole directory"
    return None


def _output_reason(settings: dict) -> str | None:
    file_type = _dict(settings.get("output_settings")).get("file_type")
    if file_type not in support.output_file_types():
        return f"The browser version cannot write {file_type} files"
    return None


def _record_id_reason(settings: dict) -> str | None:
    if _dict(settings.get("record_id_input")).get("group_by") is True:
        return "Per-group record IDs are not available in the browser version"
    return None


_SETTINGS_PREDICATES = {
    "filter": _filter_reason,
    "join": _join_reason,
    "select": _select_reason,
    "record_id": _record_id_reason,
    "group_by": _group_by_reason,
    "pivot": _pivot_reason,
    "read": _read_reason,
    "output": _output_reason,
}


def classify_node(node_id: int, node_type: str, settings: dict | None) -> NodeShareStatus:
    """Classify one node from its **dumped** ``setting_input`` dict."""
    tier = support.tier_for(node_type)
    if tier is support.SupportTier.LOCKED:
        return NodeShareStatus(node_id, node_type, "placeholder", _LOCKED_REASON)
    if tier is not support.SupportTier.SUPPORTED:
        return NodeShareStatus(node_id, node_type, "placeholder", _ABSENT_REASON)

    always = _ALWAYS_PLACEHOLDER.get(node_type)
    if always:
        return NodeShareStatus(node_id, node_type, "placeholder", always)

    predicate = _SETTINGS_PREDICATES.get(node_type)
    if predicate is not None:
        reason = predicate(_dict(settings))
        if reason:
            return NodeShareStatus(node_id, node_type, "placeholder", reason)
    return NodeShareStatus(node_id, node_type, "supported")
