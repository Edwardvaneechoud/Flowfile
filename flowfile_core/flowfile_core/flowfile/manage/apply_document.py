"""Make an open flow what a document says, as one undoable step, through the editor's own operations.

A merge and the settling of a merge conflict both end with a flow document the open canvas should become. Writing the
graph from it would bypass every check a person's edit passes, so the difference is turned into the editor's batch
operations (``notebook.reconcile`` with every node in one changed cell, so nothing is pinned) and applied in one
transaction, which records one undo step and restores the graph if any operation fails.
"""

from __future__ import annotations

from flowfile_core.schemas.history_schema import HistoryActionType, HistoryState


def _names(document: dict) -> dict[int, str | None]:
    return {node["id"]: node.get("node_reference") for node in document.get("nodes") or []}


def apply_document(flow, target: dict, label: str, current_user) -> tuple[HistoryState, list[str]]:
    """Turn ``flow`` into ``target`` (a saved-flow document); answers the history state and reconcile's warnings."""
    from flowfile_core.notebook.reconcile import reconcile
    from flowfile_core.routes.routes import _run_operations, edit_flow

    live = flow.get_flowfile_data().model_dump(mode="json")
    every_id = [n["id"] for n in target.get("nodes") or []] + [n["id"] for n in live.get("nodes") or []]
    plan = reconcile(
        live,
        target,
        {"document"},
        {"document": every_id},
        _names(live),
        _names(target),
        live["flowfile_settings"].get("parameters") or [],
        (target.get("flowfile_settings") or {}).get("parameters") or [],
        group_id_ceiling=flow.group_id_ceiling,
        groups_cell_changed=True,
    )
    if not plan.operations:
        return flow.get_history_state(), list(plan.warnings)
    with edit_flow(flow, label, HistoryActionType.BATCH) as txn:
        _run_operations(flow, flow.flow_id, plan.operations, current_user)
    return txn.history, list(plan.warnings)
