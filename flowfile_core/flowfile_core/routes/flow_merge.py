"""Settle the merge conflicts held on an open flow, one at a time, as undoable edits.

A merge that could not settle a step itself holds the conflict on the open flow (``flow.merge_conflicts``, a
``shared.flow_merge.conflict`` wrapper without ``merged``, saved with the flow): the canvas shows one side of the step
(the wrapper's ``shown``; the person's own version after a hub merge), and running and publishing the flow are
refused until every conflict is settled. Settling one on the side the canvas shows only stops holding it; on the other
side the canvas takes that side of the step through the editor's operations, one undo step each.
"""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.flowfile.manage.apply_document import apply_document
from flowfile_core.routes.routes import edit_flow, get_flow_or_404
from flowfile_core.schemas.history_schema import HistoryState
from shared.flow_merge.conflict import is_conflicted, resolve
from shared.flow_merge.render import side_lines

router = APIRouter(prefix="/editor/merge", tags=["editor"])


class MergeConflictOut(BaseModel):
    key: str
    kind: str
    label: str
    message: str
    step_id: int | None = None
    ours_label: str
    theirs_label: str
    ours: list[str]
    theirs: list[str]


class MergeConflictsOut(BaseModel):
    conflicts: list[MergeConflictOut]
    history: HistoryState | None = None


class ResolveConflictRequest(BaseModel):
    flow_id: int
    key: str
    take: Literal["ours", "theirs"]


def _rows(held: dict | None) -> list[MergeConflictOut]:
    if not held:
        return []
    sides = held.get("sides") or {}
    rows = []
    for conflict in held.get("conflicts") or []:
        kind, _, name = conflict["key"].partition(":")
        rows.append(
            MergeConflictOut(
                key=conflict["key"],
                kind=conflict["kind"],
                label=conflict.get("label") or conflict["key"],
                message=conflict.get("message") or "",
                step_id=int(name) if kind == "step" else None,
                ours_label=sides.get("ours", "ours"),
                theirs_label=sides.get("theirs", "theirs"),
                ours=side_lines(conflict.get("base"), conflict.get("ours")) if kind == "step" else [],
                theirs=side_lines(conflict.get("base"), conflict.get("theirs")) if kind == "step" else [],
            )
        )
    return rows


@router.get("/conflicts")
def list_conflicts(flow_id: int, current_user=Depends(get_current_active_user)) -> MergeConflictsOut:
    flow = get_flow_or_404(flow_id, current_user)
    return MergeConflictsOut(conflicts=_rows(getattr(flow, "merge_conflicts", None)))


@router.post("/resolve/")
def resolve_conflict(
    request: ResolveConflictRequest, current_user=Depends(get_current_active_user)
) -> MergeConflictsOut:
    """Take one side of one conflict; the conflicts left come back, none once the flow holds together again.

    Taking the side the canvas shows (the held set's ``shown``) only stops holding the conflict: the canvas, and any
    edit made to the step since the merge, stays as it is and no undo step is recorded. Taking the other side applies
    that side's version of the step as one undo step, which also carries the conflict, so its undo holds it again.
    """
    flow = get_flow_or_404(request.flow_id, current_user)
    # Read and replace the held set under one lock, or two settles put back each other's conflict.
    with flow.edit_lock():
        held = getattr(flow, "merge_conflicts", None)
        if not held or request.key not in {c["key"] for c in held.get("conflicts") or []}:
            raise HTTPException(status_code=404, detail=f"No open merge conflict {request.key} in this flow.")
        label = next(c.get("label") or c["key"] for c in held["conflicts"] if c["key"] == request.key)
        side = (held.get("sides") or {}).get(request.take, request.take)
        description = f"Settle {label}: take {side}"
        if held.get("shown") == request.take:
            left = [c for c in held["conflicts"] if c["key"] != request.key]
            with edit_flow(flow, description, record=False) as txn:
                flow.merge_conflicts = {**held, "conflicts": left} if left else None
            history = txn.history
        else:
            current = {**held, "merged": flow.get_flowfile_data().model_dump(mode="json")}
            settled = resolve(current, {request.key: request.take})
            target = settled["merged"] if is_conflicted(settled) else settled
            still_held = {k: v for k, v in settled.items() if k != "merged"} if is_conflicted(settled) else None

            def _hold() -> None:
                flow.merge_conflicts = still_held

            history, _ = apply_document(flow, target, description, current_user, then=_hold)
    if getattr(flow, "project_merge", None):
        from flowfile_core.project import project_sync

        project_sync.record_editor_settle(flow)
    return MergeConflictsOut(conflicts=_rows(flow.merge_conflicts), history=history)


def refuse_open_conflicts(flow) -> None:
    """409 ``UNRESOLVED_CONFLICTS`` while the flow holds merge conflicts nobody settled."""
    held = getattr(flow, "merge_conflicts", None)
    if held:
        count = len(held.get("conflicts") or [])
        raise HTTPException(
            status_code=409,
            detail={
                "kind": "unresolved_conflicts",
                "code": "UNRESOLVED_CONFLICTS",
                "count": count,
                "message": f"Settle the {count} merge conflict{'s' if count != 1 else ''} in this flow first.",
            },
        )
