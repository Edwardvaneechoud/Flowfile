"""HTTP routes for ``GraphDiff`` staging.

Mounted under ``/ai`` from :mod:`flowfile_core.ai.routes`. Auth via
``Depends(get_current_active_user)``; the feature-flag gate covers
all three endpoints through the parent ``ai_router``.

Three endpoints:

* ``POST /ai/diff/stage`` — register a
  :class:`flowfile_core.ai.diff.GraphDiff` composed from a list of
  staged tool results. Returns the server-generated ``diff_id``.
* ``POST /ai/diff/{diff_id}/accept`` — drift check, atomic apply via
  :func:`flowfile_core.ai.diff.apply_diff`, and one-transaction flip
  of every collected ``audit_id`` to ``"accepted"`` via
  :func:`flowfile_core.ai.audit.update_diff_action`.
* ``POST /ai/diff/{diff_id}/reject`` — flip every collected
  ``audit_id`` to ``"rejected"``, pop from the store, no graph
  mutation.

Error mapping:

* ``404`` — unknown ``diff_id``; flow not found.
* ``409`` — drift detected before mutation (diff stays in store).
* ``422`` — Pydantic validation; cross-flow id mismatch; staged-diff
  inconsistency (staged connection references a node id that's
  neither live nor in the diff's additions; diff stays in store);
  mid-batch raise (graph is rolled back; diff stays in store so user
  can fix-and-retry).
* ``503`` — ``FEATURE_FLAG_AI`` off (inherited router-level).
"""

from __future__ import annotations

import asyncio
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from flowfile_core import flow_file_handler
from flowfile_core.ai import audit, diff
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import SessionLocal, ensure_db_initialized, get_db

router = APIRouter()


_GRAPH_PREFIX = "flowfile.graph."
_ADD_PREFIX = "flowfile.graph.add_"
_CONNECT_NAME = "flowfile.graph.connect"
_DELETE_NODE_NAME = "flowfile.graph.delete_node"
_DELETE_CONNECTION_NAME = "flowfile.graph.delete_connection"


# Request / response shapes


class StagedToolResult(BaseModel):
    """One staged tool call from a prior ``execute_tool_call(mode="stage")``.

    Mirrors the subset of :class:`ToolExecutionResult` the staging
    route actually needs: tool name (for dispatch), audit id (for
    accept/reject flip), and the per-op ``staged_node_payload`` shape
    the executor emits.
    """

    tool_name: str = Field(min_length=1)
    audit_id: int | None = None
    staged_node_payload: dict[str, Any] = Field(default_factory=dict)


class StageDiffRequest(BaseModel):
    """Body for ``POST /ai/diff/stage``."""

    session_id: str = Field(min_length=1)
    flow_id: int = Field(ge=0)
    staged_results: list[StagedToolResult] = Field(default_factory=list)
    rationale: str | None = Field(default=None, max_length=2_000)


class StageDiffResponse(BaseModel):
    """Response from ``POST /ai/diff/stage``."""

    diff_id: str
    op_count: int


class AcceptDiffRequest(BaseModel):
    """Body for ``POST /ai/diff/{diff_id}/accept``.

    The route already knows the diff's ``flow_id`` from the registered
    record; the body's ``flow_id`` is a redundancy check that catches the
    "client sends wrong id" class of bug — it must match the stored value
    or 422.
    """

    flow_id: int = Field(ge=0)


class AcceptDiffResponse(BaseModel):
    """Response from ``POST /ai/diff/{diff_id}/accept``."""

    status: Literal["accepted"] = "accepted"
    diff_id: str
    applied_node_ids: list[int]
    applied_connection_count: int
    removed_node_ids: list[int]
    removed_connection_count: int
    audit_ids_updated: list[int]
    history_action: str = "batch"


class RejectDiffResponse(BaseModel):
    """Response from ``POST /ai/diff/{diff_id}/reject``."""

    status: Literal["rejected"] = "rejected"
    diff_id: str
    audit_ids_updated: list[int]


# Helpers


def _resolve_flow(flow_id: int):
    flow = flow_file_handler.get_flow(flow_id)
    if flow is None:
        raise HTTPException(status_code=404, detail=f"Flow {flow_id} not found")
    return flow


def _bin_staged_results(staged: list[StagedToolResult]) -> diff.GraphDiff:
    """Bin staged results into the four :class:`GraphDiff` buckets.

    Thin route-layer adapter around
    :func:`flowfile_core.ai.diff.bundle_staged_results`: converts the
    ``StagedToolResult`` route models into the public
    :class:`flowfile_core.ai.diff.StagedToolEntry` shape and maps any
    ``ValueError`` from the bundler to ``HTTPException(422)``. The
    bundler itself is shared with the planner (which builds diffs
    without going through the HTTP route).
    """
    entries = [
        diff.StagedToolEntry(
            tool_name=entry.tool_name,
            audit_id=entry.audit_id,
            staged_node_payload=entry.staged_node_payload,
        )
        for entry in staged
    ]
    try:
        return diff.bundle_staged_results(entries)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _flip_audit_actions(audit_ids: list[int], action: audit.DiffAction) -> list[int]:
    """Update ``diff_action`` on every audit row under one DB transaction.

    Returns the subset of ``audit_ids`` that resolved to a real row;
    stale ids (DB reset between staging and accept) are skipped
    silently rather than failing the request — matches the audit
    contract.
    """
    if not audit_ids:
        return []
    updated: list[int] = []
    ensure_db_initialized()
    with SessionLocal() as session:
        for aid in audit_ids:
            row = audit.update_diff_action(aid, action, db=session)
            if row is not None:
                updated.append(aid)
        session.commit()
    return updated


# Routes


@router.post("/diff/stage", response_model=StageDiffResponse, tags=["ai"])
async def stage_diff(
    body: StageDiffRequest,
    current_user=Depends(get_current_active_user),  # noqa: ARG001 — auth gate only
) -> StageDiffResponse:
    """Register a :class:`GraphDiff` from a list of staged tool results.

    The staged results come from prior
    ``execute_tool_call(mode="stage")`` calls — typically driven by the
    planner, but this endpoint is callable directly so the diff
    machinery is testable in isolation.

    Errors:

    * ``422`` — unsupported tool name / bad payload shape.
    * ``503`` — ``FEATURE_FLAG_AI`` off.
    """
    bundled = _bin_staged_results(body.staged_results)
    graph_diff = diff.GraphDiff(
        session_id=body.session_id,
        flow_id=body.flow_id,
        additions=bundled.additions,
        connections_added=bundled.connections_added,
        deletions=bundled.deletions,
        connections_removed=bundled.connections_removed,
        rationale=body.rationale,
    )
    diff_id = diff.register_diff(graph_diff)
    op_count = (
        len(graph_diff.additions)
        + len(graph_diff.connections_added)
        + len(graph_diff.deletions)
        + len(graph_diff.connections_removed)
    )
    return StageDiffResponse(diff_id=diff_id, op_count=op_count)


@router.post(
    "/diff/{diff_id}/accept",
    response_model=AcceptDiffResponse,
    tags=["ai"],
)
async def accept_diff(
    diff_id: str,
    body: AcceptDiffRequest,
    current_user=Depends(get_current_active_user),  # noqa: ARG001 — auth gate only
    db: Session = Depends(get_db),  # noqa: ARG001 — surfaces 503 + ensures session import path
) -> AcceptDiffResponse:
    """Apply ``diff_id`` atomically and flip every audit row to ``"accepted"``.

    Errors:

    * ``404`` — unknown ``diff_id``; or stored ``flow_id`` no longer
      resolves via ``flow_file_handler``.
    * ``409`` — drift (one or more referenced node ids missing). Diff
      stays in the store so the user can fix the underlying graph and
      retry.
    * ``422`` — body ``flow_id`` doesn't match the stored diff's
      ``flow_id``; staged-diff inconsistency (a staged connection
      references a node id that's neither live nor in this diff's
      additions; diff stays in store so the user can Reject and ask
      the agent to retry); or mid-batch raise during apply (graph is
      rolled back; diff stays in store).
    * ``503`` — ``FEATURE_FLAG_AI`` off.
    """
    graph_diff = diff.get_diff(diff_id)
    if graph_diff is None:
        raise HTTPException(status_code=404, detail=f"Unknown diff_id {diff_id!r}")
    if body.flow_id != graph_diff.flow_id:
        raise HTTPException(
            status_code=422,
            detail=(f"flow_id mismatch: body says {body.flow_id}, " f"diff was staged for {graph_diff.flow_id}"),
        )

    flow = _resolve_flow(graph_diff.flow_id)

    try:
        # apply_diff walks every staged add/modify/delete and calls
        # flow.add_<node_type> for each — every call goes through
        # @with_history_capture which serializes the entire live graph.
        # That's hundreds of milliseconds to seconds of synchronous work
        # for any non-trivial flow. Run it on a worker thread so this
        # async route doesn't block the FastAPI event loop.
        result = await asyncio.to_thread(diff.apply_diff, flow, graph_diff)
    except diff.DiffDriftError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "diff_drift",
                "missing_node_ids": exc.missing_node_ids,
                "diff_id": diff_id,
            },
        ) from exc
    except diff.DiffInconsistentError as exc:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "diff_inconsistent",
                "missing_endpoints": [[eid, role] for eid, role in exc.missing_endpoints],
                "diff_id": diff_id,
            },
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=422,
            detail=f"apply_diff failed: {exc}",
        ) from exc

    updated = _flip_audit_actions(result.audit_ids, "accepted")
    diff.pop_diff(diff_id)
    return AcceptDiffResponse(
        diff_id=diff_id,
        applied_node_ids=result.applied_node_ids,
        applied_connection_count=result.applied_connection_count,
        removed_node_ids=result.removed_node_ids,
        removed_connection_count=result.removed_connection_count,
        audit_ids_updated=updated,
        history_action=result.history_action,
    )


@router.post(
    "/diff/{diff_id}/reject",
    response_model=RejectDiffResponse,
    tags=["ai"],
)
async def reject_diff(
    diff_id: str,
    current_user=Depends(get_current_active_user),  # noqa: ARG001 — auth gate only
    db: Session = Depends(get_db),  # noqa: ARG001 — ensures session import path
) -> RejectDiffResponse:
    """Discard ``diff_id`` and flip every audit row to ``"rejected"``.

    No graph mutation happens — the diff was never applied. The audit
    trail records the user's verdict so the pass-rate aggregator and
    cost-per-flow tooling can subtract rejected proposals from the
    "accepted diffs" denominator.

    Errors:

    * ``404`` — unknown ``diff_id``.
    * ``503`` — ``FEATURE_FLAG_AI`` off.
    """
    graph_diff = diff.pop_diff(diff_id)
    if graph_diff is None:
        raise HTTPException(status_code=404, detail=f"Unknown diff_id {diff_id!r}")
    audit_ids = diff.collect_audit_ids(graph_diff)
    updated = _flip_audit_actions(audit_ids, "rejected")
    return RejectDiffResponse(diff_id=diff_id, audit_ids_updated=updated)


__all__ = [
    "AcceptDiffRequest",
    "AcceptDiffResponse",
    "RejectDiffResponse",
    "StageDiffRequest",
    "StageDiffResponse",
    "StagedToolResult",
    "router",
]
