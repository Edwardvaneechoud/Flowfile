"""HTTP route for the "Lineage Q&A across runs" surface.

Mounted under ``/ai`` from :mod:`flowfile_core.ai.routes`. Auth via
``Depends(get_current_active_user)``; the feature-flag gate covers
this through the parent ``ai_router``.

Like ``run_failure_routes`` and ``docgen_routes``, this surface is
read-only by construction — ``tools=None`` is passed to the
provider's ``stream()`` so no ``tool_call_delta`` is ever emitted.
The route streams an Assist-level natural-language answer to the
user's lineage question.

The single endpoint takes ``{flow_id, question, ...}``, looks up the
``FlowGraph`` via the existing ``flow_file_handler`` singleton,
optionally narrows the pinned set to a focus node, builds a
schema-grounded :class:`PromptContext` via
:func:`flowfile_core.ai.context.render_prompt_context` (surface
``"lineage"``), reads the flow's run history from the catalog
(``FlowRunService.list_runs`` + ``get_run_detail``) — falling back
to ``flow.latest_run_info`` when the flow has no
``source_registration_id`` — and appends a structured ``## Run
history`` block (per-run summary table + per-node aggregates) plus a
``## Question`` block to ``ctx.user``. The composed ``[system,
user]`` message pair is streamed through the SSE primitives exactly
so the wire format matches ``/ai/chat/stream`` and
``/ai/explain_run_failure``.

Provider resolution flows through
:func:`flowfile_core.ai.byok.get_configured_provider` so BYOK rows
+ env-var fallback + surface-keyed model defaults are honoured
identically to the chat route.
"""

from __future__ import annotations

import json
import logging
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from flowfile_core import flow_file_handler
from flowfile_core.ai.byok import ProviderNotConfiguredError, get_configured_provider
from flowfile_core.ai.context import render_prompt_context
from flowfile_core.ai.providers import (
    Message,
    UnknownProviderError,
    is_resolvable_provider,
    resolvable_provider_names,
)
from flowfile_core.ai.streaming import make_streaming_response, sse_stream
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db

logger = logging.getLogger(__name__)

router = APIRouter()


SamplesMode = Literal["off", "regex"]

_HISTORY_LIMIT_DEFAULT = 10
_HISTORY_LIMIT_MAX = 50


class LineageQuestionRequest(BaseModel):
    """Body for ``POST /ai/lineage_question``.

    ``focus_node_id`` is optional — when provided, the prompt
    builder's BFS pins that single node id and walks upstream; when
    omitted, every node id in the flow is pinned (whole-flow lineage).

    ``history_limit`` controls how many recent runs the route pulls
    from the catalog. Defaults to 10, capped at 50 to keep the prompt
    block bounded against multi-thousand-run flows.

    ``samples_mode`` defaults to ``"off"``. The frontend doesn't
    expose a UI toggle today; a per-flow safety config workstream
    will surface it.
    """

    flow_id: int
    question: str = Field(min_length=1)
    provider: str = Field(min_length=1)
    model: str | None = None
    focus_node_id: int | None = None
    history_limit: int = Field(default=_HISTORY_LIMIT_DEFAULT, ge=1, le=_HISTORY_LIMIT_MAX)
    samples_mode: SamplesMode = "off"
    max_tokens: int | None = Field(default=None, gt=0)


@dataclass(slots=True)
class _RunSummary:
    """One row in the per-run summary table.

    Mirrors the ``FlowRun`` columns we render — kept private so callers
    don't accidentally lean on the shape.
    """

    run_id: int
    started_at: datetime | None
    ended_at: datetime | None
    duration_seconds: float | None
    success: bool | None
    nodes_completed: int
    number_of_nodes: int
    run_type: str


@dataclass(slots=True)
class _NodeAggregate:
    """Per-node behaviour aggregates across the history window.

    ``most_recent_error`` is ``(run_id, error_text)`` so the rendering
    layer can name the run when describing the failure.
    """

    node_id: int
    node_name: str | None
    node_type: str | None
    success_count: int = 0
    failure_count: int = 0
    skip_count: int = 0
    most_recent_error: tuple[int, str] | None = None
    run_times_ms: list[int] = field(default_factory=list)


@dataclass(slots=True)
class _LineageWindow:
    """The materialised history window the formatter renders.

    ``runs`` is newest-first (matches ``FlowRunService.list_runs`` order).
    ``per_node`` is keyed by node id (so the route can subset by
    ``focus_node_id`` cheaply).
    """

    flow_name: str
    registration_id: int | None
    runs: list[_RunSummary]
    per_node: dict[int, _NodeAggregate]


def _ensure_known_provider(name: str) -> None:
    if not is_resolvable_provider(name):
        raise HTTPException(
            status_code=404,
            detail=f"Unknown provider {name!r}; supported: {resolvable_provider_names()}",
        )


def _resolve_flow_name(flow: Any, flow_id: int) -> str:
    """Return the flow display name.

    Falls back to ``f"flow-{flow_id}"`` when ``flow_settings.name`` is
    missing or empty (e.g. an unsaved flow).
    """

    settings = getattr(flow, "flow_settings", None)
    name = getattr(settings, "name", None) if settings is not None else None
    if isinstance(name, str) and name.strip():
        return name.strip()
    return f"flow-{flow_id}"


def _build_catalog_service(db: Session) -> CatalogService:
    """Construct a ``CatalogService`` for one request.

    Mirrors :func:`flowfile_core.routes.catalog.get_catalog_service`
    without taking a hard import dependency on the routes layer.
    """

    return CatalogService(SQLAlchemyCatalogRepository(db))


def _node_label_for(flow: Any, node_id: int) -> tuple[str | None, str | None]:
    """Return ``(node_name, node_type)`` if the flow still has the node.

    Returns ``(None, None)`` when the historical run referenced a node
    id that no longer exists in the live flow — happens when a node has
    been deleted between runs.
    """

    node = flow.get_node(node_id) if hasattr(flow, "get_node") else None
    if node is None:
        return None, None
    return getattr(node, "name", None), getattr(node, "node_type", None)


def _aggregate_node_results(
    flow: Any,
    runs: list[_RunSummary],
    node_results_by_run: dict[int, list[dict[str, Any]]],
) -> dict[int, _NodeAggregate]:
    """Roll up per-node behaviour across the run window.

    ``runs`` is newest-first. The aggregator walks each run's parsed
    node-result list defensively — corrupt rows are skipped, never
    raised — and keeps:

    * success / failure / skip counts
    * the most recent error (run_id + text), where "most recent" means
      first encountered while iterating ``runs`` in newest-first order
    * run-time samples (only for successful runs, to avoid mixing
      truncated failures into the median)
    """

    aggregates: dict[int, _NodeAggregate] = {}

    for run in runs:
        results = node_results_by_run.get(run.run_id, [])
        for raw in results:
            node_id = raw.get("node_id")
            if not isinstance(node_id, int):
                continue
            agg = aggregates.get(node_id)
            if agg is None:
                name, ntype = _node_label_for(flow, node_id)
                agg = _NodeAggregate(node_id=node_id, node_name=name, node_type=ntype)
                aggregates[node_id] = agg
            success = raw.get("success")
            error = raw.get("error") or ""
            run_time_ms = raw.get("run_time_ms")
            if success is True:
                agg.success_count += 1
                if isinstance(run_time_ms, int) and run_time_ms >= 0:
                    agg.run_times_ms.append(run_time_ms)
            elif success is False:
                agg.failure_count += 1
                if agg.most_recent_error is None and error:
                    agg.most_recent_error = (run.run_id, error.strip())
            else:
                agg.skip_count += 1
    return aggregates


def _parse_node_results_json(raw: str | None) -> list[dict[str, Any]]:
    """Defensive parse of the ``FlowRun.node_results_json`` Text column.

    Returns an empty list (not None, not a raise) on any parse error so
    a single corrupt row can't drop the whole history block. The
    failure is logged at WARNING so it's still observable.
    """

    if raw is None or not raw.strip():
        return []
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError) as exc:
        logger.warning("Failed to parse node_results_json: %s", exc)
        return []
    if not isinstance(parsed, list):
        logger.warning("node_results_json was not a list: %r", type(parsed).__name__)
        return []
    return [item for item in parsed if isinstance(item, dict)]


def _run_summary_from_out(run_out: Any) -> _RunSummary:
    """Adapt a ``FlowRunOut`` Pydantic to the private ``_RunSummary``.

    Tolerates missing fields (defaults match the ``FlowRunOut`` Pydantic
    defaults — ``nodes_completed=0``, ``number_of_nodes=0``,
    ``run_type="in_designer_run"``) so a partial row from an older
    schema doesn't hard-fail the route.
    """

    return _RunSummary(
        run_id=int(run_out.id),
        started_at=getattr(run_out, "started_at", None),
        ended_at=getattr(run_out, "ended_at", None),
        duration_seconds=getattr(run_out, "duration_seconds", None),
        success=getattr(run_out, "success", None),
        nodes_completed=int(getattr(run_out, "nodes_completed", 0) or 0),
        number_of_nodes=int(getattr(run_out, "number_of_nodes", 0) or 0),
        run_type=str(getattr(run_out, "run_type", "in_designer_run") or "in_designer_run"),
    )


def _summary_from_in_memory_run(flow: Any, run_info: Any) -> _RunSummary:
    """Wrap ``flow.latest_run_info`` (in-memory ``RunInformation``) into a
    single ``_RunSummary`` so the formatter can render it identically to
    persisted rows when ``source_registration_id`` is ``None``.
    """

    started = getattr(run_info, "start_time", None)
    ended = getattr(run_info, "end_time", None)
    duration: float | None = None
    if isinstance(started, datetime) and isinstance(ended, datetime):
        try:
            duration = (ended - started).total_seconds()
        except TypeError:
            duration = None
    nodes_completed = int(getattr(run_info, "nodes_completed", 0) or 0)
    number_of_nodes = int(getattr(run_info, "number_of_nodes", 0) or len(flow.nodes))
    return _RunSummary(
        run_id=0,  # in-memory run has no DB id; rendered as "live"
        started_at=started,
        ended_at=ended,
        duration_seconds=duration,
        success=getattr(run_info, "success", None),
        nodes_completed=nodes_completed,
        number_of_nodes=number_of_nodes,
        run_type=str(getattr(run_info, "run_type", "in_designer_run") or "in_designer_run"),
    )


def _node_results_from_in_memory_run(run_info: Any) -> list[dict[str, Any]]:
    """Convert ``RunInformation.node_step_result`` (Pydantic) into the
    same dict shape that ``_aggregate_node_results`` consumes from the
    persisted ``node_results_json`` parse.
    """

    out: list[dict[str, Any]] = []
    for nr in getattr(run_info, "node_step_result", None) or []:
        try:
            out.append(nr.model_dump(mode="json"))
        except Exception as exc:  # noqa: BLE001 — pydantic raises various
            logger.warning("Failed to dump in-memory node_result: %s", exc)
    return out


def _collect_run_history(
    flow: Any,
    flow_id: int,
    catalog_service: CatalogService,
    *,
    limit: int,
) -> _LineageWindow:
    """Materialise the per-flow run-history window the LLM will read.

    Resolution order (matches the route-side fallback contract):

    1. If ``flow.flow_settings.source_registration_id`` is set, query
       :meth:`CatalogService.list_runs` paginated by ``registration_id``
       (newest-first), then load each run's ``node_results_json`` via
       :meth:`CatalogService.get_run_detail`.
    2. If no registration is attached **and** the flow has a
       ``latest_run_info``, wrap that into a single-row in-memory window.
    3. Otherwise return an empty window — the formatter renders the
       "no history available" branch.
    """

    flow_name = _resolve_flow_name(flow, flow_id)
    settings = getattr(flow, "flow_settings", None)
    registration_id = getattr(settings, "source_registration_id", None) if settings is not None else None

    if registration_id is not None:
        try:
            paginated = catalog_service.list_runs(registration_id=registration_id, limit=limit, offset=0)
        except Exception as exc:  # noqa: BLE001 — defensive against repo failures
            logger.warning("CatalogService.list_runs failed for registration %s: %s", registration_id, exc)
            paginated = None
        run_outs = list(getattr(paginated, "items", []) or []) if paginated is not None else []
        runs = [_run_summary_from_out(r) for r in run_outs]
        node_results_by_run: dict[int, list[dict[str, Any]]] = {}
        for summary in runs:
            try:
                detail = catalog_service.get_run_detail(summary.run_id)
            except Exception as exc:  # noqa: BLE001
                logger.warning("CatalogService.get_run_detail failed for run %s: %s", summary.run_id, exc)
                continue
            node_results_by_run[summary.run_id] = _parse_node_results_json(getattr(detail, "node_results_json", None))
        per_node = _aggregate_node_results(flow, runs, node_results_by_run)
        return _LineageWindow(
            flow_name=flow_name,
            registration_id=registration_id,
            runs=runs,
            per_node=per_node,
        )

    run_info = getattr(flow, "latest_run_info", None)
    if run_info is not None:
        summary = _summary_from_in_memory_run(flow, run_info)
        node_results_by_run = {summary.run_id: _node_results_from_in_memory_run(run_info)}
        per_node = _aggregate_node_results(flow, [summary], node_results_by_run)
        return _LineageWindow(
            flow_name=flow_name,
            registration_id=None,
            runs=[summary],
            per_node=per_node,
        )

    return _LineageWindow(
        flow_name=flow_name,
        registration_id=None,
        runs=[],
        per_node={},
    )


def _format_iso(value: datetime | None) -> str:
    if value is None:
        return "--"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "--"
    if seconds >= 1:
        return f"{seconds:.1f}s"
    return f"{int(seconds * 1000)}ms"


def _format_success(success: bool | None) -> str:
    if success is True:
        return "OK"
    if success is False:
        return "FAIL"
    return "RUNNING"


def _format_run_history_block(
    window: _LineageWindow,
    *,
    focus_node_id: int | None,
) -> str:
    """Render the deterministic ``## Run history`` block.

    When ``focus_node_id`` is provided, the per-node aggregates are
    filtered to that node only — keeps the prompt block focused when
    the user is asking about a specific node's lineage. When ``None``,
    every node in the aggregate is rendered (whole-flow lineage).
    """

    reg_label = (
        f"registration_id={window.registration_id}"
        if window.registration_id is not None
        else "no registration_id (in-memory or unsaved flow)"
    )

    if not window.runs:
        return (
            "## Run history\n\n"
            f"Flow: `{window.flow_name}` ({reg_label}) — no run history available; "
            "the flow has not been registered or run yet.\n"
        )

    lines: list[str] = ["## Run history", ""]
    lines.append(f"Flow: `{window.flow_name}` ({reg_label})")
    lines.append(f"Showing last {len(window.runs)} run(s):")
    lines.append("")
    lines.append("| Run | Started | Ended | Duration | Success | Nodes | Type |")
    lines.append("|---|---|---|---|---|---|---|")
    for run in window.runs:
        run_id_label = f"#{run.run_id}" if run.run_id else "live"
        started = _format_iso(run.started_at)
        ended = _format_iso(run.ended_at)
        duration = _format_duration(run.duration_seconds)
        success = _format_success(run.success)
        nodes = f"{run.nodes_completed}/{run.number_of_nodes}"
        lines.append(f"| {run_id_label} | {started} | {ended} | {duration} | {success} | {nodes} | {run.run_type} |")

    if focus_node_id is not None:
        scope_label = f"focus on node {focus_node_id}"
        per_node_iter = [(focus_node_id, window.per_node.get(focus_node_id))]
    else:
        scope_label = "all nodes"
        per_node_iter = sorted(window.per_node.items(), key=lambda kv: kv[0])

    lines.append("")
    lines.append(f"### Per-node behaviour ({scope_label})")

    rendered_any = False
    for node_id, agg in per_node_iter:
        if agg is None:
            lines.append("")
            lines.append(f"#### Node {node_id}")
            lines.append("- No run history found for this node id in the window above.")
            continue
        rendered_any = True
        total = agg.success_count + agg.failure_count + agg.skip_count
        label = agg.node_name or f"node-{node_id}"
        node_type = agg.node_type or "unknown"
        lines.append("")
        lines.append(f"#### Node {node_id} — `{label}` ({node_type})")
        lines.append(f"- Successful: {agg.success_count}/{total} runs in this window.")
        if agg.failure_count > 0 and agg.most_recent_error is not None:
            err_run_id, err_text = agg.most_recent_error
            run_label = f"#{err_run_id}" if err_run_id else "live"
            lines.append(f"- Most recent error (run {run_label}): `{err_text}`")
        elif agg.failure_count > 0:
            lines.append(f"- {agg.failure_count} failed run(s) in this window.")
        if agg.skip_count > 0:
            lines.append(f"- Skipped in {agg.skip_count} run(s) (typically due to upstream failure).")
        if agg.run_times_ms:
            median = int(statistics.median(agg.run_times_ms))
            lo = min(agg.run_times_ms)
            hi = max(agg.run_times_ms)
            lines.append(f"- Run-time: median {median}ms, range {lo}-{hi}ms.")

    if not rendered_any and focus_node_id is None and window.per_node:
        # Defensive — shouldn't trigger because per_node_iter is built from the
        # same dict — but keeps the formatter from emitting an empty section.
        lines.append("- No per-node aggregates could be computed.")

    return "\n".join(lines) + "\n"


def _compose_lineage_user_message(
    *,
    rendered_user: str,
    history_block: str,
    question: str,
) -> str:
    """Build the user-message body the LLM sees.

    Re-uses the deterministic ``ctx.user`` (subgraph + schemas +
    settings) verbatim so its prompt-cache hash stays stable, then
    appends the ``## Run history`` block and a ``## Question`` block
    carrying the user's literal question + answer-shape rules.
    """

    return (
        f"{rendered_user}\n\n"
        f"{history_block}\n"
        "## Question\n\n"
        f"{question.strip()}\n\n"
        "Rules:\n"
        "- Cite only column names, node names, and node ids that appear in the schemas "
        "or run history above.\n"
        "- When you can identify a change point across runs, name the run id and date.\n"
        "- If the question can't be answered from the history above, say so explicitly.\n"
        "- Do not propose graph mutations — this is read-only assist.\n"
        "- Output plain English. Do not wrap the answer in code fences.\n"
    )


def _resolve_pinned_node_ids(flow: Any, focus_node_id: int | None, flow_id: int) -> list[int]:
    """Decide which nodes to pin into the subgraph extraction.

    * ``focus_node_id is None`` → every node (whole-flow lineage).
    * ``focus_node_id is set`` → that single node id; the prompt
      builder's BFS walks upstream from it to bound the rendered
      context.
    """

    if focus_node_id is None:
        return [node.node_id for node in flow.nodes]

    if flow.get_node(focus_node_id) is None:
        raise HTTPException(
            status_code=422,
            detail=f"focus_node_id {focus_node_id} not found in flow {flow_id}",
        )
    return [focus_node_id]


@router.post("/lineage_question", tags=["ai"])
async def lineage_question(
    body: LineageQuestionRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Stream an Assist-level lineage answer grounded in run history.

    Errors before the stream opens:

    * ``404`` — provider name is not in :data:`PROVIDERS`.
    * ``409`` — no BYOK row, no env-var fallback, not Ollama
      (:class:`ProviderNotConfiguredError`).
    * ``422`` — flow not found, flow has no nodes, focus node not found,
      empty question, or Pydantic validation.
    * ``503`` — ``FEATURE_FLAG_AI`` is off (inherited from the parent
      router-level dependency).

    Once the stream opens, transient provider errors surface as
    ``event: error`` payloads from :func:`sse_stream` and the response
    closes — same posture as :mod:`flowfile_core.ai.chat_routes` and
    :mod:`flowfile_core.ai.run_failure_routes`.
    """

    _ensure_known_provider(body.provider)

    flow = flow_file_handler.get_flow(body.flow_id)
    if flow is None:
        raise HTTPException(status_code=422, detail=f"Flow {body.flow_id} not found")

    if not flow.nodes:
        raise HTTPException(
            status_code=422,
            detail=f"Flow {body.flow_id} has no nodes",
        )

    pinned_node_ids = _resolve_pinned_node_ids(flow, body.focus_node_id, body.flow_id)

    try:
        provider = get_configured_provider(
            db,
            current_user.id,
            body.provider,
            surface="lineage",
            model=body.model,
        )
    except ProviderNotConfiguredError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except UnknownProviderError as exc:
        # Defence-in-depth — _ensure_known_provider already covers the
        # PROVIDERS-side mapping, but provider_factory has its own check.
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    ctx = render_prompt_context(
        flow,
        pinned_node_ids,
        surface="lineage",
        samples_mode=body.samples_mode,
    )

    catalog_service = _build_catalog_service(db)
    window = _collect_run_history(flow, body.flow_id, catalog_service, limit=body.history_limit)
    history_block = _format_run_history_block(window, focus_node_id=body.focus_node_id)
    user_text = _compose_lineage_user_message(
        rendered_user=ctx.user,
        history_block=history_block,
        question=body.question,
    )
    messages = [
        Message(role="system", content=ctx.system),
        Message(role="user", content=user_text),
    ]

    provider_stream = provider.stream(
        messages=messages,
        tools=None,
        max_tokens=body.max_tokens,
    )

    return make_streaming_response(sse_stream(provider_stream))


__all__ = ["router"]
