"""Post-apply observation for ``surface=agent_live``.

After every successful ``execute_tool_call(mode="apply")`` round, the
agent_live planner needs to surface what the new node ACTUALLY did
back to the LLM as the next tool reply. Two paths, picked by the
flow's execution mode:

* **Performance mode** — call :py:meth:`FlowNode.get_resulting_data`
  on the just-added node. This runs the actual polars pipeline up
  to that node (cheap for typical small-data flows). Returns the
  real output schema + a small sample of rows.

* **Development mode** — call :py:meth:`FlowNode.get_predicted_schema`
  with ``force=True`` to refresh the prediction without actually
  running the pipeline, and synthesize a few representative sample
  rows from upstream's predicted sample. Cheaper than a real
  collect, useful when the user is iterating without committing to
  full execution.

The result either way is an :class:`ObservationResult` shaped to be
serialized into the LLM-facing tool reply. The planner stitches it
into the message history; the LLM observes either *"✓ Output
schema: X, Y, Z; Sample: …"* or *"✗ ColumnNotFoundError: 'amout'
not found"* and decides what to do next.

Auto-undo of the just-added node on failure happens at the planner
level (``_undo_last_apply``); this module only OBSERVES — it does
not mutate the graph. That keeps the observation logic side-effect
free + the undo path explicitly visible at the planner layer.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

from flowfile_core.run_lock import get_flow_run_lock

logger = logging.getLogger(__name__)


_MAX_SAMPLE_ROWS: int = 3


@dataclass
class ObservationResult:
    """Outcome of observing a node after a live apply.

    ``success`` carries the real (or sample-derived) output schema
    and a small sample of rows when the post-apply evaluation
    completed; ``failure`` carries the error message that the LLM
    should read on its next round so it can self-correct.
    """

    success: bool
    node_id: int
    node_type: str
    output_schema: list[dict[str, Any]] = field(default_factory=list)
    sample_rows: list[dict[str, Any]] = field(default_factory=list)
    error_message: str = ""
    error_kind: str = ""


async def observe_after_apply(flow: Any, node_id: int) -> ObservationResult:
    """Run the post-apply observation for ``node_id`` on ``flow``.

    Single entry point. Reads ``flow.flow_settings.execution_mode``
    to pick between the Performance (real run via
    ``flow.trigger_fetch_node``) and Development (schema-only via
    ``get_predicted_schema``) paths. Catches any exception from the
    underlying flowfile layer and returns it as an
    :class:`ObservationResult` with ``success=False``; never
    re-raises so the planner can route cleanly to its retry path.

    Async because both paths can dispatch synchronous worker calls
    (``ExternalDfFetcher`` / ``websockets.sync.client.connect``) and
    must release the FastAPI event loop, and because Performance
    mode acquires the per-flow ``flow_run_lock`` to coordinate with
    UI-driven runs (matching ``routes.trigger_fetch_node_data``).

    The LLM never sees this function directly — the planner's
    ``_observe_after_apply`` wrapper formats the result via
    :func:`format_observation_block` and appends it to the next
    tool reply.
    """
    try:
        node = flow.get_node(node_id)
    except Exception as exc:  # defensive — flow.get_node shouldn't raise
        logger.exception("observe_after_apply: flow.get_node(%s) raised: %s", node_id, exc)
        return ObservationResult(
            success=False,
            node_id=node_id,
            node_type="unknown",
            error_kind=type(exc).__name__,
            error_message=f"failed to look up node {node_id} after apply: {exc}",
        )
    if node is None:
        return ObservationResult(
            success=False,
            node_id=node_id,
            node_type="unknown",
            error_kind="NodeNotFound",
            error_message=(
                f"node {node_id} disappeared from the flow between apply "
                "and observation"
            ),
        )

    node_type = getattr(node, "node_type", "") or ""

    execution_mode = ""
    try:
        execution_mode = str(flow.flow_settings.execution_mode or "")
    except Exception:
        # Never block on a missing settings field; treat the run as Development (the cheap path).
        pass

    # explore_data always runs for real, so its panel has data by the time the LLM points the user at it.
    if node_type == "explore_data" or execution_mode.lower() == "performance":
        return await _observe_performance(flow, node, node_id, node_type)
    return await _observe_development(node, node_id, node_type)


async def _observe_performance(flow: Any, node: Any, node_id: int, node_type: str) -> ObservationResult:
    """Performance-mode observation: actually run the pipeline up to
    the node and capture both schema + sample rows.

    Goes through ``flow.trigger_fetch_node`` — the same primitive
    the UI's ``POST /node/trigger_fetch_data`` route uses
    (``routes.py:244-259``) — so AI observations and user-initiated
    runs are serialised through the per-flow ``flow_run_lock`` and
    the ``flow.flow_settings.is_running`` guard. The lock is held
    only for the duration of this fetch; concurrent runs queue
    cleanly on the asyncio.Lock instead of busy-failing with 422.

    The fetch itself runs on a worker thread via
    ``asyncio.to_thread`` so the synchronous polars / worker IO
    inside ``trigger_fetch_node`` doesn't block the FastAPI event
    loop.
    """
    lock = get_flow_run_lock(flow.flow_id)
    async with lock:
        if flow.flow_settings.is_running:
            return ObservationResult(
                success=False,
                node_id=node_id,
                node_type=node_type,
                error_kind="FlowAlreadyRunning",
                error_message=(
                    f"flow {flow.flow_id} is already running (likely a "
                    "UI run or another agent observation); the agent will "
                    "retry on the next round"
                ),
            )
        try:
            flow.validate_if_node_can_be_fetched(node_id)
        except Exception as exc:
            return _failure_from_exc(node_id, node_type, exc)
        try:
            await asyncio.to_thread(flow.trigger_fetch_node, node_id)
        except Exception as exc:
            return _failure_from_exc(node_id, node_type, exc)

    # trigger_fetch_node records the error instead of raising; fail here so the planner's auto-undo fires.
    run_error = getattr(node.results, "errors", None)
    if run_error:
        return ObservationResult(
            success=False,
            node_id=node_id,
            node_type=node_type,
            error_kind="RunError",
            error_message=str(run_error),
        )

    resulting_data = getattr(node.results, "resulting_data", None)
    if resulting_data is None:
        return ObservationResult(
            success=False,
            node_id=node_id,
            node_type=node_type,
            error_kind="EmptyResult",
            error_message=(
                f"node {node_id} ({node_type}) returned no data on "
                "Performance-mode evaluation; check upstream wiring "
                "or settings"
            ),
        )

    schema = _extract_schema(node)
    sample_rows = _extract_sample_rows(node)
    return ObservationResult(
        success=True,
        node_id=node_id,
        node_type=node_type,
        output_schema=schema,
        sample_rows=sample_rows,
    )


async def _observe_development(node: Any, node_id: int, node_type: str) -> ObservationResult:
    """Development-mode observation: schema only via
    ``get_predicted_schema``. Cheaper — no kernel run beyond what
    the schema callback does internally.

    Wrapped in ``asyncio.to_thread`` because the schema-prediction
    fall-through (``flow_node._predicted_data_getter``) can still
    invoke the node's function with predicted-empty inputs, which
    for nodes like ``explore_data`` dispatches a synchronous
    ``ExternalDfFetcher`` request to the worker. Keep that off the
    event loop.

    Sample rows are best-effort: if the predicted schema produced
    a sample frame as a side effect (some flowfile node types
    cache one), include up to :data:`_MAX_SAMPLE_ROWS`. Otherwise
    return an empty list — the LLM sees "schema only" in the
    observation block.
    """
    try:
        predicted = await asyncio.to_thread(node.get_predicted_schema, force=True)
    except Exception as exc:
        return _failure_from_exc(node_id, node_type, exc)
    if predicted is None:
        return ObservationResult(
            success=False,
            node_id=node_id,
            node_type=node_type,
            error_kind="UnpredictableSchema",
            error_message=(
                f"node {node_id} ({node_type}) couldn't predict its output "
                "schema in Development mode; consider switching the flow "
                "to Performance mode for this run, or fix the node's "
                "settings to make the schema deterministic"
            ),
        )
    schema = _columns_to_dict_list(predicted)
    return ObservationResult(
        success=True,
        node_id=node_id,
        node_type=node_type,
        output_schema=schema,
        sample_rows=_dev_mode_sample_rows(node),
    )


def _extract_schema(node: Any) -> list[dict[str, Any]]:
    """Read ``node.get_predicted_schema()`` (cached after a successful
    apply) and project to the wire-friendly column-dict shape.
    """
    try:
        cols = node.get_predicted_schema()
    except Exception:
        cols = None
    return _columns_to_dict_list(cols)


def _columns_to_dict_list(cols: Any) -> list[dict[str, Any]]:
    if not cols:
        return []
    out: list[dict[str, Any]] = []
    for col in cols:
        name = getattr(col, "column_name", None) or getattr(col, "name", "?")
        dtype = getattr(col, "data_type", None) or "Unknown"
        out.append({"name": str(name), "data_type": str(dtype)})
    return out


def _extract_sample_rows(node: Any) -> list[dict[str, Any]]:
    """Up to :data:`_MAX_SAMPLE_ROWS` rows of the node's result: from the
    preview grid the fetch already sampled, else by ``sample`` on the
    backend that holds its reference, so core collects nothing of it.
    No reference or a failed read gives ``[]`` — a missing sample
    shouldn't fail the observation when the schema is fine.
    """
    ref = node.results.result_ref
    if ref is None:
        return []
    import io

    import polars as pl

    from flowfile_core.flowfile.execution import registry

    try:
        grid = getattr(node.results, "example_data_generator", None)
        if grid is not None:
            return grid().slice(0, _MAX_SAMPLE_ROWS).to_pylist()
        sample = registry.backend(ref.backend_id).sample(ref, _MAX_SAMPLE_ROWS)
        return pl.read_ipc_stream(io.BytesIO(sample)).to_dicts()[:_MAX_SAMPLE_ROWS]
    except Exception:
        logger.exception("observe_after_apply: sample-row read failed")
        return []


def _dev_mode_sample_rows(node: Any) -> list[dict[str, Any]]:
    """Best-effort sample-row collection in Development mode. Most
    node types don't expose a cheap "predicted sample" so we return
    an empty list; the LLM still sees the schema."""
    del node
    return []


def _failure_from_exc(node_id: int, node_type: str, exc: Exception) -> ObservationResult:
    msg = str(exc).strip() or repr(exc)
    # Polars traces can pass 5 KB; the first 800 characters carry the actionable line.
    if len(msg) > 800:
        msg = msg[:800] + " … [truncated]"
    return ObservationResult(
        success=False,
        node_id=node_id,
        node_type=node_type,
        error_kind=type(exc).__name__,
        error_message=msg,
    )


def format_observation_block(result: ObservationResult) -> str:
    """Render an :class:`ObservationResult` as the markdown text the
    LLM consumes in its next-round tool reply.

    Two shapes:

    * **success** — *"✓ Output schema: …; Sample: …"* with the
      column list and up to :data:`_MAX_SAMPLE_ROWS` row dicts.
    * **failure** — *"✗ <ErrorKind>: <message>"* — the planner
      auto-undoes the just-added node and increments the retry
      budget.
    """
    if not result.success:
        kind = result.error_kind or "Error"
        return (
            f"✗ Step on node {result.node_id} ({result.node_type or 'unknown'}) failed.\n"
            f"{kind}: {result.error_message}"
        )

    schema_lines = [
        f"  - {col.get('name', '?')}: {col.get('data_type', 'Unknown')}"
        for col in result.output_schema
    ]
    schema_block = "\n".join(schema_lines) if schema_lines else "  (empty)"

    parts = [
        f"✓ Step on node {result.node_id} ({result.node_type or 'unknown'}) succeeded.",
        "Output schema:",
        schema_block,
    ]
    if result.sample_rows:
        sample_lines = [
            "  " + ", ".join(f"{k}={v!r}" for k, v in row.items())
            for row in result.sample_rows
        ]
        parts.append(f"Sample ({len(result.sample_rows)} row(s)):")
        parts.extend(sample_lines)
    return "\n".join(parts)


__all__ = [
    "ObservationResult",
    "observe_after_apply",
    "format_observation_block",
]
