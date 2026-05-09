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
"""Cap on the number of sample rows surfaced in the observation
block. The LLM doesn't need a full dataset preview — three rows is
enough to confirm shape and spot-check values without flooding the
prompt. Performance-bounded too: ``collect(n_records=3)`` short-
circuits at three on streamable plans."""


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
    """Each entry: ``{"name": str, "data_type": str}``. Same shape
    as the predictor's persisted predicted_output_schema and the
    staged_node_payload's predicted_schema, so the LLM sees a
    consistent column-shape across surfaces."""
    sample_rows: list[dict[str, Any]] = field(default_factory=list)
    """At most :data:`_MAX_SAMPLE_ROWS` representative rows. Empty
    on failure or when the executor couldn't materialise data
    (e.g. dynamic node + Development mode → schema-only)."""
    error_message: str = ""
    """Cleaned-up runtime error text (Pydantic / Polars exception
    messages, truncated) the LLM consumes on its next round. Empty
    on success."""
    error_kind: str = ""
    """Short category label for the error, e.g.
    ``"ColumnNotFoundError"`` / ``"TypeError"`` / ``"OverflowError"``,
    extracted from the exception type. Empty on success. Used for
    audit / log filtering — the LLM sees the full
    ``error_message`` in the prompt."""


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
        # Settings access shouldn't fail, but if it does we treat
        # the run as Development (cheap path) — never block on a
        # missing settings field.
        pass

    # ``explore_data`` is the user-facing data inspector; the chat
    # trail's *"You can view the data in the explore_data panel to
    # inspect the results"* announcement is meaningless if the panel
    # is empty. Force the Performance-mode path (real
    # ``trigger_fetch_node``) for this node type regardless of the
    # flow's configured execution_mode so the panel always has data
    # by the time the LLM advertises it.
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

    # ``trigger_fetch_node`` writes the runtime error (if any) into
    # ``node.results.errors`` rather than re-raising. Surface it as
    # a failure observation so the planner's auto-undo path fires.
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
    sample_rows = _extract_sample_rows(resulting_data)
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


def _extract_sample_rows(resulting_data: Any) -> list[dict[str, Any]]:
    """Collect up to :data:`_MAX_SAMPLE_ROWS` from the resulting
    data engine. Catches collection errors and returns ``[]`` —
    a missing sample shouldn't fail the whole observation when the
    schema is fine.
    """
    try:
        df = resulting_data.collect(n_records=_MAX_SAMPLE_ROWS)
    except Exception:
        logger.exception("observe_after_apply: sample-row collect failed")
        return []
    try:
        return df.to_dicts()[:_MAX_SAMPLE_ROWS]
    except Exception:
        # Polars ``to_dicts`` should always work on a real DataFrame,
        # but if it doesn't the caller still has the schema + the
        # success flag.
        return []


def _dev_mode_sample_rows(node: Any) -> list[dict[str, Any]]:
    """Best-effort sample-row collection in Development mode. Most
    node types don't expose a cheap "predicted sample" so we return
    an empty list; the LLM still sees the schema."""
    # Hook for future enhancement: some upstream nodes carry a
    # predicted-sample frame on their schema_callback. We currently
    # skip — schema-only observation is enough for the LLM to decide
    # what to do next without an actual collect.
    del node
    return []


def _failure_from_exc(node_id: int, node_type: str, exc: Exception) -> ObservationResult:
    msg = str(exc).strip() or repr(exc)
    # Truncate very long messages so they don't blow the LLM
    # context. Polars stack traces can be 5+ KB; the first 800
    # chars almost always carry the actionable error line.
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
