"""
Node execution logic - separate from node definition.

Handles the 'how to run' independently from 'what to run'.
Enables stateless execution by accepting external state providers.
"""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import replace
from typing import TYPE_CHECKING, Protocol

from flowfile_core.configs import settings
from flowfile_core.flowfile.execution.context import (
    DEFAULT_PREVIEW_ROWS,
    ExecutionContext,
    current_context,
    use_context,
)
from flowfile_core.flowfile.execution.errors import ComputeError
from flowfile_core.flowfile.execution.protocol import SubmitHints
from flowfile_core.flowfile.flow_node.models import (
    ExecutionDecision,
    InvalidationReason,
    InvalidExpressionError,
)
from flowfile_core.flowfile.flow_node.state import NodeExecutionState, SourceFileInfo
from flowfile_core.flowfile.parameter_resolver import node_parameters_resolved
from shared.path_utils import is_url

if TYPE_CHECKING:
    from flowfile_core.configs.flow_logger import NodeLogger
    from flowfile_core.flowfile.flow_node.flow_node import FlowNode

IN_CORE_NODE_TYPES = frozenset({"output", "api_response", "flow_output", "run_flow"})
"""Node types whose result is computed when their function returns: a sink's is the input it wrote, a ``run_flow``
node's the child flow's output. They are never wide, ask no preview, and are materialised only for a boundary input,
so otherwise no backend computes or samples their rows during the run."""


def is_narrow_transform(node: FlowNode) -> bool:
    """Whether ``node`` maps rows without reshaping data across them, by its template's ``transform_type``.

    A custom node declares its own in its manifest. A node without a template default (manual input, the readers)
    is not narrow.
    """
    return node.node_default is not None and node.node_default.transform_type == "narrow"


def hints_for(
    node: FlowNode,
    context: ExecutionContext,
    performance_mode: bool,
    running: Collection[int | str] | None = None,
) -> SubmitHints:
    """The hints ``node`` submits its plan with, which the backend's materialisation policy reads.

    - ``must_materialise``: a ``cache_results`` node outside ``IN_CORE_NODE_TYPES``, or any node reading an input
      that crossed from another backend (``_boundary_inputs``), so no result refers to the exchange prefix once the
      run removes it.
    - ``wide``: neither ``is_narrow_transform`` nor in ``IN_CORE_NODE_TYPES``.
    - ``fanout``: the number of distinct downstream nodes among *running*, the ids of the nodes this run executes,
      or of every downstream node when *running* is ``None``. The worker stores a wide result read by more than one,
      which would otherwise be computed again inside each consumer's plan.
    - ``preview_rows``: the context's, else ``DEFAULT_PREVIEW_ROWS``. ``None`` for ``IN_CORE_NODE_TYPES`` and in
      *performance_mode*, except for a ``cache_results`` node.
    """
    in_core = node.node_type in IN_CORE_NODE_TYPES
    caches = node.node_settings.cache_results
    preview_rows = None
    if not in_core and (caches or not performance_mode):
        preview_rows = context.preview_rows if context.preview_rows is not None else DEFAULT_PREVIEW_ROWS
    consumers = {consumer.node_id for consumer in node.leads_to_nodes}
    if running is not None:
        consumers = {consumer for consumer in consumers if consumer in running}
    return SubmitHints(
        must_materialise=(caches and not in_core) or bool(node._boundary_inputs),
        wide=not in_core and not is_narrow_transform(node),
        fanout=len(consumers),
        preview_rows=preview_rows,
    )


class StateProvider(Protocol):
    """
    Protocol for providing/storing node state.

    Implement this to store state externally (Redis, DB, etc.)
    for stateless worker support.
    """

    def get_state(self, node_id: str | int, flow_id: str) -> NodeExecutionState:
        """Retrieve state for a node."""
        ...

    def save_state(self, node_id: str | int, flow_id: str, state: NodeExecutionState) -> None:
        """Persist state for a node."""
        ...


class InMemoryStateProvider:
    """
    Default state provider: state lives in FlowNode._execution_state.

    This maintains current behavior where state is kept in memory
    on the node instance itself.
    """

    def __init__(self, node: FlowNode):
        self._node = node

    def get_state(self, node_id: str | int, flow_id: str) -> NodeExecutionState:
        return self._node._execution_state

    def save_state(self, node_id: str | int, flow_id: str, state: NodeExecutionState) -> None:
        # For in-memory, state is already on the node - nothing to do
        pass


class NodeExecutor:
    """
    Handles node execution logic.

    Separated from FlowNode to allow:
    - Stateless execution (state from external source)
    - Easier testing
    - Clear separation of concerns

    Performance note: This class is designed to be reused via lazy
    instantiation on FlowNode._executor to avoid repeated object creation.
    """

    __slots__ = ("node", "state_provider")

    def __init__(
        self,
        node: FlowNode,
        state_provider: StateProvider | None = None,
    ):
        self.node = node
        self.state_provider = state_provider or InMemoryStateProvider(node)

    def execute(
        self,
        reset_cache: bool = False,
        performance_mode: bool = False,
        retry: bool = True,
        node_logger: NodeLogger = None,
        unreachable: Exception | None = None,
        running: Collection[int | str] | None = None,
        preview: bool | None = None,
    ) -> None:
        """
        Main execution entry point.

        Called by FlowNode.execute_node(). ``_decide_execution()`` is the single
        source of truth for whether the node runs. When it runs, the node submits
        its plan with ``hints_for`` to the current context's backend, whose policy
        decides how.

        A ``ComputeError`` while deciding (asking a backend about a cached result)
        fails the node through the normal error path, its previous results cleared.
        The node is not run, on the worker or in core. A failed node keeps its
        exception, and every read of it (``get_resulting_data``) raises it until the
        next run clears it.

        Args:
            reset_cache: Force cache invalidation
            performance_mode: Skip example data generation for speed
            retry: Allow retry on recoverable errors
            node_logger: Logger for this node's execution
            unreachable: The error the handshake of the node's placed backend raised. The node fails with it and
                nothing is decided or sent.
            running: The ids of the nodes this run executes, which ``hints_for`` counts the consumers among;
                ``None`` counts every consumer.
            preview: Fill the 100-row grid even when *performance_mode* sends no ``preview_rows``, by a ``sample``
                taken before the node is reported run, so a failure on the rows fails the node; ``None`` means
                ``not performance_mode``, the grid then sampled on first read. A ``cache_results`` node always fills
                it.

        A ``reset_cache`` that released a stored result the node does not store again (a Performance-mode fetch of
        a node an earlier run stored) drops the results of the nodes below, whose plans scanned its file.
        """
        if node_logger is None:
            raise ValueError("node_logger is required")

        state = self.state_provider.get_state(self.node.node_id, self.node.parent_uuid)

        released_stored = reset_cache and self._clear_cache(state)
        try:
            self._run(state, reset_cache, performance_mode, retry, node_logger, unreachable, running, preview)
        finally:
            if released_stored and not self._holds_stored_result():
                self.node.drop_downstream_results()

    def _run(
        self,
        state: NodeExecutionState,
        reset_cache: bool,
        performance_mode: bool,
        retry: bool,
        node_logger: NodeLogger,
        unreachable: Exception | None,
        running: Collection[int | str] | None,
        preview: bool | None,
    ) -> None:
        if unreachable is not None:
            self._prepare_for_execution(state)
            self._handle_error(state, unreachable, performance_mode, False, node_logger, running)
            return

        try:
            decision = self._decide_execution(state, performance_mode, reset_cache)
        except ComputeError as e:
            self._prepare_for_execution(state)
            self._handle_error(state, e, performance_mode, retry, node_logger, running)
            return
        if not decision.should_run:
            return

        context = current_context()
        hints = hints_for(self.node, context, performance_mode, running)
        reason_str = decision.reason.name if decision.reason else "UNKNOWN"
        node_logger.info(f"Starting to run {self.node.__name__} ({reason_str} -> {context.backend_id}, {hints})")

        # A cache_results node keeps its example data even in Performance mode.
        effective_performance_mode = performance_mode and not self.node.node_settings.cache_results
        grid = (preview if preview is not None else not performance_mode) or not effective_performance_mode

        if decision.reason == InvalidationReason.SOURCE_FILE_CHANGED:
            # A backend keys results by node hash, which a source change does not rotate.
            self._clear_cache(state)

        self._prepare_for_execution(state)
        self.node.reset()
        self._release_deferred_seed()
        self.node._last_exception_class = None  # a previous run's class must never describe this one

        # Snapshot before the scan so files arriving mid-run are not silently marked as seen.
        pending_source_info = self._pending_source_snapshot()

        try:
            self._raise_on_invalid_expression()
            self.node._do_execute(hints, grid, node_logger)
            if preview:
                self.node.read_example_data()
            if self.node.results.resulting_data is not None:
                state.result_schema = self.node.results.resulting_data.schema
            if self.node.results.errors is None and not self.node.node_stats.is_canceled:
                state.mark_successful()
            if pending_source_info is not None:
                state.source_file_info = pending_source_info
            self._sync_state_to_legacy(state)
            self.state_provider.save_state(self.node.node_id, self.node.parent_uuid, state)
        except Exception as e:
            if state.is_canceled:
                # Cancelled mid-run: record a clean cancellation, not a failure, and skip the retry path.
                node_logger.info("Node execution cancelled")
                state.reset_results_only()
                self._sync_state_to_legacy(state)
                return
            self._handle_error(state, e, effective_performance_mode, retry, node_logger, running, preview)

    def _raise_on_invalid_expression(self) -> None:
        """Fail a formula/advanced-filter node whose expression cannot run against its input.

        The check is data-free (a zero-row collect). Running it here, before the node builds
        the lazy plan, makes a leaf node's bad expression surface at all and attributes it to this
        node instead of whichever downstream sink would otherwise be the first to collect it.

        A formula node's own ``collect_schema`` step does not type-check the temporal/string
        namespaces, so the chain check is what catches those — it is not redundant with it.
        """
        from flowfile_core.flowfile.settings_validation import node_expression_issue, node_formula_chain_issue

        if self.node.node_type == "formula":
            message = node_formula_chain_issue(self.node, allow_prediction=False)
            if message is not None:
                raise InvalidExpressionError(message)
            return
        issue = node_expression_issue(self.node, allow_prediction=False)
        if issue is not None and issue.kind in ("type", "parse"):
            raise InvalidExpressionError(issue.message)

    def _decide_execution(
        self,
        state: NodeExecutionState,
        performance_mode: bool,
        force_refresh: bool,
    ) -> ExecutionDecision:
        """
        Single source of truth for whether a node runs.

        A ``cache_results`` node skips while the run's backend still ``exists`` the stored
        result the node holds under its current hash (``FlowNode.held_result``). A node
        holding none has no cache. A deferred plan is no cache either, so such a node
        follows the ordinary rules below. A cached node whose last run failed reruns
        anyway: that clears the error, and ``FlowNode._do_execute`` reads the stored
        result back.

        Only a node that has run with its current setup is asked whether its result
        vanished, so a node reset by a move to another backend never asks the one it left.

        Raises:
            ComputeError: Asking the backend whether the stored result exists failed.
                ``execute`` records it as a node failure, never as a missing cache.
        """
        if self.node.node_template.node_group == "output":
            return ExecutionDecision(True, InvalidationReason.OUTPUT_NODE)

        if self.node.node_type == "run_flow":
            # The referenced flow file can change without any settings change, so it always reruns.
            return ExecutionDecision(True, InvalidationReason.OUTPUT_NODE)

        if self.node.node_type == "gate" and self._gate_routes_on_formula():
            # The formula's data can change without any hash changing, so the gate re-decides every run.
            return ExecutionDecision(True, InvalidationReason.OUTPUT_NODE)

        if force_refresh:
            return ExecutionDecision(True, InvalidationReason.FORCED_REFRESH)

        # Changed source data outranks every cache: the node hash does not rotate when a file changes.
        if self._source_file_changed(state):
            return ExecutionDecision(True, InvalidationReason.SOURCE_FILE_CHANGED)

        # A vanished result outranks the "already ran" skip: skipping would hand downstream a dangling plan.
        if state.has_run_with_current_setup and self.node.results.artifact_is_missing():
            return ExecutionDecision(True, InvalidationReason.CACHE_MISSING)

        # Before performance_mode, so a cached result is kept even when upstream produces no new data.
        if self.node.node_settings.cache_results:
            backend = current_context().backend
            held = self.node.held_result(backend.id)
            if held is None or held.state == "materialised":
                if state.error is None and held is not None and backend.exists(held):
                    return ExecutionDecision(False, None)
                return ExecutionDecision(True, InvalidationReason.CACHE_MISSING)

        if performance_mode:
            return ExecutionDecision(True, InvalidationReason.PERFORMANCE_MODE)

        if not state.has_run_with_current_setup:
            return ExecutionDecision(True, InvalidationReason.NEVER_RAN)

        return ExecutionDecision(False, None)

    def _gate_routes_on_formula(self) -> bool:
        gate_input = getattr(self.node.setting_input, "gate_input", None)
        return gate_input is not None and getattr(gate_input, "condition_source", "parameter") == "formula"

    def _source_file_changed(self, state: NodeExecutionState) -> bool:
        """
        Check if source file has changed since last successful run.

        Only applicable to read nodes. Returns False for other node types.
        """
        if self.node.node_type != "read":
            return False

        path = self._get_source_path()
        if not path:
            return False

        # First time - no previous info to compare
        if state.source_file_info is None:
            return False

        return state.source_file_info.has_changed()

    def _get_source_path(self) -> str | None:
        """Get the source file path for read nodes."""
        setting_input = self.node.setting_input
        if not hasattr(setting_input, "received_file") or not setting_input.received_file:
            return None

        rf = setting_input.received_file
        if hasattr(rf, "abs_file_path") and rf.abs_file_path:
            return rf.abs_file_path
        return rf.path if hasattr(rf, "path") else None

    def _pending_source_snapshot(self) -> SourceFileInfo | None:
        """Snapshot the source *before* the scan, to be committed only if the run succeeds.

        Taken up front so a file written while the node is reading counts as a later change
        instead of being folded into this run's fingerprint. Returns None for non-read nodes
        and for sources that cannot be stat'ed, in which case any existing snapshot is kept.
        """
        if self.node.node_type != "read":
            return None

        received_file = getattr(self.node.setting_input, "received_file", None)
        if received_file is not None and not is_url(getattr(received_file, "path", None)):
            # Idempotent: recomputes the pattern from the ${param}-substituted path.
            received_file.set_absolute_filepath()

        return self._snapshot_source(self._get_source_path())

    def _snapshot_source(self, path: str | None) -> SourceFileInfo | None:
        """Fingerprint *path* for change detection: an aggregate in directory mode, a single stat otherwise."""
        if self.node.node_type != "read" or not path:
            return None

        received_file = getattr(self.node.setting_input, "received_file", None)
        if received_file is not None and getattr(received_file, "scan_mode", "single_file") == "directory":
            return SourceFileInfo.from_pattern(path)
        return SourceFileInfo.from_path(path)

    def _prepare_for_execution(self, state: NodeExecutionState) -> None:
        """Prepare node state before execution."""
        self.node.clear_table_example()
        state.reset_results_only()
        state.is_canceled = False  # clear any stale cancel flag from a prior run
        self.node.node_stats.is_canceled = False
        self.node.results.errors = None
        self.node._last_run_error = None
        self.node.results.resulting_data = None
        self.node.results.example_data = None

    def _release_deferred_seed(self) -> None:
        """Let this run execute a seeded node; called after ``reset()``, which re-arms a node placed deferred."""
        if self.node.deferred_until_run:
            self.node.deferred_until_run = False
            # the seed's per-handle schemas would outlive the run: multi-output refills skip them
            self.node._named_schemas = {}

    def _clear_cache(self, state: NodeExecutionState) -> bool:
        """Clear cached results; when that released operation outputs, the nodes downstream drop theirs too.

        Answers whether a stored result was released: its file is gone until the node stores again under its hash.
        """
        minted_outputs = bool(self.node._operation_refs)
        released_stored = self._holds_stored_result()
        self.node.remove_cache()
        if minted_outputs:
            self.node.drop_downstream_results()
        state.has_run_with_current_setup = False
        state.has_completed_last_run = False
        return released_stored

    def _holds_stored_result(self) -> bool:
        ref = self.node.results.result_ref
        return ref is not None and ref.state == "materialised"

    def _sync_state_to_legacy(self, state: NodeExecutionState) -> None:
        """Sync _execution_state to legacy node_stats for backwards compatibility."""
        self.node.node_stats._has_run_with_current_setup = state.has_run_with_current_setup
        self.node.node_stats.has_completed_last_run = state.has_completed_last_run
        self.node.node_stats.is_canceled = state.is_canceled
        self.node.node_stats.error = state.error

    def _handle_error(
        self,
        state: NodeExecutionState,
        error: Exception,
        performance_mode: bool,
        retry: bool,
        node_logger: NodeLogger,
        running: Collection[int | str] | None = None,
        preview: bool | None = None,
    ) -> None:
        """Handle execution errors with retry logic.

        With ``retry`` set, a Polars missing-file error reruns the node's inputs, then the node once with *preview*.
        With ``settings.FLOWFILE_COMPUTE_PROTOCOL`` off the message alone is enough, the results being core's own
        files. With it on, an input must also hold a result that is gone (``NodeResults.artifact_is_missing``).
        Otherwise the error is the node's own and the inputs' results are kept. An input reruns on the backend that
        holds its result for its current setup (``FlowNode.held_result``), else where the node runs.
        """
        error_str = str(error)
        state.mark_failed(error_str)
        self._sync_state_to_legacy(state)
        self.node.results.errors = error_str
        # Class name only, never the message; a worker wrapper carries the original class.
        self.node._last_exception_class = getattr(error, "original_class", None) or type(error).__name__
        self.node._last_run_error = error

        # Retry on missing file errors (upstream cache was cleared)
        if (
            "(os error 2)" in error_str
            and retry
            and (
                not settings.FLOWFILE_COMPUTE_PROTOCOL
                or any(
                    node_input.results.artifact_is_missing() for node_input in self.node.node_inputs.get_all_inputs()
                )
            )
        ):
            node_logger.warning("Input file missing, retrying upstream nodes...")
            for node_input in self.node.node_inputs.get_all_inputs():
                context = current_context()
                ref = node_input.results.result_ref
                held = node_input.held_result(ref.backend_id) if ref is not None else None
                if settings.FLOWFILE_COMPUTE_PROTOCOL and held is not None and held.backend_id != context.backend_id:
                    context = replace(context, backend_id=held.backend_id)
                # The run loop substitutes only into the node it is about to run.
                try:
                    with use_context(context), node_parameters_resolved(node_input):
                        node_input.execute_node(
                            performance_mode=performance_mode,
                            retry=True,
                            reset_cache=True,
                            node_logger=node_logger,
                            running=running,
                        )
                except ValueError as e:
                    node_logger.error(f"Could not retry upstream node {node_input.node_id}: {e}")
            # Retry this node once (no further retries)
            self.execute(
                performance_mode=performance_mode,
                retry=False,
                node_logger=node_logger,
                running=running,
                preview=preview,
            )
            return

        node_logger.error(f"Error running node: {error}")
