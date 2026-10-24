"""
Node execution logic - separate from node definition.

Handles the 'how to run' independently from 'what to run'.
Enables stateless execution by accepting external state providers.
"""

from __future__ import annotations

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
"""Node types whose result is already computed when their function returns: a sink's result is the input it
wrote, a ``run_flow`` node's the output of the child flow it ran. They are submitted never wide and without an inline
preview, and materialised only when they read an input that crossed from another backend, so otherwise no backend
computes or samples their rows during the run."""


def is_narrow_transform(node: FlowNode) -> bool:
    """Whether ``node`` maps rows without reshaping data across them, by its template's ``transform_type``.

    The node templates mark the narrow types (select, filter, formula, sample, union and the like), and a custom
    node declares its own in its manifest. A node without a template default (manual input, the readers) is
    not narrow.
    """
    return node.node_default is not None and node.node_default.transform_type == "narrow"


def hints_for(node: FlowNode, context: ExecutionContext, performance_mode: bool) -> SubmitHints:
    """The hints ``node`` submits its plan with; the backend's materialisation policy reads them.

    - ``must_materialise``: the node caches its results (``cache_results``) and is not one of
      ``IN_CORE_NODE_TYPES``; or it reads an input that crossed from another backend (``_boundary_inputs``), whatever
      its type, so no result refers to the exchange store's prefix once the run removes it.
    - ``wide``: the node is not a narrow transform (``is_narrow_transform``) and not one of ``IN_CORE_NODE_TYPES``.
    - ``fanout``: how many distinct downstream nodes read the result. No backend's policy reads it yet.
    - ``preview_rows``: the context's ``preview_rows``, or ``DEFAULT_PREVIEW_ROWS`` when the context keeps none
      but the node still needs a preview. ``None`` in ``performance_mode``, except for a ``cache_results`` node,
      which keeps its example data in every mode, and ``None`` for ``IN_CORE_NODE_TYPES``.

    Args:
        node: The node about to submit.
        context: The run's execution context.
        performance_mode: The run's Performance mode flag, as the executor received it.
    """
    in_core = node.node_type in IN_CORE_NODE_TYPES
    caches = node.node_settings.cache_results
    preview_rows = None
    if not in_core and (caches or not performance_mode):
        preview_rows = context.preview_rows if context.preview_rows is not None else DEFAULT_PREVIEW_ROWS
    return SubmitHints(
        must_materialise=(caches and not in_core) or bool(node._boundary_inputs),
        wide=not in_core and not is_narrow_transform(node),
        fanout=len({consumer.node_id for consumer in node.leads_to_nodes}),
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
    ) -> None:
        """
        Main execution entry point.

        Called by FlowNode.execute_node(). Uses _decide_execution() as the
        single source of truth for whether the node should run; when it runs,
        the node submits its plan with ``hints_for`` to the backend of the
        current execution context, whose policy decides how it runs.

        When the decision cannot be made because asking the backend about a cached
        result failed (any ``ComputeError``: ``ComputeUnavailable`` for a worker that
        cannot be reached, ``VersionMismatch`` for one running other versions), the
        node fails through the same path as a failed run: its previous results are
        cleared and ``_handle_error`` records the typed message. The node is not run,
        neither on the worker nor in core, and the next run that executes it clears
        the error again.

        A node that fails keeps the exception it failed with (``_handle_error``), and
        until the next run clears it every read of the node (``get_resulting_data``)
        raises that exception, never the frame the run built before it failed.

        Args:
            reset_cache: Force cache invalidation
            performance_mode: Skip example data generation for speed
            retry: Allow retry on recoverable errors
            node_logger: Logger for this node's execution
            unreachable: The error the handshake of the node's placed backend raised: the node fails with it through
                the same path, nothing is decided or sent.
        """
        if node_logger is None:
            raise ValueError("node_logger is required")

        state = self.state_provider.get_state(self.node.node_id, self.node.parent_uuid)

        if reset_cache:
            self._clear_cache(state)

        if unreachable is not None:
            self._prepare_for_execution(state)
            self._handle_error(state, unreachable, performance_mode, False, node_logger)
            return

        try:
            decision = self._decide_execution(state, performance_mode, reset_cache)
        except ComputeError as e:
            self._prepare_for_execution(state)
            self._handle_error(state, e, performance_mode, retry, node_logger)
            return
        if not decision.should_run:
            return

        context = current_context()
        hints = hints_for(self.node, context, performance_mode)
        reason_str = decision.reason.name if decision.reason else "UNKNOWN"
        node_logger.info(f"Starting to run {self.node.__name__} ({reason_str} -> {context.backend_id}, {hints})")

        # A cache_results node keeps its example data even in Performance mode.
        effective_performance_mode = performance_mode and not self.node.node_settings.cache_results

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
            self.node._do_execute(hints, effective_performance_mode, node_logger)
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
                # Cancelled mid-run (e.g. a cancel_check-aborted DB read): record a clean
                # cancellation instead of a failure, and skip the error/retry path.
                node_logger.info("Node execution cancelled")
                state.reset_results_only()
                self._sync_state_to_legacy(state)
                return
            self._handle_error(state, e, effective_performance_mode, retry, node_logger)

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

        A ``cache_results`` node skips while the run's backend still holds the rows its
        last run stored: the decision asks ``exists`` of the stored result the node holds
        under its current hash (``FlowNode.held_result``). A node holding no result for its
        current hash on the run's backend has no cache. A result held as a deferred plan is
        no cache either, so a cached node holding one follows the ordinary rules below: in
        Development mode it is skipped after a successful run, and Performance mode reruns it.

        A cached node whose last run failed reruns even when the backend still holds its
        result: skipping it would keep the failed run's error on the node, while running it
        clears that error and ``FlowNode._do_execute`` reads the stored result back rather
        than recomputing it.

        Whether the held result vanished is asked only of a node that has run with its current setup, the one the
        "already ran" rule would skip, so a node reset by a move to another backend never asks the backend it left.

        Raises:
            ComputeError: Asking the backend whether the stored result still exists
                failed (``ComputeUnavailable`` when the worker cannot be reached). ``execute``
                records this as a node failure; it is never read as a missing cache.
        """
        if self.node.node_template.node_group == "output":
            return ExecutionDecision(True, InvalidationReason.OUTPUT_NODE)

        if self.node.node_type == "run_flow":
            # The referenced flow file can change without any parent-settings
            # change, so a subflow node is never considered up to date.
            return ExecutionDecision(True, InvalidationReason.OUTPUT_NODE)

        if self.node.node_type == "gate" and self._gate_routes_on_formula():
            # The formula's source data can change without any hash changing,
            # so the gate must re-evaluate its routing decision every run.
            return ExecutionDecision(True, InvalidationReason.OUTPUT_NODE)

        if force_refresh:
            return ExecutionDecision(True, InvalidationReason.FORCED_REFRESH)

        # Changed source data outranks every cache: the node hash does not rotate when a file
        # on disk changes, so a cache hit here would serve data the user just replaced.
        if self._source_file_changed(state):
            return ExecutionDecision(True, InvalidationReason.SOURCE_FILE_CHANGED)

        # A vanished result outranks the "already ran" skip below: skipping would hand downstream a dangling plan.
        if state.has_run_with_current_setup and self.node.results.artifact_is_missing():
            return ExecutionDecision(True, InvalidationReason.CACHE_MISSING)

        # Cache-enabled nodes: check the stored result is still held.
        # This must come before performance_mode so cached results are preserved
        # even when upstream nodes produce no new data.
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

        # Already ran with current settings → skip
        # Results are available in memory from previous execution
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

    def _clear_cache(self, state: NodeExecutionState) -> None:
        """Clear cached results; when that released operation outputs, the nodes downstream drop theirs too."""
        minted_outputs = bool(self.node._operation_refs)
        self.node.remove_cache()
        if minted_outputs:
            self.node.drop_downstream_results()
        state.has_run_with_current_setup = False
        state.has_completed_last_run = False

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
    ) -> None:
        """Handle execution errors with retry logic.

        A missing-file error from Polars reruns the node's inputs and then the node once, when ``retry`` is set. With
        ``settings.FLOWFILE_COMPUTE_PROTOCOL`` off the message alone counts as an input's file gone, as the results are
        core's own files. With it on an input must also hold a result that is gone (``NodeResults.artifact_is_missing``,
        which asks the backend that minted it); otherwise the error is the node's own and it fails with it, the inputs'
        results kept. With it on an input holding a result for its current setup (``FlowNode.held_result``) reruns on
        that result's backend, and an input the node reads from another backend crosses through its edge again when the
        node retries; any other input reruns where the node runs, which composes it into its plan.
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
                        )
                except ValueError as e:
                    node_logger.error(f"Could not retry upstream node {node_input.node_id}: {e}")
            # Retry this node once (no further retries)
            self.execute(
                performance_mode=performance_mode,
                retry=False,
                node_logger=node_logger,
            )
            return

        node_logger.error(f"Error running node: {error}")
