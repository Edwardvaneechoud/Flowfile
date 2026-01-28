"""
Node execution logic - separate from node definition.

Handles the 'how to run' independently from 'what to run'.
Enables stateless execution by accepting external state providers.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from flowfile_core.flowfile.flow_data_engine.subprocess_operations import (
    results_exists,
)
from flowfile_core.flowfile.flow_node.models import (
    ExecutionDecision,
    ExecutionStrategy,
    InvalidationReason,
)
from flowfile_core.flowfile.flow_node.state import NodeExecutionState, SourceFileInfo
from flowfile_core.schemas import schemas

if TYPE_CHECKING:
    from flowfile_core.configs.flow_logger import NodeLogger
    from flowfile_core.flowfile.flow_node.flow_node import FlowNode


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
    - Different execution strategies
    - Easier testing
    - Clear separation of concerns

    Performance note: This class is designed to be reused via lazy
    instantiation on FlowNode._executor to avoid repeated object creation.
    """

    __slots__ = ('node', 'state_provider')

    def __init__(
        self,
        node: FlowNode,
        state_provider: StateProvider | None = None,
    ):
        self.node = node
        self.state_provider = state_provider or InMemoryStateProvider(node)

    def execute(
        self,
        run_location: schemas.ExecutionLocationsLiteral,
        reset_cache: bool = False,
        performance_mode: bool = False,
        retry: bool = True,
        node_logger: NodeLogger = None,
        optimize_for_downstream: bool = True,
    ) -> None:
        """
        Main execution entry point.

        This method is called when the fast-path in FlowNode.execute_node()
        determines that full execution logic is needed.

        Args:
            run_location: Where to execute ('local' or 'remote')
            reset_cache: Force cache invalidation
            performance_mode: Skip example data generation for speed
            retry: Allow retry on recoverable errors
            node_logger: Logger for this node's execution
            optimize_for_downstream: Cache wide transforms for downstream nodes
        """
        if node_logger is None:
            raise ValueError("node_logger is required")

        state = self.state_provider.get_state(self.node.node_id, self.node.parent_uuid)

        # Handle explicit cache reset
        if reset_cache:
            self._clear_cache(state)

        # Decide execution strategy
        decision = self._decide_execution(state, run_location, performance_mode, reset_cache)

        # Override for wide transforms when optimizing for downstream
        if (decision.should_run
            and decision.strategy == ExecutionStrategy.LOCAL_WITH_SAMPLING
            and self.node.node_default
            and self.node.node_default.transform_type == "wide"
            and optimize_for_downstream
            and run_location != "local"):
            decision = ExecutionDecision(True, ExecutionStrategy.REMOTE, decision.reason)

        if not decision.should_run:
            return

        reason_str = decision.reason.name if decision.reason else "UNKNOWN"
        strategy_str = decision.strategy.name
        node_logger.info(f"Starting to run {self.node.__name__} ({reason_str} -> {strategy_str})")

        # Override performance_mode when cache_results is enabled
        # This ensures example data is generated even in Performance mode
        effective_performance_mode = performance_mode
        if self.node.node_settings.cache_results:
            effective_performance_mode = False

        # Prepare and execute
        self._prepare_for_execution(state)
        self.node.reset()

        try:
            self._execute_with_strategy(state, decision.strategy, effective_performance_mode, node_logger)
            self._update_source_file_info(state)
            self._sync_state_to_legacy(state)
            self.state_provider.save_state(self.node.node_id, self.node.parent_uuid, state)
        except Exception as e:
            self._handle_error(state, e, run_location, effective_performance_mode, retry, node_logger)

    def _decide_execution(
        self,
        state: NodeExecutionState,
        run_location: schemas.ExecutionLocationsLiteral,
        performance_mode: bool,
        force_refresh: bool,
    ) -> ExecutionDecision:
        """
        Single source of truth for execution decisions.

        Determines both WHETHER to run and HOW to run in one place.
        """
        # Output nodes always run
        if self.node.node_template.node_group == "output":
            strategy = self._determine_strategy(run_location)
            return ExecutionDecision(True, strategy, InvalidationReason.OUTPUT_NODE)

        # Forced refresh (reset_cache=True)
        if force_refresh:
            strategy = self._determine_strategy(run_location)
            return ExecutionDecision(True, strategy, InvalidationReason.FORCED_REFRESH)

        # Never ran before
        if not state.has_run_with_current_setup:
            strategy = self._determine_strategy(run_location)
            return ExecutionDecision(True, strategy, InvalidationReason.NEVER_RAN)

        # Check if source file changed (for read nodes)
        if self._source_file_changed(state):
            strategy = self._determine_strategy(run_location)
            return ExecutionDecision(True, strategy, InvalidationReason.SOURCE_FILE_CHANGED)

        # Cache-enabled nodes: check if cache file is still present
        if self.node.node_settings.cache_results:
            if results_exists(self.node.hash):
                return ExecutionDecision(False, ExecutionStrategy.SKIP, None)
            strategy = self._determine_strategy(run_location)
            return ExecutionDecision(True, strategy, InvalidationReason.CACHE_MISSING)

        # Already ran with current settings → skip
        # Results are available in memory from previous execution
        return ExecutionDecision(False, ExecutionStrategy.SKIP, None)

    def _determine_strategy(
        self,
        run_location: schemas.ExecutionLocationsLiteral,
    ) -> ExecutionStrategy:
        """Determine the execution strategy based on location and node settings.

        Decision logic:
        - local → FULL_LOCAL
        - remote + cache_results → REMOTE (caching needs full materialization)
        - remote + narrow transform → LOCAL_WITH_SAMPLING (fast local compute + external sampler)
        - remote → REMOTE

        Narrow transforms (e.g., select, sample, union) only operate on columns
        without reshaping data, so they're cheap to compute locally. An external
        sampler provides preview data for the UI.

        When cache_results is enabled, the node must run fully remote so the
        result can be materialized and stored in the cache.
        """
        # Local execution mode (e.g., WASM, no worker available)
        if run_location == "local":
            return ExecutionStrategy.FULL_LOCAL

        # Caching requires full remote execution to materialize and store results
        if self.node.node_settings.cache_results:
            return ExecutionStrategy.REMOTE

        # Narrow transforms are lightweight column-level operations that can
        # run locally with an external sampler for preview data
        if (self.node.node_default is not None
                and self.node.node_default.transform_type == "narrow"):
            return ExecutionStrategy.LOCAL_WITH_SAMPLING

        # Full remote execution for wide transforms and everything else
        return ExecutionStrategy.REMOTE

    def _execute_with_strategy(
        self,
        state: NodeExecutionState,
        strategy: ExecutionStrategy,
        performance_mode: bool,
        node_logger: NodeLogger,
    ) -> None:
        """Execute using the determined strategy."""
        match strategy:
            case ExecutionStrategy.SKIP:
                return
            case ExecutionStrategy.FULL_LOCAL:
                self._do_full_local(state, performance_mode)
            case ExecutionStrategy.LOCAL_WITH_SAMPLING:
                self._do_local_with_sampling(state, performance_mode, node_logger.flow_id)
            case ExecutionStrategy.REMOTE:
                self._do_remote(state, performance_mode, node_logger)

    def _do_full_local(self, state: NodeExecutionState, performance_mode: bool) -> None:
        """
        100% in-process execution.

        Used for WASM environments or when no external worker is available.
        """
        self.node._do_execute_full_local(performance_mode)
        if not performance_mode:
            state.mark_successful()
            if self.node.results.resulting_data is not None:
                state.result_schema = self.node.results.resulting_data.schema

    def _do_local_with_sampling(
        self,
        state: NodeExecutionState,
        performance_mode: bool,
        flow_id: int
    ) -> None:
        """
        In-process execution with external sampler for preview data.

        The main computation runs locally, but sample data is generated
        via an external process for the UI preview.
        """
        self.node._do_execute_local_with_sampling(performance_mode, flow_id)
        if self.node.results.resulting_data is not None:
            state.result_schema = self.node.results.resulting_data.schema
        if self.node.results.errors is None and not self.node.node_stats.is_canceled:
            state.mark_successful()

    def _do_remote(
        self,
        state: NodeExecutionState,
        performance_mode: bool,
        node_logger: NodeLogger
    ) -> None:
        """
        Full remote worker execution.

        Computation is offloaded to an external worker process.
        """
        self.node._do_execute_remote(performance_mode, node_logger)
        if self.node.results.resulting_data is not None:
            state.result_schema = self.node.results.resulting_data.schema
            state.mark_successful()

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
        if not hasattr(setting_input, 'received_file') or not setting_input.received_file:
            return None

        rf = setting_input.received_file
        # Prefer absolute path if available
        if hasattr(rf, 'abs_file_path') and rf.abs_file_path:
            return rf.abs_file_path
        return rf.path if hasattr(rf, 'path') else None

    def _update_source_file_info(self, state: NodeExecutionState) -> None:
        """Update source file tracking after successful execution."""
        if self.node.node_type != "read":
            return

        path = self._get_source_path()
        if path:
            state.source_file_info = SourceFileInfo.from_path(path)

    def _prepare_for_execution(self, state: NodeExecutionState) -> None:
        """Prepare node state before execution."""
        self.node.clear_table_example()
        state.reset_results_only()
        self.node.results.errors = None
        self.node.results.resulting_data = None
        self.node.results.example_data = None

    def _clear_cache(self, state: NodeExecutionState) -> None:
        """Clear cached results."""
        self.node.remove_cache()
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
        run_location: schemas.ExecutionLocationsLiteral,
        performance_mode: bool,
        retry: bool,
        node_logger: NodeLogger,
    ) -> None:
        """Handle execution errors with retry logic."""
        error_str = str(error)
        state.mark_failed(error_str)
        self._sync_state_to_legacy(state)
        self.node.results.errors = error_str

        # Retry on missing file errors (upstream cache was cleared)
        if "No such file or directory (os error" in error_str and retry:
            node_logger.warning("Input file missing, retrying upstream nodes...")
            for node_input in self.node.node_inputs.get_all_inputs():
                # Recursively execute upstream nodes
                node_input.execute_node(
                    run_location=run_location,
                    performance_mode=performance_mode,
                    retry=True,
                    reset_cache=True,
                    node_logger=node_logger,
                )
            # Retry this node once (no further retries)
            self.execute(
                run_location=run_location,
                performance_mode=performance_mode,
                retry=False,
                node_logger=node_logger,
            )
            return

        # Log appropriate error message
        if "Connection refused" in error_str and "/submit_query/" in error_str:
            node_logger.warning(
                "Could not connect to remote worker. "
                "Ensure the worker process is running, or change settings to local execution."
            )
            node_logger.error("Remote worker connection refused")
        else:
            node_logger.error(f"Error running node: {error}")
