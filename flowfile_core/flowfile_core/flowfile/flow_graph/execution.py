"""Running a `FlowGraph`: run claiming, freshness probes, gate routing, staged execution and fetch-one."""

import datetime
import threading
from collections.abc import Collection, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import ExitStack
from contextvars import ContextVar
from time import time
from typing import TYPE_CHECKING, Any, Literal

import polars as pl
from polars_expr_transformer import simple_function_to_expr as to_expr

from flowfile_core.configs import logger
from flowfile_core.configs.flow_logger import FlowLogger
from flowfile_core.database.connection import get_db_context
from flowfile_core.events import publish
from flowfile_core.flowfile.catalog_cdc import (
    read_cursor,
    resolve_consumer_key,
)
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import (
    FlowDataEngine,
)
from flowfile_core.flowfile.flow_graph._base import GraphMixinBase
from flowfile_core.flowfile.flow_graph._root import root
from flowfile_core.flowfile.flow_graph.cloud import _cloud_change_read_target, _is_cloud_change_read
from flowfile_core.flowfile.flow_graph.freshness import (
    _catalog_reader_source_fingerprint,
    _cdc_since_param_state,
    _delta_reader_fingerprint,
    _probe_version_entry,
)
from flowfile_core.flowfile.flow_node.flow_node import (
    FlowNode,
)
from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE
from flowfile_core.flowfile.param_types import ParamValue, typed_parameter_values
from flowfile_core.flowfile.parameter_resolver import (
    node_parameters_resolved,
    resolve_expression_parameters,
)
from flowfile_core.flowfile.util.execution_orderer import ExecutionPlan, ExecutionStage, compute_execution_plan
from flowfile_core.flowfile.util.skip_rules import (
    GATE_NODE_TYPE,
    NodeRunStatus,
    classify_from_inputs,
    dead_gate_handles,
    effective_input_status,
    gate_has_else_output,
    parameter_gate_is_open,
    uses_any_rule,
)
from flowfile_core.kernel.execution import (
    KernelHold,
)
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.schemas.output_model import NodeResult, RunInformation

if TYPE_CHECKING:
    pass


ambient_kernel_hold: ContextVar[KernelHold | None] = ContextVar("ambient_kernel_hold", default=None)
"""The ``kernel_hold`` of the run whose thread this is: ``run_graph`` sets it for its own thread and every node it
executes, so a graph opened in-process during the run (a virtual table's producer) refuses the held kernels too.
Schema prefetches copy their starting context, so they carry it as well.
"""


def _gate_formula_matches(source: FlowDataEngine, formula: str) -> bool:
    """True when at least one row of *source* satisfies the gate's formula.

    The formula is the Filter node's flowfile expression language applied as
    a row predicate; matching is existence — ``filter(pred).head(1)`` — so
    only a bounded single-row collect happens in core. An empty formula, a
    formula that does not parse, or one referencing unknown columns raises,
    failing the gate visibly instead of silently picking a branch. An empty
    input (zero rows) is simply "no match": the gate closes.
    """
    if not formula.strip():
        raise ValueError("Gate is set to route on a formula, but no formula is configured")
    predicate = to_expr(formula)
    data_frame = source.data_frame
    lazy_frame = data_frame.lazy() if isinstance(data_frame, pl.DataFrame) else data_frame
    return lazy_frame.filter(predicate).head(1).collect().height > 0


def skip_node_message(flow_logger: FlowLogger, nodes: list[FlowNode]) -> None:
    """Logs a warning message listing all nodes that will be skipped during execution.

    Args:
        flow_logger: The logger instance for the flow.
        nodes: A list of FlowNode objects to be skipped.
    """
    if len(nodes) > 0:
        msg = "\n".join(str(node) for node in nodes)
        flow_logger.warning(f"skipping nodes:\n{msg}")


def execution_order_message(flow_logger: FlowLogger, stages: list[ExecutionStage]) -> None:
    """Logs an informational message showing the determined execution order with parallel stages.

    Args:
        flow_logger: The logger instance for the flow.
        stages: A list of ExecutionStage objects in execution order.
    """
    lines: list[str] = []
    for i, stage in enumerate(stages):
        node_strs = ", ".join(str(node) for node in stage)
        parallel_tag = " (parallel)" if len(stage) > 1 else ""
        lines.append(f"  Stage {i}{parallel_tag}: [{node_strs}]")
    flow_logger.info("execution order:\n" + "\n".join(lines))


class ExecutionMixin(GraphMixinBase):
    @property
    def last_closed_gate_handles(self) -> Mapping[str | int, frozenset[str]]:
        """Gate id -> the output handles the last ``run_graph`` routed dead (parameter and formula gates)."""
        return self._last_closed_gate_handles

    def check_flow_laziness(self) -> tuple[bool, list[str]]:
        """Check whether the flow supports lazy execution for virtual tables.

        Finds all catalog-writer nodes in the graph and checks whether their
        upstream dependencies are fully lazy.  Only the nodes that actually
        feed into a catalog writer matter — unrelated branches (e.g. an
        Explore Data node on a separate path) are ignored.

        Returns a tuple of (is_optimizable, reasons_if_not).
        """
        catalog_writers = [n for n in self.nodes if n.node_type == "catalog_writer"]
        if not catalog_writers:
            # No catalog writer → nothing to optimise; treat as non-lazy
            return False, ["No catalog writer node found in the flow"]
        all_reasons: list[str] = []
        for writer in catalog_writers:
            _, reasons = writer.check_upstream_laziness()
            all_reasons.extend(reasons)
        seen: set[str] = set()
        unique: list[str] = []
        for r in all_reasons:
            if r not in seen:
                seen.add(r)
                unique.append(r)
        return len(unique) == 0, unique

    @property
    def execution_mode(self) -> schemas.ExecutionModeLiteral:
        """Gets the current execution mode ('Development' or 'Performance')."""
        return self.flow_settings.execution_mode

    def get_implicit_starter_nodes(self) -> list[FlowNode]:
        """Finds nodes that can act as starting points but are not explicitly defined as such.

        Some nodes, like the Polars Code node, can function without an input. This
        method identifies such nodes if they have no incoming connections.

        Returns:
            A list of `FlowNode` objects that are implicit starting nodes.
        """
        starting_node_ids = [node.node_id for node in self._flow_starts]
        implicit_starting_nodes = []
        for node in self.nodes:
            if node.node_template.can_be_start and not node.has_input and node.node_id not in starting_node_ids:
                implicit_starting_nodes.append(node)
        return implicit_starting_nodes

    @execution_mode.setter
    def execution_mode(self, mode: schemas.ExecutionModeLiteral):
        """Sets the execution mode for the flow.

        Args:
            mode: The execution mode to set.
        """
        self.flow_settings.execution_mode = mode

    @property
    def execution_location(self) -> schemas.ExecutionLocationsLiteral:
        """Gets the current execution location."""
        return self.flow_settings.execution_location

    @execution_location.setter
    def execution_location(self, execution_location: schemas.ExecutionLocationsLiteral):
        """Sets the execution location for the flow.

        Args:
            execution_location: The execution location to set.
        """
        if self.flow_settings.execution_location != execution_location:
            self.reset()
        self.flow_settings.execution_location = execution_location

    def validate_if_node_can_be_fetched(self, node_id: int) -> None:
        flow_node = self._node_db.get(node_id)
        if not flow_node:
            raise Exception("Node not found found")
        execution_plan = compute_execution_plan(
            nodes=self.nodes, flow_starts=self._flow_starts + self.get_implicit_starter_nodes()
        )
        if flow_node.node_id in [skip_node.node_id for skip_node in execution_plan.skip_nodes]:
            raise Exception("Node can not be executed because it does not have it's inputs")

    def create_initial_run_information(self, number_of_nodes: int, run_type: Literal["fetch_one", "full_run"]):
        return RunInformation(
            flow_id=self.flow_id,
            start_time=datetime.datetime.now(),
            end_time=None,
            success=None,
            is_running=True,
            execution_mode=self.flow_settings.execution_mode,
            number_of_nodes=number_of_nodes,
            node_step_result=[],
            run_type=run_type,
        )

    def create_empty_run_information(self) -> RunInformation:
        return RunInformation(
            flow_id=self.flow_id,
            start_time=None,
            end_time=None,
            success=None,
            is_running=False,
            execution_mode=self.flow_settings.execution_mode,
            number_of_nodes=0,
            node_step_result=[],
            run_type="init",
        )

    def try_claim_run(self) -> bool:
        """Atomically claim the flow's single-run slot; False when a run is already in flight.

        Waits for an in-flight edit first, so a run never starts on a half-applied mutation
        (lock order: edit lock, then claim lock).
        """
        with self.edit_lock(bounded=False), self._run_claim_lock:
            if self.flow_settings.is_running:
                return False
            self.flow_settings.is_running = True
        self._bump_revision("run_started")
        return True

    def release_run(self) -> None:
        """Release the single-run slot claimed by try_claim_run (idempotent)."""
        with self._run_claim_lock:
            was_running = self.flow_settings.is_running
            self._kernel_hold = None
            self._commit_sources = True
            self.flow_settings.is_running = False
        if was_running:
            self._bump_revision("run_ended")

    def trigger_fetch_node(
        self,
        node_id: int,
        *,
        performance_mode: bool = False,
        reset_cache: bool = True,
    ) -> RunInformation | None:
        """Executes a specific node in the graph by its ID.

        The defaults are the data-preview contract: a non-performance run, so the
        node stores its result and can serve the 100-row example grid. Callers
        that only need the node's query plan (the Explore Data drawer) pass
        ``performance_mode=True``, which skips that store entirely, and
        ``reset_cache=False`` so exploring doesn't evict a useful cache.
        """
        if not self.try_claim_run():
            raise Exception("Flow is already running")
        flow_node = self.get_node(node_id)
        self.flow_settings.is_canceled = False
        self.flow_logger.clear_log_file()
        self.latest_run_info = self.create_initial_run_information(1, "fetch_one")
        node_logger = self.flow_logger.get_node_logger(flow_node.node_id)
        node_result = NodeResult(
            node_id=flow_node.node_id,
            node_name=flow_node.name,
            description=flow_node.get_node_information().description,
        )
        logger.info(f"Starting to run: node {flow_node.node_id}, start time: {node_result.start_timestamp}")
        self.latest_run_info.node_step_result.append(node_result)
        try:
            with node_parameters_resolved(flow_node):
                flow_node.execute_node(
                    run_location=self.flow_settings.execution_location,
                    performance_mode=performance_mode,
                    node_logger=node_logger,
                    optimize_for_downstream=False,
                    reset_cache=reset_cache,
                )
            errors = flow_node.results.errors
            node_result.finish(success=errors is None, error="" if errors is None else str(errors))
            if self.flow_settings.is_canceled:
                node_result.success = None
            if node_result.success:
                self.latest_run_info.nodes_completed += 1
            self.latest_run_info.end_time = datetime.datetime.now()
            return self.get_run_info()
        except Exception as e:
            node_logger.error(f"Error in node {flow_node.node_id}: {e}")
            node_result.finish(success=False, error=str(e))
        finally:
            self.release_run()

    @staticmethod
    def _resolve_input_names(node: FlowNode | None, table_count: int) -> list[str] | None:
        """Derive named input keys from connected source nodes.

        Uses the source node's ``node_reference`` when set, otherwise
        falls back to ``df_{node_id}``.  Returns ``None`` when no node
        is available, there are no input tables, or the number of
        connected sources doesn't match ``table_count`` (original
        unnamed behaviour).
        """
        if node is None or table_count == 0:
            return None
        input_names: list[str] = []
        for source_node in node.all_inputs:
            ref = getattr(source_node.setting_input, "node_reference", None)
            name = ref if ref else f"df_{source_node.node_id}"
            input_names.append(name)
        if len(input_names) != table_count:
            return None
        return input_names

    def _get_upstream_node_ids(self, node_id: int) -> list[int]:
        """Get all upstream node IDs (direct and transitive) for *node_id*.

        Traverses the ``all_inputs`` links recursively and returns a
        deduplicated list in breadth-first order.
        """
        node = self.get_node(node_id)
        if node is None:
            return []

        visited: set[int] = set()
        result: list[int] = []
        queue = list(node.all_inputs)
        while queue:
            current = queue.pop(0)
            cid = current.node_id
            if cid in visited:
                continue
            visited.add(cid)
            result.append(cid)
            queue.extend(current.all_inputs)
        return result

    def _get_required_kernel_ids(self) -> set[str]:
        """Return the set of kernel IDs used by ``python_script`` nodes."""
        kernel_ids: set[str] = set()
        for node in self.nodes:
            if node.node_type == "python_script" and node.setting_input is not None:
                kid = getattr(
                    getattr(node.setting_input, "python_script_input", None),
                    "kernel_id",
                    None,
                )
                if kid:
                    kernel_ids.add(kid)
        return kernel_ids

    def _compute_rerun_python_script_node_ids(
        self,
        plan_skip_ids: set[str | int],
    ) -> set[int]:
        """Return node IDs for ``python_script`` nodes that will re-execute.

        A python_script node will re-execute (and thus needs its old
        artifacts cleared) when:

        * It is NOT in the execution-plan skip set, **and**
        * Its execution state indicates it has NOT already run with the
          current setup (i.e. its cache is stale or it never ran).
        """
        rerun: set[int] = set()
        for node in self.nodes:
            if node.node_type != "python_script":
                continue
            if node.node_id in plan_skip_ids:
                continue
            if not node._execution_state.has_run_with_current_setup:
                rerun.add(node.node_id)
        return rerun

    def _group_rerun_nodes_by_kernel(
        self,
        rerun_node_ids: set[int],
    ) -> dict[str, set[int]]:
        """Group *rerun_node_ids* by their kernel ID.

        Returns a mapping ``kernel_id → {node_id, …}``.
        """
        kernel_nodes: dict[str, set[int]] = {}
        for node in self.nodes:
            if node.node_id not in rerun_node_ids:
                continue
            if node.node_type == "python_script" and node.setting_input is not None:
                kid = getattr(
                    getattr(node.setting_input, "python_script_input", None),
                    "kernel_id",
                    None,
                )
                if kid:
                    kernel_nodes.setdefault(kid, set()).add(node.node_id)
        return kernel_nodes

    def _execute_single_node(
        self,
        node: FlowNode,
        performance_mode: bool,
        run_info_lock: threading.Lock,
    ) -> tuple[NodeResult, FlowNode]:
        """Executes a single node, records its result, and returns both.

        Thread-safe: uses run_info_lock when mutating shared run information.

        Args:
            node: The node to execute.
            performance_mode: Whether to run in performance mode.
            run_info_lock: Lock protecting shared RunInformation state.

        Returns:
            A (NodeResult, FlowNode) tuple for post-stage failure propagation.
        """
        node_logger = self.flow_logger.get_node_logger(node.node_id)
        node_result = NodeResult(
            node_id=node.node_id,
            node_name=node.name,
            description=node.get_node_information().description,
        )

        with run_info_lock:
            self.latest_run_info.node_step_result.append(node_result)

        with ExitStack() as params_scope:
            params_scope.callback(ambient_kernel_hold.reset, ambient_kernel_hold.set(self._kernel_hold))
            try:
                params_scope.enter_context(node_parameters_resolved(node))
            except ValueError as e:
                # Never executed this run: a stale class must not describe this failure.
                node._last_exception_class = None
                node_logger.error(f"Parameter resolution failed for node {node.node_id}: {e}")
                node_result.finish(success=False, error=str(e))
                return node_result, node

            logger.info(f"Starting to run: node {node.node_id}, start time: {node_result.start_timestamp}")
            node.execute_node(
                run_location=self.flow_settings.execution_location,
                performance_mode=performance_mode,
                node_logger=node_logger,
            )
        try:
            errors = node.results.errors
            if self.flow_settings.is_canceled:
                node_result.error = "" if errors is None else str(errors)
                node_result.success = None
                node_result.is_running = False
                return node_result, node
            node_result.finish(success=errors is None, error="" if errors is None else str(errors))
        except Exception as e:
            node_logger.error(f"Error in node {node.node_id}: {e}")
            node_result.finish(success=False, error=str(e))

        node_logger.info(f"Completed node with success: {node_result.success}")
        if node_result.success:
            with run_info_lock:
                self.latest_run_info.nodes_completed += 1

        return node_result, node

    def _prepare_rerun_artifacts(self, plan_skip_ids: set[str | int]) -> None:
        """Prepare artifact state for nodes that will re-run.

        Computes which python_script nodes need re-execution, expands the set
        to include producer nodes whose artifacts were deleted, marks them
        stale, and clears both metadata and kernel-side artifacts.
        """
        rerun_node_ids = self._compute_rerun_python_script_node_ids(plan_skip_ids)

        # Expand re-run set: if a re-running node previously deleted
        # artifacts, the original producer nodes must also re-run so
        # those artifacts are available again in the kernel store.
        while True:
            deleted_producers = self.artifact_context.get_producer_nodes_for_deletions(
                rerun_node_ids,
            )
            new_ids = deleted_producers - rerun_node_ids
            if not new_ids:
                break
            rerun_node_ids |= new_ids

        # Force producer nodes (added due to artifact deletions) to
        # actually re-execute by marking their execution state stale.
        for nid in rerun_node_ids:
            node = self.get_node(nid)
            if node is not None and node._execution_state.has_run_with_current_setup:
                node._execution_state.has_run_with_current_setup = False

        # Also purge stale metadata for nodes not in this graph
        # (e.g. injected externally or left over from removed nodes).
        graph_node_ids = set(self._node_db.keys())
        stale_node_ids = {nid for nid in self.artifact_context._node_states if nid not in graph_node_ids}
        nodes_to_clear = rerun_node_ids | stale_node_ids
        if nodes_to_clear:
            self.artifact_context.clear_nodes(nodes_to_clear)

        if rerun_node_ids:
            kernel_node_map = self._group_rerun_nodes_by_kernel(rerun_node_ids)
            for kid, node_ids_for_kernel in kernel_node_map.items():
                try:
                    manager = root().get_kernel_manager()
                    manager.clear_node_artifacts_sync(
                        kid, list(node_ids_for_kernel), flow_id=self.flow_id, flow_logger=self.flow_logger
                    )
                except Exception:
                    logger.debug(
                        "Could not clear node artifacts for kernel '%s', nodes %s",
                        kid,
                        sorted(node_ids_for_kernel),
                    )

    def _execute_stages(
        self,
        execution_plan: ExecutionPlan,
        performance_mode: bool,
        params: dict[str, ParamValue],
        skip_node_ids: set[str | int],
        deliberate_skip_ids: set[str | int] | None = None,
        closed_gate_handles: dict[str | int, frozenset[str]] | None = None,
    ) -> set[str | int]:
        """Execute all stages in the plan, running independent nodes in parallel.

        Iterates through stages sequentially. Within each stage, independent
        nodes are executed in parallel (or sequentially if parallelism is
        disabled). Because stages are topological, each node is decided from its
        immediate inputs' statuses via the trigger rules (skip_rules) right
        before its stage runs — a failure skips its dependents stage by stage
        instead of via a blind transitive closure.

        ``skip_node_ids`` arrives holding the static (misconfiguration) skips
        and is mutated in place to accumulate every skipped node, preserving
        the caller's view. ``deliberate_skip_ids`` holds gate-driven skips and
        is likewise mutated when the rules propagate one.
        ``closed_gate_handles`` (gate id -> dead output handles) holds
        parameter-mode routing decided before the run; formula gates join it —
        mutated in place, same dict the caller created — as they execute and
        evaluate.

        Returns:
            Set of node IDs that failed during execution.
        """
        run_info_lock = threading.Lock()
        failed_node_ids: set[str | int] = set()
        if deliberate_skip_ids is None:
            deliberate_skip_ids = set()
        if closed_gate_handles is None:
            closed_gate_handles = {}

        statuses: dict[str | int, NodeRunStatus] = {}
        for node_id in skip_node_ids:
            statuses[node_id] = NodeRunStatus.SKIPPED_ERROR
        for node_id in deliberate_skip_ids:
            statuses[node_id] = NodeRunStatus.SKIPPED_DELIBERATE
            skip_node_ids.add(node_id)

        any_rule_nodes_touched: list[FlowNode] = []
        try:
            for stage in execution_plan.stages:
                if self.flow_settings.is_canceled:
                    self.flow_logger.info("Flow canceled")
                    break

                nodes_to_run: list[FlowNode] = []
                for node in stage.nodes:
                    already_decided = statuses.get(node.node_id)
                    if already_decided is not None and already_decided != NodeRunStatus.RUN:
                        skip_node_ids.add(node.node_id)
                        if already_decided == NodeRunStatus.SKIPPED_DELIBERATE:
                            self._record_deliberate_skips({node.node_id}, count_toward_total=False)
                        else:
                            self.flow_logger.get_node_logger(node.node_id).info(f"Skipping node {node.node_id}")
                        continue
                    input_statuses = [
                        effective_input_status(statuses, input_node.node_id, src_handle, closed_gate_handles)
                        for input_node, src_handle in node._slot_input_pairs()
                        if input_node is not None
                    ]
                    decision = classify_from_inputs(node, input_statuses)
                    if decision == NodeRunStatus.RUN:
                        if uses_any_rule(node):
                            self._apply_any_input_liveness(node, statuses, closed_gate_handles)
                            any_rule_nodes_touched.append(node)
                        nodes_to_run.append(node)
                        continue
                    statuses[node.node_id] = decision
                    skip_node_ids.add(node.node_id)
                    if decision == NodeRunStatus.SKIPPED_DELIBERATE:
                        deliberate_skip_ids.add(node.node_id)
                        self._record_deliberate_skips({node.node_id}, count_toward_total=False)
                    else:
                        self.flow_logger.get_node_logger(node.node_id).info(f"Skipping node {node.node_id}")

                if not nodes_to_run:
                    continue

                is_local = self.flow_settings.execution_location == "local"
                max_workers = 1 if is_local else self.flow_settings.max_parallel_workers
                if len(nodes_to_run) == 1 or max_workers == 1:
                    stage_results = [
                        self._execute_single_node(node, performance_mode, run_info_lock) for node in nodes_to_run
                    ]
                else:
                    stage_results: list[tuple[NodeResult, FlowNode]] = []
                    workers = min(max_workers, len(nodes_to_run))
                    with ThreadPoolExecutor(max_workers=workers) as executor:
                        futures = {
                            executor.submit(self._execute_single_node, node, performance_mode, run_info_lock): node
                            for node in nodes_to_run
                        }
                        for future in as_completed(futures):
                            stage_results.append(future.result())

                for node_result, node in stage_results:
                    if node_result.success:
                        statuses[node.node_id] = NodeRunStatus.RUN
                        if node.node_type == GATE_NODE_TYPE and self._gate_routes_on_formula(node):
                            # Evaluate routing from the source node's actual
                            # result, never from state stashed during the gate's
                            # own execution — executor/worker cache branches can
                            # legitimately skip the gate's function, and a stale
                            # stash would replay last run's routing decision.
                            try:
                                closed = self._formula_gate_is_closed(node, params or None)
                            except Exception as e:
                                node_result.success = False
                                node_result.error = f"Gate formula evaluation failed: {e}"
                                self.latest_run_info.nodes_completed -= 1
                                # The node itself ran fine; no stale class may describe this failure.
                                node._last_exception_class = None
                                statuses[node.node_id] = NodeRunStatus.FAILED
                                failed_node_ids.add(node.node_id)
                                skip_node_ids.add(node.node_id)
                                continue
                            dead = dead_gate_handles(not closed, gate_has_else_output(node))
                            if dead:
                                closed_gate_handles[node.node_id] = dead
                                side = "then branch" if gate_has_else_output(node) and closed else "downstream"
                                if closed:
                                    self.flow_logger.get_node_logger(node.node_id).info(
                                        f"Gate {node.node_id} closed by its formula; {side} will be skipped"
                                    )
                                else:
                                    self.flow_logger.get_node_logger(node.node_id).info(
                                        f"Gate {node.node_id} open; its else branch will be skipped"
                                    )
                    else:
                        statuses[node.node_id] = NodeRunStatus.FAILED
                        failed_node_ids.add(node.node_id)
                        skip_node_ids.add(node.node_id)
        finally:
            # A node that did not execute this run has not run: clear the run
            # flags of every skipped node so the next run re-executes it instead
            # of serving the previous run's frame from the dev-mode cache.
            # Failed nodes are already handled by mark_failed.
            for node_id in skip_node_ids - failed_node_ids:
                node = self.get_node(node_id)
                if node is None:
                    continue
                node.node_stats.has_run_with_current_setup = False
                node.node_stats.has_completed_last_run = False
                node._execution_state.has_run_with_current_setup = False
                node._execution_state.has_completed_last_run = False
            for node in any_rule_nodes_touched:
                node._skipped_input_ids_this_run = frozenset()

        return failed_node_ids

    def _apply_any_input_liveness(
        self,
        node: FlowNode,
        statuses: dict[str | int, NodeRunStatus],
        closed_gate_handles: dict[str | int, frozenset[str]] | None = None,
    ) -> None:
        """Prepare an ANY-rule node (union) to run with a partial input set.

        Stashes which inputs were deliberately skipped this run — keyed by
        (source id, source handle), so a two-output gate feeding the union is
        judged per edge — so input assembly can drop them from the concat, and
        invalidates the node when the surviving-input set differs from the
        previous run — the node's hash only folds settings and input hashes, so
        a gate flip would otherwise serve the previous run's partial result from
        the dev-mode cache (and the worker's cache_results lookup).
        """
        input_pairs = [
            (input_node, src_handle) for input_node, src_handle in node._slot_input_pairs() if input_node is not None
        ]
        surviving = frozenset(
            (input_node.node_id, src_handle)
            for input_node, src_handle in input_pairs
            if effective_input_status(statuses, input_node.node_id, src_handle, closed_gate_handles)
            == NodeRunStatus.RUN
        )
        node._skipped_input_ids_this_run = frozenset(
            (input_node.node_id, src_handle)
            for input_node, src_handle in input_pairs
            if (input_node.node_id, src_handle) not in surviving
        )
        previous = self._any_input_liveness.get(node.node_id)
        if previous is not None and previous != surviving:
            self.flow_logger.info(
                f"Node {node.node_id}: surviving inputs changed since last run "
                f"({sorted(map(str, previous))} -> {sorted(map(str, surviving))}); invalidating cached result"
            )
            node.invalidate_cache()
        elif previous is None and node._skipped_input_ids_this_run:
            # No liveness history (fresh graph, undo-rebuilt nodes) but running
            # with a partial input set: any surviving cache may hold a concat
            # built under a different gate topology — invalidate to be safe.
            node.invalidate_cache()
        self._any_input_liveness[node.node_id] = surviving

    @staticmethod
    def _gate_routes_on_formula(node: FlowNode) -> bool:
        gate_input = getattr(node.setting_input, "gate_input", None)
        return gate_input is not None and gate_input.condition_source == "formula"

    def _formula_gate_is_closed(self, node: FlowNode, params: dict[str, Any] | None = None) -> bool:
        """Fresh routing decision for a formula gate, from its source node's result.

        The predicate runs against the control input when one is connected,
        else against the gate's own data input — resolved through the edge's
        recorded source handle, like input assembly, so a control wired to a
        secondary output (e.g. a split filter's fail side) probes that frame,
        not the source's default output. The memoized result is served when
        the source ran in an earlier stage; the resolution lazily pulls when
        the gate's own cache branch short-cut input assembly. ``${param}``
        refs in the formula render as typed expression literals (strings
        quoted) like every other expression field — the stage loop sees the
        restored (unsubstituted) settings. Raises on a missing/unparsable
        formula — the caller fails the gate visibly.
        """
        gate_input = node.setting_input.gate_input
        source_node = node.node_inputs.right_input
        if source_node is None and node.node_inputs.main_inputs:
            source_node = node.node_inputs.main_inputs[0]
        if source_node is None:
            raise ValueError("gate has no input to evaluate its formula against")
        handle = node._input_output_handles.get(source_node.node_id, DEFAULT_OUTPUT_HANDLE)
        source_result = node._resolve_input_result_for_handle(source_node, handle)
        if source_result is None:
            raise ValueError("gate input produced no result to evaluate its formula against")
        formula = resolve_expression_parameters(gate_input.formula, params or {})
        return not _gate_formula_matches(source_result, formula)

    def _evaluate_gate_conditions(self, nodes: list[FlowNode] | None = None) -> dict[str | int, frozenset[str]]:
        """Evaluate parameter-mode gates (of ``nodes``, default all) before the run; returns per-gate dead handles.

        Formula gates are decided when they execute (stage loop). A gate with
        an else output always routes exactly one handle dead on a successful
        evaluation. A gate whose condition cannot be evaluated (unknown
        parameter, un-coercible value) gets no entry — fully open — so it
        fails visibly at execution and its downstream (both sides) error-skips
        — fail-closed either way, but with the error on the canvas instead of
        a silently-picked branch.
        """
        closed: dict[str | int, frozenset[str]] = {}
        for node in self.nodes if nodes is None else nodes:
            if node.node_type != "gate" or not node.is_correct:
                continue
            gate_input = getattr(node.setting_input, "gate_input", None)
            if gate_input is None or gate_input.condition_source != "parameter":
                continue
            try:
                is_open = parameter_gate_is_open(gate_input, self.flow_settings.parameters)
                dead = dead_gate_handles(is_open, gate_has_else_output(node))
                if dead:
                    closed[node.node_id] = dead
            except ValueError:
                # A broken condition must fail loudly, and the gate's own hash
                # does not fold flow parameters — after one green run the
                # dev-mode cache would skip its function and silently treat
                # the gate as open. Invalidate so it re-executes (defeating
                # the dev-mode skip and any cache_results hit) and raises the
                # validation error on the canvas.
                node.invalidate_cache()
                continue
        return closed

    def _run_post_execution_callbacks(
        self,
        failed_node_ids: set[str | int],
        skip_node_ids: set[str | int],
        deliberate_skip_ids: set[str | int] | None = None,
        run_node_ids: set[str | int] | None = None,
    ) -> None:
        """Invoke _on_flow_complete callbacks registered by source nodes.

        Each callback receives ``success=True`` when the node and all its
        downstream dependents completed without failure or error-driven skip.
        Used e.g. by Kafka sources to commit offsets only on full success.

        A *deliberate* skip (a branch gated off by design) counts as complete:
        a Kafka source upstream of a closed gate must still commit its offsets,
        otherwise it would re-read the same messages on every run. Skips caused
        by a failure or misconfiguration keep blocking the callback.

        ``run_node_ids`` (a run restricted to those nodes) calls back only a node whose whole
        downstream is among them; any other keeps its callback for a run that reaches it all.

        Note: the caller must guard against cancellation — this method is
        only invoked when ``is_canceled`` is False.
        """
        deliberate_skip_ids = deliberate_skip_ids or set()
        incomplete_node_ids = (failed_node_ids | skip_node_ids) - deliberate_skip_ids

        for n in self.nodes:
            callback = n._on_flow_complete
            if callback is None:
                continue
            downstream = list(n.get_all_dependent_nodes())
            if run_node_ids is not None and any(
                node_id not in run_node_ids for node_id in [n.node_id, *(dep.node_id for dep in downstream)]
            ):
                continue
            downstream_incomplete = n.node_id in incomplete_node_ids or any(
                dep.node_id in incomplete_node_ids for dep in downstream
            )
            success = not downstream_incomplete
            if success and deliberate_skip_ids:
                gated_outputs = [
                    dep.node_id
                    for dep in downstream
                    if dep.node_id in deliberate_skip_ids and dep.node_template.node_group == "output"
                ]
                if gated_outputs:
                    self.flow_logger.warning(
                        f"Node {n.node_id}: committing source progress while output node(s) "
                        f"{gated_outputs} were deliberately skipped by a gate"
                    )
            try:
                callback(success)
            except Exception as e:
                self.flow_logger.error(f"Post-execution callback failed for node {n.node_id}: {e}")
            n._on_flow_complete = None

    def _record_deliberate_skips(self, deliberate_skip_ids: set[str | int], count_toward_total: bool = True) -> None:
        """Record deliberately-skipped nodes as green, zero-work results.

        Deliberate skips (closed-gate branches) are part of a successful run:
        each gets a ``NodeResult(skipped=True, success=True)`` row so the run
        report and canvas can show the state, and counts as completed so
        progress reaches the denominator. Their run flags are cleared so the
        next run re-executes them if the gate opens — a node that did not
        execute this run has not run.

        ``count_toward_total`` is True for plan-level skips (excluded from
        ``ExecutionPlan.node_count``, so the denominator must grow) and False
        for skips decided mid-run on nodes the plan already counted. Only ever
        called single-threaded — before the stages start or between stage
        executions — so the bare ``node_step_result.append`` is safe.
        """
        if not deliberate_skip_ids or self.latest_run_info is None:
            return
        already_recorded = {nr.node_id for nr in self.latest_run_info.node_step_result if nr.skipped}
        for node_id in sorted(deliberate_skip_ids, key=str):
            node = self.get_node(node_id)
            if node is None or node.node_id in already_recorded:
                continue
            now = time()
            self.latest_run_info.node_step_result.append(
                NodeResult(
                    node_id=node.node_id,
                    node_name=node.name,
                    success=True,
                    skipped=True,
                    start_timestamp=now,
                    end_timestamp=now,
                    run_time_ms=0,
                    is_running=False,
                )
            )
            self.latest_run_info.nodes_completed += 1
            if count_toward_total:
                self.latest_run_info.number_of_nodes += 1
            node.node_stats.has_run_with_current_setup = False
            node.node_stats.has_completed_last_run = False
            node._execution_state.has_run_with_current_setup = False
            node._execution_state.has_completed_last_run = False
            self.flow_logger.get_node_logger(node.node_id).info(
                f"Node {node.node_id} deliberately skipped (gated off); marked as skipped, not failed"
            )

    def _refresh_catalog_reader_freshness(self, nodes: list[FlowNode] | None = None) -> None:
        """Invalidate catalog_reader nodes (of ``nodes``, default all) whose Delta sources changed since their last run.

        The node hash is source-blind (settings + upstream hashes only), so in
        Development mode an unchanged-settings reader is skipped and downstream
        keeps reading a frozen worker snapshot. This probes the live Delta
        versions once per run and bumps the node's cache epoch on drift — which
        rotates the hash, defeats the dev-mode skip AND the explicit
        cache_results worker lookup, and cascades resets downstream.

        Pinned ``delta_version`` readers are deliberate time travel and are
        never probed. Probe failures fail open (invalidate) so the real error
        surfaces on the canvas instead of a silently-served stale snapshot.
        Cloud readers get the same pass only when they read a Delta change feed.
        """
        version_cache: dict[str, int] = {}
        opts_by_namespace: dict[int | None, dict | None] = {}
        for node in self.nodes if nodes is None else nodes:
            settings = node.setting_input
            cloud_change_read = (
                node.node_type == "cloud_storage_reader"
                and isinstance(settings, input_schema.NodeCloudStorageReader)
                and _is_cloud_change_read(settings.cloud_storage_settings)
            )
            if not cloud_change_read:
                if node.node_type != "catalog_reader" or not isinstance(settings, input_schema.NodeCatalogReader):
                    continue
                if not settings.sql_query and settings.delta_version is not None:
                    continue
            try:
                if cloud_change_read:
                    fingerprint, force = self._cloud_change_read_fingerprint(node, version_cache), False
                else:
                    fingerprint, force = _catalog_reader_source_fingerprint(
                        settings, version_cache, opts_by_namespace, cdc_state=self._cdc_fingerprint_state(node)
                    )
            except Exception:
                self.flow_logger.warning(
                    f"Node {node.node_id}: could not probe source freshness; re-running to be safe"
                )
                fingerprint, force = None, True
            recorded = node._execution_state.source_version_info
            if force or (recorded is not None and fingerprint != recorded):
                node.invalidate_cache()
                self.flow_logger.info(f"Node {node.node_id}: source changed; invalidating cached result")
            node._execution_state.source_version_info = fingerprint

    def _cdc_fingerprint_state(self, node: FlowNode) -> int | str | None:
        """The run state a change reader's output depends on, for its freshness fingerprint.

        ``since_last_run`` folds in the stored cursor; the version and timestamp modes fold in the
        resolved ``since`` value when it comes from a flow parameter (a literal is already part of
        the settings hash). Unresolvable (no registration, no table id, no cursor yet, unknown
        parameter) means "nothing to fold in" — the head probe alone then decides freshness.
        """
        settings = node.setting_input
        if settings.cdc_mode in ("since_version", "since_timestamp"):
            return _cdc_since_param_state(settings, node)
        if settings.cdc_mode != "since_last_run" or not settings.catalog_table_id:
            return None
        try:
            consumer_key, _ = resolve_consumer_key(self, settings)
            with get_db_context() as db:
                cursor = read_cursor(db, settings.catalog_table_id, consumer_key)
        except Exception:
            return None
        return cursor.last_version if cursor is not None else None

    def _cloud_change_read_fingerprint(self, node: FlowNode, version_cache: dict[str, int]) -> str:
        """Freshness fingerprint of a cloud Delta change reader: live head plus a ``${param}`` ``since`` value.

        Path and connection ``${param}`` refs are resolved for the probe. Raises on probe failures (an
        undefined parameter included), which the caller treats as a fail-open re-run.
        """
        read_settings = node.setting_input.cloud_storage_settings
        cdc_state = _cdc_since_param_state(read_settings, node)
        with node_parameters_resolved(node):
            path, storage_options = _cloud_change_read_target(read_settings, node.setting_input.user_id)
            head = _probe_version_entry(path, storage_options, version_cache)
        return _delta_reader_fingerprint(path, head, cdc_state)

    def _refresh_read_source_freshness(self, nodes: list[FlowNode] | None = None) -> None:
        """Invalidate read nodes (of ``nodes``, default all) whose source files changed since their last run.

        Same rationale as the catalog pass above: the node hash is source-blind, so a changed
        or added file would let Development mode and cache_results serve stale results — and
        because downstream hashes fold in this node's, the whole chain would skip. The epoch
        bump must happen here, before the stages run: a rotation done inside the executor is
        reverted by _execute_single_node's hash save/restore. The stale worker entry is purged
        first, while the hash it was stored under is still current, and reset() must follow
        invalidate_cache() immediately — any hash access in between re-memoizes the hash and
        the downstream reset cascade never fires.
        """
        for node in self.nodes if nodes is None else nodes:
            if node.node_type != "read":
                continue
            info = node._execution_state.source_file_info
            if info is None or not info.has_changed():
                continue
            try:
                node.remove_cache()
            except Exception:
                self.flow_logger.warning(f"Node {node.node_id}: could not purge the stale worker cache entry")
            node.invalidate_cache()
            node.reset()
            self.flow_logger.info(f"Node {node.node_id}: source files changed; invalidating cached result")

    def run_graph(
        self,
        *,
        node_ids: Collection[int | str] | None = None,
        kernel_hold: KernelHold | None = None,
        commit_sources: bool = True,
    ) -> RunInformation | None:
        """Executes the entire data flow graph from start to finish.

        Independent nodes within the same execution stage are run in parallel
        using threads. Stages are processed sequentially so that all dependencies
        are satisfied before a stage begins.

        Args:
            node_ids: Restrict the run to these nodes; pass a closed set (each node together with
                every node it reads from), since a node whose input lies outside it is unreachable.
                Only they are probed, routed, planned, executed and reported, and a source's
                post-execution callback fires only when its whole downstream is among them.
                ``None`` runs the whole graph.
            kernel_hold: Kernels whose execution lock the caller holds while it waits on this run; a kernel
                node on one fails at once (``KernelBusyError``) instead of waiting, here and in subflows.
                ``None`` keeps the hold of a run this one runs inside (``ambient_kernel_hold``), such as a
                subflow of a virtual table's producer.
            commit_sources: ``False`` for a run that only looks at rows (a notebook's lineage run that holds no
                output node): no source's post-execution callback fires, so no change-feed cursor or Kafka
                offset moves. The callbacks stay set and fire in the next run that commits. A subflow this
                run runs inherits it (``_commit_sources``), so its sources commit only when this run does.

        Returns:
            A RunInformation object summarizing the execution results.

        Raises:
            Exception: If the flow is already running.
        """
        if not self.try_claim_run():
            raise Exception("Flow is already running")
        if kernel_hold is None:
            kernel_hold = ambient_kernel_hold.get()
        self._kernel_hold = kernel_hold
        self._commit_sources = commit_sources
        ambient = ambient_kernel_hold.set(kernel_hold)
        released = False
        try:
            self.flow_settings.is_canceled = False
            self.flow_logger.clear_log_file()
            self.flow_logger.info("Starting to run flowfile flow...")

            publish("flow_run_started", graph=self)

            selected = None if node_ids is None else set(node_ids)
            run_nodes = self.nodes if selected is None else [n for n in self.nodes if n.node_id in selected]
            self._refresh_catalog_reader_freshness(run_nodes)
            self._refresh_read_source_freshness(run_nodes)

            params: dict[str, ParamValue] = typed_parameter_values(self.flow_settings.parameters)
            # Parameter-mode gates are routed before anything runs; the plan
            # classifies each dead handle's downstream as deliberately skipped
            # (green, not failed). Formula gates are decided when they execute.
            closed_gate_handles = self._evaluate_gate_conditions(run_nodes)
            # Same dict the stage loop folds formula-gate decisions into, so it ends as the run's final routing.
            self._last_closed_gate_handles = closed_gate_handles

            flow_starts = self._flow_starts + self.get_implicit_starter_nodes()
            execution_plan = compute_execution_plan(
                nodes=run_nodes,
                flow_starts=flow_starts if selected is None else [n for n in flow_starts if n.node_id in selected],
                closed_gate_handles=closed_gate_handles,
            )

            plan_skip_ids: set[str | int] = {n.node_id for n in execution_plan.skip_nodes}
            deliberate_skip_ids: set[str | int] = {n.node_id for n in execution_plan.deliberate_skip_nodes}
            not_selected_ids = set() if selected is None else {n.node_id for n in self.nodes} - selected
            self._prepare_rerun_artifacts(plan_skip_ids | deliberate_skip_ids | not_selected_ids)

            self.latest_run_info = self.create_initial_run_information(execution_plan.node_count, "full_run")
            skip_node_message(self.flow_logger, execution_plan.skip_nodes)
            execution_order_message(self.flow_logger, execution_plan.stages)

            performance_mode = self.flow_settings.execution_mode == "Performance"
            self._record_deliberate_skips(deliberate_skip_ids)

            failed_node_ids = self._execute_stages(
                execution_plan, performance_mode, params, plan_skip_ids, deliberate_skip_ids, closed_gate_handles
            )
            if commit_sources and not self.flow_settings.is_canceled:
                self._run_post_execution_callbacks(failed_node_ids, plan_skip_ids, deliberate_skip_ids, selected)

            self.latest_run_info.end_time = datetime.datetime.now()
            self.flow_logger.info("Flow completed!")
            self.end_datetime = datetime.datetime.now()
            self.release_run()
            released = True
            if self.flow_settings.is_canceled:
                self.flow_logger.info("Flow canceled")
            run_info = self.get_run_info()
            publish("flow_run_finished", graph=self, run_info=run_info)
            return run_info
        except BaseException as e:
            # A pyo3 panic is BaseException-only; `except Exception` would miss it.
            publish("flow_run_crashed", graph=self, error=e)
            raise
        finally:
            ambient_kernel_hold.reset(ambient)
            # Released once: a second release would end a run another caller claimed meanwhile.
            if not released:
                self.release_run()

    def get_run_info(self) -> RunInformation:
        """Gets a summary of the most recent graph execution.

        Returns:
            A RunInformation object with details about the last run.
        """
        is_running = self.flow_settings.is_running
        if self.latest_run_info is None:
            return self.create_empty_run_information()

        run_info = self.latest_run_info
        run_info.is_running = is_running
        run_info.execution_mode = self.flow_settings.execution_mode
        if not is_running and run_info.success is None:
            run_info.success = all(nr.success for nr in run_info.node_step_result)
        return run_info

    def cancel(self):
        """Cancels an ongoing graph execution."""

        if not self.flow_settings.is_running:
            return
        self.flow_settings.is_canceled = True
        for node in self.nodes:
            node.cancel()

    def close_flow(self):
        """Performs cleanup operations, such as clearing node caches."""

        for node in self.nodes:
            node.remove_cache()

    def reset(self):
        """Forces a deep reset on all nodes in the graph."""

        for node in self.nodes:
            node.reset(True)
