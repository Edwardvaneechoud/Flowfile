"""User-defined (custom) nodes: placement from the registry, schema prediction, local or worker
execution and kernel execution of kernel-environment nodes.
"""

import os
import threading
from collections.abc import Callable
from copy import deepcopy

import polars as pl

from flowfile_core.configs import logger
from flowfile_core.configs.node_store import CUSTOM_NODE_STORE, register_missing_node_template
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import (
    FlowDataEngine,
)
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.models import custom_node_task_id
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
    CustomNodeExecuteInput,
    ExternalCustomNodeFetcher,
)
from flowfile_core.flowfile.flow_graph._base import GraphMixinBase
from flowfile_core.flowfile.flow_graph._root import root
from flowfile_core.flowfile.flow_graph.execution import ambient_kernel_hold
from flowfile_core.flowfile.flow_node.flow_node import (
    FlowNode,
    data_needed_block_reason,
    kernel_block_reason,
    kernel_not_run_reason,
)
from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE, output_handle
from flowfile_core.flowfile.node_designer.custom_node import CustomNodeBase
from flowfile_core.flowfile.parameter_resolver import (
    apply_parameters_in_place,
    find_unresolved_in_model,
)
from flowfile_core.flowfile.schema_callbacks import (
    pl_schema_to_flowfile_columns,
)
from flowfile_core.flowfile.user_defined.dispatch import resolve_secret_payload
from flowfile_core.flowfile.user_defined.kernel_codegen import generate_kernel_script
from flowfile_core.flowfile.user_defined.registry import (
    KernelDependencyError,
    KernelRequiredError,
    missing_custom_node_error,
)
from flowfile_core.kernel.execution import (
    build_execute_request,
    clear_stale_parquets,
    forward_kernel_logs,
    read_kernel_outputs,
    write_inputs_to_parquet,
)
from flowfile_core.kernel.matching import verify_kernel_for_node
from flowfile_core.schemas import input_schema


class CustomNodeBuildersMixin(GraphMixinBase):
    def add_user_defined_node(
        self, *, custom_node: CustomNodeBase, user_defined_node_settings: input_schema.UserDefinedNode
    ):
        """Adds a user-defined custom node to the graph.

        When the custom node has a ``kernel_id`` set, the process code is sent
        to the kernel for execution instead of running locally.  This enables
        custom nodes to use external packages installed on the kernel.

        Args:
            custom_node: The custom node instance to add.
            user_defined_node_settings: The settings for the user-defined node.
        """
        kernel_id = user_defined_node_settings.kernel_id or custom_node.kernel_id
        if (custom_node.environment == "kernel" or custom_node.requires_kernel) and not kernel_id:
            raise KernelRequiredError(custom_node.item)

        registry_entry = root().user_defined_registry.get(custom_node.item)
        if registry_entry is not None and registry_entry.source_hash:
            user_defined_node_settings.node_source_hash = registry_entry.source_hash

        # Output handles are structural — the node class declares them; the settings
        # copy is a persistence snapshot kept in sync for save/codegen.
        output_names = list(custom_node.output_names or user_defined_node_settings.output_names)
        user_defined_node_settings.output_names = output_names

        if kernel_id:
            _func = self._make_kernel_user_defined_func(
                custom_node=custom_node,
                user_defined_node_settings=user_defined_node_settings,
                kernel_id=kernel_id,
                output_names=output_names,
                registry_entry=registry_entry,
            )
        else:
            _func = self._make_local_user_defined_func(
                custom_node=custom_node,
                user_defined_node_settings=user_defined_node_settings,
                output_names=output_names,
                registry_entry=registry_entry,
            )

        # Wire the hook through add_node_step so user_provided_schema_callback is set
        # BEFORE setting_input triggers reset(): otherwise a 0-input node's eager
        # schema prefetch would run the real function (kernel/worker) in the background.
        schema_callback = None
        if type(custom_node).predict_output_schema is not CustomNodeBase.predict_output_schema:
            schema_callback = self._make_user_defined_schema_callback(
                custom_node=custom_node,
                node_id=user_defined_node_settings.node_id,
                output_names=output_names,
            )
        elif kernel_id or bool(getattr(custom_node, "requires_data_for_prediction", False)):
            # Hookless kernel or data-dependent node: never predict by executing; block until run.
            schema_callback = self._make_blocked_prediction_callback(
                node_id=user_defined_node_settings.node_id, on_kernel=bool(kernel_id)
            )
        else:
            # Traceability: a stale registry class (or a genuinely hook-less node)
            # lands here and schema prediction degrades to the execution tier.
            logger.info(
                f"custom node {custom_node.item}: no predict_output_schema override on "
                f"{type(custom_node).__module__}.{type(custom_node).__name__}; "
                f"schema prediction uses the execution tier"
            )

        self.add_node_step(
            node_id=user_defined_node_settings.node_id,
            function=_func,
            setting_input=user_defined_node_settings,
            input_node_ids=user_defined_node_settings.depending_on_ids,
            node_type=custom_node.item,
            schema_callback=schema_callback,
        )
        node = self.get_node(user_defined_node_settings.node_id)
        node._executes_on_kernel = bool(kernel_id)
        node._prediction_requires_data = bool(getattr(custom_node, "requires_data_for_prediction", False))
        if custom_node.number_of_inputs == 0:
            self.add_node_to_starting_list(node)
        if custom_node.settings_schema is not None and user_defined_node_settings.settings:
            report = custom_node.settings_schema.populate_values_report(user_defined_node_settings.settings)
            if report.has_drift:
                unknown = report.unknown_sections + report.unknown_components
                node.results.warnings = (
                    f"Stored settings no longer match the node's schema; ignored keys: {', '.join(sorted(unknown))}"
                )

    def add_missing_user_defined_node(
        self, *, user_defined_node_settings: input_schema.UserDefinedNode, node_type: str, error: str
    ):
        """Adds a placeholder for a custom node that cannot be loaded on this machine.

        The stored settings are preserved verbatim (lossless re-save), the node
        renders with its connections, and running the flow fails this node with
        ``error`` instead of silently dropping it.
        """
        register_missing_node_template(node_type)

        def _missing_custom_node(*_flow_data_engine: FlowDataEngine) -> FlowDataEngine:
            raise ValueError(error)

        self.add_node_step(
            node_id=user_defined_node_settings.node_id,
            function=_missing_custom_node,
            setting_input=user_defined_node_settings,
            input_node_ids=user_defined_node_settings.depending_on_ids,
            node_type=node_type,
        )
        node = self.get_node(user_defined_node_settings.node_id)
        node.results.errors = error

    def _place_user_defined_node(
        self, node_type: str, user_defined_node_settings: input_schema.UserDefinedNode
    ) -> None:
        """Place a custom node from the store, degrading to a missing-node placeholder when
        its type isn't installed. Shared by copy and both flow-restore paths."""
        if node_type not in CUSTOM_NODE_STORE:
            root().user_defined_registry.refresh()
        user_defined_node_class = CUSTOM_NODE_STORE.get(node_type)
        if user_defined_node_class is not None:
            self.add_user_defined_node(
                custom_node=user_defined_node_class.from_settings(user_defined_node_settings.settings),
                user_defined_node_settings=user_defined_node_settings,
            )
        else:
            self.add_missing_user_defined_node(
                user_defined_node_settings=user_defined_node_settings,
                node_type=node_type,
                error=missing_custom_node_error(node_type),
            )

    @staticmethod
    def _predicted_value_to_columns(value) -> list[FlowfileColumn] | None:
        """Normalize a predict_output_schema return value into FlowfileColumns.

        LazyFrames/DataFrames contribute their (lazily resolved) schema; a plain
        ``pl.Schema`` is tolerated for hand-declared shapes. None means unusable.
        """
        if isinstance(value, pl.LazyFrame):
            return pl_schema_to_flowfile_columns(value.collect_schema())
        if isinstance(value, pl.DataFrame):
            return pl_schema_to_flowfile_columns(value.schema)
        if isinstance(value, pl.Schema):
            return pl_schema_to_flowfile_columns(value)
        return None

    def _make_blocked_prediction_callback(self, *, node_id: int, on_kernel: bool = False) -> Callable:
        """Hookless ``requires_data_for_prediction=True`` or kernel node: prediction must
        never execute ``process()``. Wired through ``add_node_step`` so a 0-input
        node's eager prefetch hits this cheap callback instead of the real
        function. ``on_kernel`` selects the kernel wording of the warning."""

        def schema_callback() -> list[FlowfileColumn]:
            node = self.get_node(node_id)
            if node is None:
                return []
            if node.node_stats.has_completed_last_run and node.node_schema.result_schema:
                node._schema_prediction_blocked = None
                return node.node_schema.result_schema
            reason = kernel_not_run_reason(node) if on_kernel else data_needed_block_reason(node)
            node._schema_prediction_blocked = reason
            node.results.warnings = reason
            return []

        return schema_callback

    def _make_user_defined_schema_callback(
        self, *, custom_node: CustomNodeBase, node_id: int, output_names: list[str]
    ) -> Callable:
        """Build a schema callback from the node's ``predict_output_schema`` hook.

        The hook always runs in core, even for kernel nodes, and returns a frame
        (or dict of frames) whose schema is read lazily. Data-needing hooks
        (``requires_data_for_prediction=True``) get real upstream data —
        materialized in-core when the un-run chain is kernel-free, or a kernel
        warning instead (never an implicit kernel run). Returning ``[]`` makes
        the prediction ladder fall back to the execution-based path. The node is
        resolved lazily so the callback can be passed into ``add_node_step``
        before the node exists. The hook runs on an instance built from the
        node's settings with the flow's ``${name}`` references resolved, as
        ``process()`` sees them; the stored settings are left untouched.
        """
        resolved_output_names = output_names or ["main"]
        requires_data = bool(getattr(custom_node, "requires_data_for_prediction", False))

        def _hook_instance(node: FlowNode) -> CustomNodeBase:
            settings = node.setting_input.settings or {}
            params = node._params_getter() if node._params_getter else {}
            if not params or not find_unresolved_in_model(settings):
                return custom_node
            resolved = deepcopy(settings)
            try:
                apply_parameters_in_place(resolved, params)
            except ValueError:
                pass  # an undeclared reference stays as text, as in execution-tier prediction
            return type(custom_node).from_settings(resolved)

        def _hook_input_frame(input_node: FlowNode, src_handle: str) -> pl.LazyFrame:
            # Real lazy data when the upstream has run (worker results are
            # scan_ipc plans, cheap to sample). For data-needing hooks on a
            # kernel-free chain, materialize the un-run upstream in-core,
            # pivot-style — the hook's own collect bounds what is computed.
            if input_node.node_stats.has_completed_last_run:
                engine = (input_node._named_outputs or {}).get(src_handle) or input_node.results.resulting_data
                if engine is not None:
                    frame = engine.data_frame
                    return frame if isinstance(frame, pl.LazyFrame) else frame.lazy()
            if requires_data:
                engine = input_node.get_output(src_handle) or input_node.get_resulting_data()
            else:
                engine = input_node.get_predicted_resulting_data(src_handle)
            frame = engine.data_frame
            return frame if isinstance(frame, pl.LazyFrame) else frame.lazy()

        def schema_callback() -> list[FlowfileColumn]:
            node = self.get_node(node_id)
            if node is None:
                return []
            node._schema_prediction_blocked = None
            if requires_data:
                reason = kernel_block_reason(node, include_self=False)
                if reason:
                    # Never execute a kernel implicitly for prediction: surface
                    # the warning and let the exec-tier gate suppress fallback.
                    node._schema_prediction_blocked = reason
                    node.results.warnings = reason
                    return []
            try:
                input_frames = []
                for input_node, src_handle in node._slot_input_pairs():
                    if input_node is None:
                        input_frames.append(pl.LazyFrame())
                        continue
                    input_frames.append(_hook_input_frame(input_node, src_handle))
                predicted = _hook_instance(node).predict_output_schema(*input_frames)
            except Exception as e:
                logger.warning(f"predict_output_schema failed for node {node_id}: {e}")
                return []
            if predicted is None:
                return []
            if isinstance(predicted, dict) and not isinstance(predicted, pl.Schema):
                named: dict[str, list[FlowfileColumn]] = {}
                for i, name in enumerate(resolved_output_names):
                    columns = self._predicted_value_to_columns(predicted.get(name))
                    if columns is None:
                        logger.warning(
                            f"predict_output_schema for node {node_id} missing or unsupported "
                            f"declared output '{name}'"
                        )
                        return []
                    named[output_handle(i)] = columns
                node._named_schemas = named
                return named.get(DEFAULT_OUTPUT_HANDLE, [])
            columns = self._predicted_value_to_columns(predicted)
            if columns is None:
                logger.warning(f"predict_output_schema for node {node_id} returned an unsupported value")
                return []
            if len(resolved_output_names) > 1:
                logger.warning(
                    f"predict_output_schema for node {node_id} returned a single frame but the node "
                    f"declares outputs {resolved_output_names}; falling back to execution-based prediction"
                )
                return []
            return columns

        return schema_callback

    def _make_local_user_defined_func(
        self,
        *,
        custom_node: CustomNodeBase,
        user_defined_node_settings: input_schema.UserDefinedNode,
        output_names: list[str] | None = None,
        registry_entry=None,
    ) -> Callable:
        """Create the execution function for a non-kernel custom node.

        Offloads process() to the worker (which owns dataset memory in a
        killable subprocess) whenever the flow doesn't run in local mode and
        the node came from the registry; otherwise runs in-process (the
        --run-flow / offload-disabled fallback, and inline test classes that
        have no source file on disk). Both read the settings when the node
        executes, so ``${name}`` references arrive resolved.
        """
        resolved_output_names = output_names or custom_node.output_names or ["main"]

        def _run_in_core(*flow_data_engine: FlowDataEngine) -> FlowDataEngine | None:
            instance = type(custom_node).from_settings(user_defined_node_settings.settings or {})
            user_id = user_defined_node_settings.user_id
            if user_id is not None:
                instance.set_execution_context(user_id)

            output = instance.process(*(fde.data_frame.lazy() for fde in flow_data_engine))

            accessed_secrets = instance.get_accessed_secrets()
            if accessed_secrets:
                logger.info(f"Node '{user_defined_node_settings.node_id}' accessed secrets: {accessed_secrets}")
            if isinstance(output, dict):
                node = self.get_node(user_defined_node_settings.node_id)
                primary = None
                for i, name in enumerate(resolved_output_names):
                    if name not in output:
                        raise ValueError(f"process() did not return declared output '{name}'")
                    fde = FlowDataEngine(output[name])
                    node._named_outputs[f"output-{i}"] = fde
                    if i == 0:
                        primary = fde
                return primary
            if isinstance(output, pl.LazyFrame | pl.DataFrame):
                return FlowDataEngine(output)
            return None

        def _func(*flow_data_engine: FlowDataEngine) -> FlowDataEngine | None:
            if self.execution_location == "local" or registry_entry is None or registry_entry.source_text is None:
                return _run_in_core(*flow_data_engine)

            node = self.get_node(user_defined_node_settings.node_id)
            request = CustomNodeExecuteInput(
                task_id=custom_node_task_id(node.hash),
                node_source=registry_entry.source_text,
                class_name=registry_entry.class_name,
                settings_values=user_defined_node_settings.settings or {},
                secrets=resolve_secret_payload(custom_node, user_defined_node_settings.user_id),
                inputs=[fde.data_frame.lazy().serialize() for fde in flow_data_engine],
                output_names=resolved_output_names,
                user_id=user_defined_node_settings.user_id,
                flowfile_flow_id=self.flow_id,
                flowfile_node_id=user_defined_node_settings.node_id,
            )
            fetcher = ExternalCustomNodeFetcher(request)
            node._fetch_cached_df = fetcher
            payload = fetcher.get_payload()

            primary: FlowDataEngine | None = None
            for i, name in enumerate(resolved_output_names):
                info = payload["outputs"].get(name)
                if info is None:
                    raise ValueError(f"Worker did not return declared output '{name}'")
                fde = FlowDataEngine(pl.scan_ipc(info["path"]), number_of_records=info["row_count"])
                node._named_outputs[f"output-{i}"] = fde
                if i == 0:
                    primary = fde
            return primary

        return _func

    def _execute_on_kernel(
        self,
        *,
        node_id: int,
        kernel_id: str,
        code: str,
        output_names: list[str],
        flow_data_engine: tuple[FlowDataEngine, ...],
        declared_publishes: list[str] | None = None,
        required_dependencies: list[str] | None = None,
        node_type: str | None = None,
    ) -> FlowDataEngine | None:
        """Execute code on a kernel container and return the primary output.

        Shared logic for both custom-node kernel execution and python_script nodes.
        Handles artifact context, directory setup, input writing, kernel execution,
        log forwarding, artifact recording, and output reading.
        """
        hold = self._kernel_hold or ambient_kernel_hold.get()
        if hold is not None:
            hold.check(self.flow_id, node_id, kernel_id)
        manager = root().get_kernel_manager()
        if required_dependencies:
            # Fail fast with a clear message instead of a ModuleNotFoundError
            # from inside process(). Only provable mismatches block; an unknown
            # kernel id falls through to execute_sync's own error path.
            kernel_info = manager.get_kernel_sync(kernel_id)
            if kernel_info is not None:
                missing = verify_kernel_for_node(kernel_info, required_dependencies)
                if missing:
                    raise KernelDependencyError(node_type or "", kernel_id, kernel_info.name, missing)
        flow_id = self.flow_id
        node_logger = self.flow_logger.get_node_logger(node_id)

        self.artifact_context.clear_nodes({node_id})

        available = self.artifact_context.compute_available(
            node_id=node_id,
            kernel_id=kernel_id,
            upstream_node_ids=self._get_upstream_node_ids(node_id),
        )

        shared_base = manager.shared_volume_path
        input_dir = os.path.join(shared_base, str(flow_id), str(node_id), "inputs")
        output_dir = os.path.join(shared_base, str(flow_id), str(node_id), "outputs")
        os.makedirs(input_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)
        clear_stale_parquets(input_dir)
        clear_stale_parquets(output_dir)

        node = self.get_node(node_id)
        input_names = self._resolve_input_names(node, len(flow_data_engine))
        input_paths = write_inputs_to_parquet(
            flow_data_engine,
            manager,
            input_dir,
            flow_id,
            node_id,
            input_names=input_names,
            local=self.execution_location == "local",
        )

        request = build_execute_request(
            node_id=node_id,
            code=code,
            input_paths=input_paths,
            output_dir=output_dir,
            flow_id=flow_id,
            manager=manager,
            source_registration_id=self._flow_settings.source_registration_id,
            available_artifacts={name: ref.source_node_id for name, ref in available.items()},
        )

        cancel_event = threading.Event()
        if node is not None:
            node._kernel_cancel_context = (kernel_id, manager, request.exec_token)
            node._kernel_cancel_event = cancel_event
        if self.flow_settings.is_canceled:
            # A cancel that landed before the event was registered could not set it.
            cancel_event.set()
        try:
            result = manager.execute_sync(kernel_id, request, self.flow_logger, cancel_event=cancel_event)
        finally:
            if node is not None:
                node._kernel_cancel_context = None
                node._kernel_cancel_event = None

        forward_kernel_logs(result, node_logger)
        if not result.success:
            raise RuntimeError(f"Kernel execution failed: {result.error}")

        if result.artifacts_published:
            self.artifact_context.record_published(
                node_id=node_id,
                kernel_id=kernel_id,
                artifacts=[a.model_dump() for a in result.artifacts_published],
            )
        if result.artifacts_deleted:
            self.artifact_context.record_deleted(
                node_id=node_id,
                kernel_id=kernel_id,
                artifact_names=result.artifacts_deleted,
            )

        if declared_publishes:
            observed_names = {a.name for a in result.artifacts_published}
            for name in declared_publishes:
                if name not in observed_names:
                    node_logger.warning(f"Declared artifact '{name}' (in publishes) was not published in this run.")

        primary_result = read_kernel_outputs(output_dir=output_dir, output_names=output_names, result=result, node=node)

        if primary_result is not None:
            return primary_result
        if not flow_data_engine:
            node_logger.warning(
                "Script published no outputs — call flowfile_ctx.publish_output(df, name=...) "
                "to return data; resulting in an empty table."
            )
        return flow_data_engine[0] if flow_data_engine else FlowDataEngine(pl.LazyFrame())

    def _make_kernel_user_defined_func(
        self,
        *,
        custom_node: CustomNodeBase,
        user_defined_node_settings: input_schema.UserDefinedNode,
        kernel_id: str,
        output_names: list[str],
        registry_entry=None,
    ) -> Callable:
        """Create the execution function for a kernel-executed custom node.

        Registry-backed nodes get an AST-generated script (JSON-baked settings,
        no return-rewriting), generated here so KernelCodegenError surfaces
        early and again at execution with resolved ``${name}`` settings.
        Inline test classes with no source file fall back to the deprecated
        ``generate_kernel_code`` so existing behavior survives.
        """

        def kernel_code(instance: CustomNodeBase) -> str:
            if registry_entry is not None and registry_entry.source_text and registry_entry.class_name:
                return generate_kernel_script(
                    node_source=registry_entry.source_text,
                    class_name=registry_entry.class_name,
                    settings_values=instance._extract_settings_values(),
                    output_names=output_names,
                    number_of_inputs=instance.number_of_inputs,
                )
            return instance.generate_kernel_code()

        kernel_code(custom_node)

        declared_publishes: list[str] | None = None
        if registry_entry is not None and registry_entry.manifest is not None:
            declared_publishes = [d.name for d in registry_entry.manifest.publishes]

        required_dependencies = list(custom_node.dependencies or [])
        node_type = custom_node.item

        def _func(*flow_data_engine: FlowDataEngine) -> FlowDataEngine | None:
            instance = type(custom_node).from_settings(user_defined_node_settings.settings or {})
            return self._execute_on_kernel(
                node_id=user_defined_node_settings.node_id,
                kernel_id=kernel_id,
                code=kernel_code(instance),
                output_names=output_names,
                flow_data_engine=flow_data_engine,
                declared_publishes=declared_publishes,
                required_dependencies=required_dependencies,
                node_type=node_type,
            )

        return _func
