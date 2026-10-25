"""Machine-learning nodes: train, apply and evaluate a model."""

from typing import TYPE_CHECKING

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.execution.context import current_context
from flowfile_core.flowfile.execution.errors import ResultMissing
from flowfile_core.flowfile.execution.payloads import MlApplyPayload, MlTrainPayload, op_payload
from flowfile_core.flowfile.execution.protocol import OperationKind
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import (
    FlowDataEngine,
)
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_graph._base import GraphMixinBase
from flowfile_core.flowfile.flow_graph.catalog_resolution import _authorize_catalog_write, _effective_namespace_id
from flowfile_core.flowfile.flow_graph.history import with_history_capture
from flowfile_core.flowfile.flow_node.flow_node import (
    FlowNode,
)
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.history_schema import (
    HistoryActionType,
)

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph.graph import FlowGraph


class MlBuildersMixin(GraphMixinBase):
    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_train_model(self, train_settings: input_schema.NodeTrainModel) -> "FlowGraph":
        """Adds a Train Model node.

        Fits a model with the ``ml_train`` operation on the backend the flow
        runs on (core's own process for a local flow), and passes the input
        data through unchanged so downstream nodes can keep transforming. The
        backend holds the fitted model as a blob, which the node keeps as
        ``results.model_ref`` for an Apply Model node to score with by
        reference; a refit under another id releases the model it replaces,
        on the backend that holds it. With ``publish_to_catalog`` the held
        model is published to the global catalog with ``publish_result``,
        which has the backend copy it to the artifact store; the flow must be
        registered and the namespace writable, both checked before the fit.

        Args:
            train_settings: Settings (model name, target/features, model_type, params).

        Returns:
            The :class:`FlowGraph` instance for chaining.
        """

        def _func(data: FlowDataEngine) -> FlowDataEngine:
            # Function-local: importing flowfile_core.artifacts at module top runs migrations (~3.5s).
            from flowfile_core.artifacts.publish import publish_result
            from flowfile_core.auth.utils import get_local_user_id
            from flowfile_core.flowfile.catalog_helpers import (
                auto_register_flow,
                resolve_source_registration_id,
            )
            from flowfile_core.schemas.artifact_schema import PrepareUploadRequest
            from shared.ml.trainers import get_trainer

            settings = train_settings.train_input
            if not settings.target_column:
                raise ValueError("Train Model requires a 'target_column'.")
            if not settings.feature_columns:
                raise ValueError("Train Model requires at least one 'feature_columns' entry.")
            if settings.publish_to_catalog and not settings.model_name:
                raise ValueError("Train Model: 'model_name' is required when 'publish_to_catalog' is enabled.")

            # Validate early so the user sees a core error, not a worker stack trace.
            trainer = get_trainer(settings.model_type)
            try:
                trainer.params_class(**settings.params)
            except Exception as e:
                raise ValueError(f"Train Model: invalid params for model_type={settings.model_type!r}: {e}") from e

            prepare_request = None
            owner_id = train_settings.user_id or get_local_user_id() or 1

            if settings.publish_to_catalog:
                # Auto-register an unregistered on-disk flow (as open/save do) so artifacts get a stable lineage.
                registration_id = self._flow_settings.source_registration_id
                if registration_id is None and self._flow_settings.path:
                    auto_register_flow(
                        self._flow_settings.path,
                        self._flow_settings.name or "",
                        owner_id,
                    )
                    resolve_source_registration_id(self)
                    registration_id = self._flow_settings.source_registration_id
                if registration_id is None:
                    raise ValueError(
                        "Publishing to catalog requires the flow to be registered. "
                        "Save the flow first, or disable 'Publish to catalog'."
                    )

                tags = list({"ml", trainer.task_type, settings.model_type, *settings.catalog_tags})
                with get_db_context() as _ns_db:
                    effective_namespace_id = _effective_namespace_id(
                        CatalogService(SQLAlchemyCatalogRepository(_ns_db)), settings
                    )
                    # A published model is a catalog artifact: gate the namespace like the catalog writer.
                    _authorize_catalog_write(
                        _ns_db, train_settings.user_id, existing=None, namespace_id=effective_namespace_id
                    )
                prepare_request = PrepareUploadRequest(
                    name=settings.model_name,
                    source_registration_id=registration_id,
                    namespace_id=effective_namespace_id,
                    serialization_format=trainer.serialization_format,
                    description=settings.catalog_description
                    or f"Trained via Flowfile node {train_settings.node_id} ({settings.model_type})",
                    tags=tags,
                    source_flow_id=self.flow_id,
                    source_node_id=train_settings.node_id,
                    python_type=f"flowfile.ml.{settings.model_type}",
                    python_module="flowfile.ml",
                )

            node = self.get_node(node_id=train_settings.node_id)
            train = MlTrainPayload(
                plan=data.data_frame.lazy(),
                model_type=settings.model_type,
                target_column=settings.target_column,
                feature_columns=settings.feature_columns,
                params=settings.params,
                flow_id=self.flow_id,
                node_id=train_settings.node_id,
                job_id=node.hash,
            )
            backend = current_context(self).backend
            trained = node.run_op(backend, OperationKind.ml_train, op_payload(train), job_id=train.job_id)
            result = trained.metadata
            model = trained.refs.get("model")
            if model is None or "sha256" not in result or "size_bytes" not in result:
                raise RuntimeError(
                    f"The {backend.id!r} backend did not return the model with its sha256/size_bytes payload, got "
                    f"the outputs {sorted(trained.refs)} and {result!r}"
                )
            # run_op released the model this node fitted before; one carried onto it is the source node's to release.
            node.results.model_ref = model

            if prepare_request is not None:
                published = publish_result(
                    backend,
                    model,
                    prepare_request,
                    owner_id=owner_id,
                    sha256=result["sha256"],
                    size_bytes=result["size_bytes"],
                )
                self.flow_logger.info(
                    f"Train Model: stored '{settings.model_name}' v{published.version} "
                    f"(artifact_id={published.artifact_id}, size={result['size_bytes']}B)"
                )
                artifact_name = f"{settings.model_name} v{published.version}"
            else:
                self.flow_logger.info(f"Train Model: fitted a {result['size_bytes']}B model, held with the node")
                artifact_name = f"{settings.model_type} (flow only)"

            # Surface the model in the Artifacts tab; re-runs replace the prior entry.
            self.artifact_context.clear_nodes({train_settings.node_id})
            self.artifact_context.record_published(
                node_id=train_settings.node_id,
                kernel_id="",
                artifacts=[
                    {
                        "name": artifact_name,
                        "type_name": f"flowfile.ml.{settings.model_type}",
                        "module": "flowfile.ml",
                        "size_bytes": result["size_bytes"],
                    }
                ],
            )
            return data

        def schema_callback():
            input_node: FlowNode = self.get_node(train_settings.node_id).node_inputs.main_inputs[0]
            return input_node.schema

        depending_on_id = train_settings.depending_on_id if hasattr(train_settings, "depending_on_id") else None
        self.add_node_step(
            node_id=train_settings.node_id,
            function=_func,
            input_columns=[],
            node_type="train_model",
            setting_input=train_settings,
            schema_callback=schema_callback,
            input_node_ids=[depending_on_id] if depending_on_id is not None else None,
        )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_apply_model(self, apply_settings: input_schema.NodeApplyModel) -> "FlowGraph":
        """Adds an Apply Model node.

        Scores the input data with the ``ml_apply`` operation on the backend
        the flow runs on (core's own process for a local flow), returning a
        LazyFrame with one extra prediction column. The model is sent by
        reference: the one the upstream Train Model node holds, or a catalog
        artifact fetched onto that backend with ``fetch_artifact`` for this
        call and released after it. A model the upstream node fitted that is
        no longer held fails the node, asking for that Train Model node to run
        again. The scored rows are the node's result where they were scored: a
        stored result stays held, and a deferred one is released once the node
        holds its plan.

        Args:
            apply_settings: Settings (model_name, optional version, output_column).

        Returns:
            The :class:`FlowGraph` instance for chaining.
        """

        def _func(data: FlowDataEngine) -> FlowDataEngine:
            from flowfile_core.artifacts.publish import fetch_artifact

            settings = apply_settings.apply_input
            if not settings.output_column:
                raise ValueError("Apply Model requires an 'output_column'.")

            backend = current_context(self).backend
            fetched = None
            if settings.source == "upstream":
                if settings.upstream_node_id is None:
                    raise ValueError(
                        "Apply Model: 'upstream_node_id' is required when source='upstream'. "
                        "Pick a Train Model node in the drawer or switch to 'catalog' source."
                    )
                upstream = self.get_node(node_id=settings.upstream_node_id)
                if upstream is None or upstream.node_type != "train_model":
                    raise ValueError(
                        f"Apply Model: upstream node {settings.upstream_node_id} is not a Train Model node."
                    )
                model = upstream.results.model_ref
                if model is None:
                    raise ValueError(
                        f"Apply Model: upstream Train Model (node {settings.upstream_node_id}) "
                        "has not produced a model yet. Make sure it runs before this node "
                        "(e.g. with a Wait For barrier)."
                    )
                origin_label = f"upstream node {settings.upstream_node_id}"
            else:
                if not settings.model_name:
                    raise ValueError("Apply Model: 'model_name' is required when source='catalog'.")
                with get_db_context() as db:
                    effective_namespace_id = _effective_namespace_id(
                        CatalogService(SQLAlchemyCatalogRepository(db)), settings
                    )
                model = fetched = fetch_artifact(
                    backend, settings.model_name, settings.model_version, effective_namespace_id
                )
                version = "latest" if settings.model_version is None else f"v{settings.model_version}"
                origin_label = f"catalog '{settings.model_name}' ({version})"

            node = self.get_node(node_id=apply_settings.node_id)
            apply = MlApplyPayload(
                plan=data.data_frame.lazy(),
                model=model,
                output_column=settings.output_column,
                flow_id=self.flow_id,
                node_id=apply_settings.node_id,
                job_id=node.hash,
            )
            try:
                applied = node.run_op(backend, OperationKind.ml_apply, op_payload(apply), job_id=apply.job_id)
                ref = applied.refs["result"]
            except ResultMissing as e:
                if fetched is not None:
                    raise
                raise ValueError(
                    f"Apply Model: the model upstream Train Model (node {settings.upstream_node_id}) fitted is no "
                    f"longer held. Run the Train Model node {settings.upstream_node_id} again."
                ) from e
            finally:
                if fetched is not None:
                    backend.release(fetched)
            self.flow_logger.info(f"Apply Model: scored using {origin_label} -> column '{settings.output_column}'")
            scored = FlowDataEngine(backend.plan_for(ref), schema=ref.schema, number_of_records=ref.row_count)
            if ref.state == "deferred":
                backend.release(ref)
            return scored

        def schema_callback():
            input_node: FlowNode = self.get_node(apply_settings.node_id).node_inputs.main_inputs[0]
            input_schema_cols = list(input_node.schema)
            s = apply_settings.apply_input
            output_column = s.output_column or "prediction"
            # source='upstream' reads the trainer's output_dtype; 'catalog' falls back to Float64.
            output_dtype = "Float64"
            if s.source == "upstream" and s.upstream_node_id is not None:
                upstream = self.get_node(s.upstream_node_id)
                train_input = getattr(getattr(upstream, "setting_input", None), "train_input", None)
                model_type = getattr(train_input, "model_type", None)
                if model_type:
                    try:
                        from shared.ml.trainers import get_trainer

                        output_dtype = get_trainer(model_type).output_dtype
                    except ValueError:
                        pass
            return input_schema_cols + [FlowfileColumn.from_input(output_column, output_dtype)]

        depending_on_id = apply_settings.depending_on_id if hasattr(apply_settings, "depending_on_id") else None
        self.add_node_step(
            node_id=apply_settings.node_id,
            function=_func,
            input_columns=[],
            node_type="apply_model",
            setting_input=apply_settings,
            schema_callback=schema_callback,
            input_node_ids=[depending_on_id] if depending_on_id is not None else None,
        )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_evaluate_model(self, evaluate_settings: input_schema.NodeEvaluateModel) -> "FlowGraph":
        """Adds an Evaluate Model node.

        Compares the *actual* and *predicted* columns already present on the
        input dataframe and emits a long-form ``(metric, value)`` frame.
        Pure polars — no worker offload, no model file read.

        ``task_type="auto"`` resolves the metric set from the configured
        upstream Train Model node's trainer; otherwise uses the explicit
        ``regression`` / ``classification`` choice from settings.
        """

        def _resolve_task_type() -> str:
            s = evaluate_settings.evaluate_input
            if s.task_type != "auto":
                return s.task_type
            if s.upstream_train_node_id is not None:
                upstream = self.get_node(s.upstream_train_node_id)
                train_input = getattr(getattr(upstream, "setting_input", None), "train_input", None)
                model_type = getattr(train_input, "model_type", None)
                if model_type:
                    try:
                        from shared.ml.trainers import get_trainer

                        return get_trainer(model_type).task_type
                    except ValueError:
                        pass
            return "regression"

        def _func(data: FlowDataEngine) -> FlowDataEngine:
            from shared.ml.metrics import compute_metrics

            settings = evaluate_settings.evaluate_input
            if not settings.actual_column:
                raise ValueError("Evaluate Model requires an 'actual_column'.")
            if not settings.predicted_column:
                raise ValueError("Evaluate Model requires a 'predicted_column'.")

            task_type = _resolve_task_type()
            metrics_lf = compute_metrics(
                data.data_frame,
                actual_column=settings.actual_column,
                predicted_column=settings.predicted_column,
                task_type=task_type,
            )
            self.flow_logger.info(
                f"Evaluate Model: {settings.predicted_column} vs {settings.actual_column} " f"(task_type={task_type})"
            )
            return FlowDataEngine(metrics_lf)

        def schema_callback():
            return [
                FlowfileColumn.from_input(column_name="metric", data_type="String"),
                FlowfileColumn.from_input(column_name="value", data_type="Float64"),
            ]

        depending_on_id = evaluate_settings.depending_on_id if hasattr(evaluate_settings, "depending_on_id") else None
        self.add_node_step(
            node_id=evaluate_settings.node_id,
            function=_func,
            input_columns=[],
            node_type="evaluate_model",
            setting_input=evaluate_settings,
            schema_callback=schema_callback,
            input_node_ids=[depending_on_id] if depending_on_id is not None else None,
        )
        return self
