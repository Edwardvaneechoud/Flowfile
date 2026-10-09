"""Machine-learning nodes: train, apply and evaluate a model."""

import os
from pathlib import Path
from typing import TYPE_CHECKING

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.configs import logger
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import (
    FlowDataEngine,
)
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_graph._base import GraphMixinBase
from flowfile_core.flowfile.flow_graph._root import root
from flowfile_core.flowfile.flow_graph.catalog_resolution import _authorize_catalog_write, _effective_namespace_id
from flowfile_core.flowfile.flow_graph.history import with_history_capture
from flowfile_core.flowfile.flow_node.flow_node import (
    FlowNode,
)
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.history_schema import (
    HistoryActionType,
)
from shared.storage_config import storage

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph.graph import FlowGraph


def ml_flow_model_path(flow_id: int, train_node_id: int | str) -> Path:
    """Path on the shared cache where a Train Model node writes its model JSON.

    Stable across runs (keyed off the train node's id), so an Apply Model node
    elsewhere in the flow can read the model without a catalog round-trip.
    """
    return storage.get_flow_cache_directory(flow_id) / "ml_models" / f"{train_node_id}.json"


class MlBuildersMixin(GraphMixinBase):
    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_train_model(self, train_settings: input_schema.NodeTrainModel) -> "FlowGraph":
        """Adds a Train Model node.

        Fits a regression model on the worker, stores the serialised artifact
        in the global catalog (via :class:`ArtifactService`), and passes the
        input data through unchanged so downstream nodes can keep transforming.

        Args:
            train_settings: Settings (model name, target/features, model_type, params).

        Returns:
            The :class:`FlowGraph` instance for chaining.
        """

        def _func(data: FlowDataEngine) -> FlowDataEngine:
            # Function-local: importing flowfile_core.artifacts at module top runs migrations (~3.5s).
            import shutil

            from flowfile_core.artifacts import get_storage_backend
            from flowfile_core.artifacts.service import ArtifactService
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

            # Flow-scoped model path keyed by node id; also the publish staging path, so we fit once.
            flow_path = ml_flow_model_path(self.flow_id, train_settings.node_id)
            flow_path.parent.mkdir(parents=True, exist_ok=True)

            prepared = None
            owner_id = train_settings.user_id or get_local_user_id() or 1
            staging_path = flow_path
            storage_backend = get_storage_backend()

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
                with get_db_context() as db:
                    prepared = ArtifactService(db, storage_backend).prepare_upload(prepare_request, owner_id=owner_id)
                if prepared.method != "file":
                    # v1 supports the shared-filesystem backend only; S3 needs a presigned-URL path on the worker.
                    with get_db_context() as db:
                        ArtifactService(db, storage_backend).delete_artifact(prepared.artifact_id)
                    raise ValueError(
                        "Train Model currently requires the filesystem artifact backend "
                        "(FLOWFILE_ARTIFACT_STORAGE=filesystem). S3 support is not implemented."
                    )
                # Train into the staging path and copy to flow_path after success so finalize_upload still works.
                staging_path = Path(prepared.path)

            node = self.get_node(node_id=train_settings.node_id)
            flow_path_written = False
            try:
                fetcher = root().MLTrainFetcher(
                    lf=data.data_frame,
                    staging_path=str(staging_path),
                    model_type=settings.model_type,
                    target_column=settings.target_column,
                    feature_columns=settings.feature_columns,
                    params=settings.params,
                    flow_id=self.flow_id,
                    node_id=train_settings.node_id,
                    file_ref=node.hash,
                    wait_on_completion=False,
                )
                node._fetch_cached_df = fetcher
                result = fetcher.get_result()
                if not isinstance(result, dict) or "sha256" not in result or "size_bytes" not in result:
                    raise RuntimeError(f"Worker did not return expected sha256/size_bytes payload, got: {result!r}")

                if prepared is not None:
                    # Atomically replace flow_path (.tmp + os.replace) before finalize_upload moves the staging file.
                    flow_tmp = flow_path.with_suffix(flow_path.suffix + ".tmp")
                    shutil.copyfile(staging_path, flow_tmp)
                    os.replace(flow_tmp, flow_path)
                    flow_path_written = True
                    with get_db_context() as db:
                        ArtifactService(db, storage_backend).finalize_upload(
                            artifact_id=prepared.artifact_id,
                            storage_key=prepared.storage_key,
                            sha256=result["sha256"],
                            size_bytes=result["size_bytes"],
                        )
            except Exception:
                if prepared is not None:
                    # Roll back the pending row on failure so no ghost artifact is shown.
                    with get_db_context() as db:
                        try:
                            ArtifactService(db, storage_backend).delete_artifact(prepared.artifact_id)
                        except Exception:
                            logger.exception("Failed to roll back pending artifact %s", prepared.artifact_id)
                    # Roll back the flow_path copy too, or the next Apply Model would use the deleted artifact.
                    if flow_path_written:
                        try:
                            flow_path.unlink(missing_ok=True)
                        except Exception:
                            logger.exception("Failed to roll back flow_path copy %s", flow_path)
                raise

            if prepared is not None:
                self.flow_logger.info(
                    f"Train Model: stored '{settings.model_name}' v{prepared.version} "
                    f"(artifact_id={prepared.artifact_id}, size={result['size_bytes']}B); "
                    f"flow copy at {flow_path}"
                )
                artifact_name = f"{settings.model_name} v{prepared.version}"
            else:
                self.flow_logger.info(f"Train Model: wrote {result['size_bytes']}B to flow path {flow_path}")
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

        Fetches the artifact from the catalog and asks the worker to score the
        input data, returning a LazyFrame with one extra ``Float64`` column.

        Args:
            apply_settings: Settings (model_name, optional version, output_column).

        Returns:
            The :class:`FlowGraph` instance for chaining.
        """

        def _func(data: FlowDataEngine) -> FlowDataEngine:
            from flowfile_core.artifacts import get_storage_backend
            from flowfile_core.artifacts.service import ArtifactService

            settings = apply_settings.apply_input
            if not settings.output_column:
                raise ValueError("Apply Model requires an 'output_column'.")

            model_path: str
            origin_label: str

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
                flow_path = ml_flow_model_path(self.flow_id, settings.upstream_node_id)
                if not flow_path.exists():
                    raise ValueError(
                        f"Apply Model: upstream Train Model (node {settings.upstream_node_id}) "
                        "has not produced a model yet. Make sure it runs before this node "
                        "(e.g. with a Wait For barrier)."
                    )
                model_path = str(flow_path)
                origin_label = f"upstream node {settings.upstream_node_id}"
            else:
                if not settings.model_name:
                    raise ValueError("Apply Model: 'model_name' is required when source='catalog'.")
                storage_backend = get_storage_backend()
                with get_db_context() as db:
                    effective_namespace_id = _effective_namespace_id(
                        CatalogService(SQLAlchemyCatalogRepository(db)), settings
                    )
                    artifact = ArtifactService(db, storage_backend).get_artifact_by_name(
                        name=settings.model_name,
                        namespace_id=effective_namespace_id,
                        version=settings.model_version,
                    )
                if artifact.download_source is None or artifact.download_source.method != "file":
                    raise ValueError(
                        "Apply Model currently requires the filesystem artifact backend "
                        "(FLOWFILE_ARTIFACT_STORAGE=filesystem). S3 support is not implemented."
                    )
                model_path = artifact.download_source.path
                if not os.path.exists(model_path):
                    raise ValueError(
                        f"Apply Model: data for catalog model '{settings.model_name}' "
                        f"v{artifact.version} (namespace {artifact.namespace_id}) is missing "
                        f"at {model_path}. If running in Docker, ensure the shared artifacts "
                        "volume is mounted into both core and the worker."
                    )
                origin_label = f"catalog '{settings.model_name}' v{artifact.version}"

            node = self.get_node(node_id=apply_settings.node_id)
            fetcher = root().MLApplyFetcher(
                lf=data.data_frame,
                model_path=model_path,
                output_column=settings.output_column,
                flow_id=self.flow_id,
                node_id=apply_settings.node_id,
                file_ref=node.hash,
                wait_on_completion=False,
            )
            node._fetch_cached_df = fetcher
            result_lf = fetcher.get_result()
            self.flow_logger.info(f"Apply Model: scored using {origin_label} -> column '{settings.output_column}'")
            return FlowDataEngine(result_lf)

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
