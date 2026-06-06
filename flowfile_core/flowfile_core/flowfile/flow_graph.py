import datetime
import functools
import io as _io
import json
import os
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from functools import partial
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from time import time
from typing import Any, Literal, NamedTuple, Union
from uuid import uuid1

import fastexcel
import polars as pl
import yaml
from fastapi.exceptions import HTTPException
from pyarrow.parquet import ParquetFile

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.delta_utils import check_source_versions_current, delete_table_storage, is_delta_table
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.configs import logger
from flowfile_core.configs.app_settings import get_google_oauth_config
from flowfile_core.configs.flow_logger import FlowLogger, NodeLogger
from flowfile_core.configs.node_store import CUSTOM_NODE_STORE
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.analytics.utils import create_graphic_walker_node_from_node_promise
from flowfile_core.flowfile.artifacts import ArtifactContext
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    get_local_cloud_connection,
    get_local_database_connection,
)
from flowfile_core.flowfile.database_connection_manager.ga_connections import (
    get_encrypted_credential,
    get_ga_connection,
)
from flowfile_core.flowfile.filter_expressions import build_filter_expression
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import (
    FlowDataEngine,
    execute_polars_code,
    execute_sql_query,
)
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn, cast_str_to_polars_type
from flowfile_core.flowfile.flow_data_engine.polars_code_parser import polars_code_parser
from flowfile_core.flowfile.flow_data_engine.read_excel_tables import (
    get_calamine_xlsx_data_types,
    get_open_xlsx_datatypes,
)
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
    ExternalCloudWriter,
    ExternalDatabaseFetcher,
    ExternalDatabaseWriter,
    ExternalDfFetcher,
    ExternalGoogleAnalyticsFetcher,
    ExternalKafkaFetcher,
    ExternalOutputWriter,
    ExternalRestApiFetcher,
    MLApplyFetcher,
    MLTrainFetcher,
    fetch_kafka_offsets,
)
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.flow_node.schema_utils import create_schema_callback_with_output_config
from flowfile_core.flowfile.graph_tree.graph_tree import (
    add_un_drawn_nodes,
    build_flow_paths,
    build_node_info,
    calculate_depth,
    define_node_connections,
    draw_merged_paths,
    draw_standalone_paths,
    group_nodes_by_depth,
)
from flowfile_core.flowfile.node_designer.custom_node import CustomNodeBase
from flowfile_core.flowfile.parameter_resolver import (
    apply_parameters_in_place,
    find_unresolved_in_model,
    restore_parameters,
)
from flowfile_core.flowfile.schema_callbacks import (
    calculate_cross_join_schema,
    calculate_fuzzy_match_schema,
    calculate_join_schema,
    pre_calculate_pivot_schema,
)
from flowfile_core.flowfile.sources import external_sources
from flowfile_core.flowfile.sources.external_sources.factory import data_source_factory
from flowfile_core.flowfile.sources.external_sources.google_analytics_source import derive_schema
from flowfile_core.flowfile.sources.external_sources.rest_api_source import (
    build_rest_api_worker_settings,
    resolve_auth_secret_encrypted,
)
from flowfile_core.flowfile.sources.external_sources.sql_source import models as sql_models
from flowfile_core.flowfile.sources.external_sources.sql_source import utils as sql_utils
from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import (
    BaseSqlSource,
    SqlSource,
    validate_sql_query,
)
from flowfile_core.flowfile.util.calculate_layout import calculate_layered_layout
from flowfile_core.flowfile.util.execution_orderer import ExecutionPlan, ExecutionStage, compute_execution_plan
from flowfile_core.flowfile.utils import snake_case_to_camel_case
from flowfile_core.kafka.connection_manager import (
    build_consumer_config,
    get_kafka_connection,
    get_kafka_connection_by_name,
)
from flowfile_core.kernel import get_kernel_manager
from flowfile_core.kernel.execution import (
    build_execute_request,
    forward_kernel_logs,
    read_kernel_outputs,
    write_inputs_to_parquet,
)
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.catalog_schema import TableWriteMetadata
from flowfile_core.schemas.cloud_storage_schemas import (
    AuthMethod,
    CloudStorageReadSettingsInternal,
    CloudStorageWriteSettingsInternal,
    FullCloudStorageConnection,
    get_cloud_storage_write_settings_worker_interface,
)
from flowfile_core.schemas.history_schema import HistoryActionType, HistoryState, UndoRedoResult
from flowfile_core.schemas.output_model import NodeData, NodeResult, RunInformation
from flowfile_core.schemas.transform_schema import CrossJoinInputManager, FuzzyMatchInputManager, JoinInputManager
from flowfile_core.secret_manager.secret_manager import (
    _encrypt_with_master_key,
    decrypt_secret,
    get_encrypted_secret,
)
from flowfile_core.utils.arrow_reader import get_read_top_n
from shared.delta_utils import get_delta_size_bytes, merge_into_delta
from shared.delta_utils import write_delta as _write_delta
from shared.google_analytics.models import (
    GoogleAnalyticsFilter as WorkerGoogleAnalyticsFilter,
)
from shared.google_analytics.models import (
    GoogleAnalyticsOrderBy as WorkerGoogleAnalyticsOrderBy,
)
from shared.google_analytics.models import (
    GoogleAnalyticsReadSettings as WorkerGoogleAnalyticsReadSettings,
)
from shared.kafka.consumer import infer_topic_schema, make_kafka_commit_callback, read_kafka_source
from shared.kafka.models import KafkaReadSettings
from shared.storage_config import storage

try:
    __version__ = version("Flowfile")
except PackageNotFoundError:
    __version__ = "0.11.2"


def represent_list_json(dumper, data):
    """Use inline style for short simple lists, block style for complex ones."""
    if len(data) <= 10 and all(isinstance(item, int | str | float | bool | type(None)) for item in data):
        return dumper.represent_sequence("tag:yaml.org,2002:seq", data, flow_style=True)
    return dumper.represent_sequence("tag:yaml.org,2002:seq", data, flow_style=False)


yaml.add_representer(list, represent_list_json)


def with_history_capture(action_type: "HistoryActionType", description_template: str = "Update {node_type} settings"):
    """Decorator to automatically capture history for FlowGraph methods.

    Wraps a method to capture state before execution and record history
    only if the state actually changed. Respects the flow's track_history setting.

    Args:
        action_type: The type of history action (e.g., HistoryActionType.UPDATE_SETTINGS).
        description_template: Template string for the history description.
            Can use {node_type} placeholder which will be replaced with the actual node type.

    Example:
        @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
        def add_filter(self, filter_settings: input_schema.NodeFilter):
            # ... implementation
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self: "FlowGraph", *args, **kwargs):
            settings_input = args[0] if args else next(iter(kwargs.values()), None)

            # Remember the session owner so restore_from_snapshot can re-stamp
            # user_id even when the live graph holds no nodes (every editor
            # mutation funnels through this decorator with a stamped user_id).
            owner_uid = getattr(settings_input, "user_id", None) if settings_input else None
            if owner_uid is not None:
                self._owner_user_id = owner_uid

            if not self.flow_settings.track_history:
                return func(self, *args, **kwargs)

            node_id = getattr(settings_input, "node_id", None) if settings_input else None
            node_type = (
                getattr(settings_input, "node_type", func.__name__.replace("add_", ""))
                if settings_input
                else func.__name__.replace("add_", "")
            )

            pre_snapshot = self.get_flowfile_data()

            result = func(self, *args, **kwargs)

            self._history_manager.capture_if_changed(
                self, pre_snapshot, action_type, description_template.format(node_type=node_type), node_id
            )
            return result

        return wrapper

    return decorator


def get_xlsx_schema(
    engine: str,
    file_path: str,
    sheet_name: str,
    start_row: int,
    start_column: int,
    end_row: int,
    end_column: int,
    has_headers: bool,
):
    """Calculates the schema of an XLSX file by reading a sample of rows.

    Args:
        engine: The engine to use for reading ('openpyxl' or 'calamine').
        file_path: The path to the XLSX file.
        sheet_name: The name of the sheet to read.
        start_row: The starting row for data reading.
        start_column: The starting column for data reading.
        end_row: The ending row for data reading.
        end_column: The ending column for data reading.
        has_headers: A boolean indicating if the file has a header row.

    Returns:
        A list of FlowfileColumn objects representing the schema.
    """
    try:
        logger.info("Starting to calculate the schema")
        if engine == "openpyxl":
            max_col = end_column if end_column > 0 else None
            return get_open_xlsx_datatypes(
                file_path=file_path,
                sheet_name=sheet_name,
                min_row=start_row + 1,
                min_col=start_column + 1,
                max_row=100,
                max_col=max_col,
                has_headers=has_headers,
            )
        elif engine == "calamine":
            return get_calamine_xlsx_data_types(
                file_path=file_path, sheet_name=sheet_name, start_row=start_row, end_row=end_row
            )
        logger.info("done calculating the schema")
    except Exception as e:
        logger.error(e)
        return []


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


def get_xlsx_schema_callback(
    engine: str,
    file_path: str,
    sheet_name: str,
    start_row: int,
    start_column: int,
    end_row: int,
    end_column: int,
    has_headers: bool,
):
    """Creates a partially applied function for lazy calculation of an XLSX schema.

    Args:
        engine: The engine to use for reading.
        file_path: The path to the XLSX file.
        sheet_name: The name of the sheet.
        start_row: The starting row.
        start_column: The starting column.
        end_row: The ending row.
        end_column: The ending column.
        has_headers: A boolean indicating if the file has headers.

    Returns:
        A callable function that, when called, will execute `get_xlsx_schema`.
    """
    return partial(
        get_xlsx_schema,
        engine=engine,
        file_path=file_path,
        sheet_name=sheet_name,
        start_row=start_row,
        start_column=start_column,
        end_row=end_row,
        end_column=end_column,
        has_headers=has_headers,
    )


def get_cloud_connection_settings(
    connection_name: str, user_id: int, auth_mode: AuthMethod
) -> FullCloudStorageConnection:
    """Retrieves cloud storage connection settings, falling back to environment variables if needed.

    Args:
        connection_name: The name of the saved connection.
        user_id: The ID of the user owning the connection.
        auth_mode: The authentication method specified by the user.

    Returns:
        A FullCloudStorageConnection object with the connection details.

    Raises:
        HTTPException: If the connection settings cannot be found.
    """
    cloud_connection_settings = get_local_cloud_connection(connection_name, user_id)
    if cloud_connection_settings is None and auth_mode in ("env_vars", transform_schema.AUTO_DATA_TYPE):
        # If the auth mode is aws-cli, we do not need connection settings
        cloud_connection_settings = FullCloudStorageConnection(storage_type="s3", auth_method="env_vars")
    elif cloud_connection_settings is None and auth_mode == "aws-cli":
        cloud_connection_settings = FullCloudStorageConnection(storage_type="s3", auth_method="aws-cli")
    if cloud_connection_settings is None:
        raise HTTPException(status_code=400, detail="Cloud connection settings not found")
    return cloud_connection_settings


# Catalog writer/reader helpers (extracted for testability)


def _resolve_virtual_table(
    is_optimized: bool,
    serialized_lf: bytes | None,
    catalog_table_id: int,
    run_location: Literal["remote", "local"] | None = None,
    node_logger: NodeLogger = None,
    source_table_versions: str | None = None,
) -> pl.LazyFrame:
    """Resolve a virtual table to a LazyFrame.

    Optimized tables deserialize a stored execution plan if source table
    versions are still current; otherwise falls back to re-executing
    the producer flow via CatalogService.
    """
    if is_optimized and serialized_lf and check_source_versions_current(source_table_versions):
        return pl.LazyFrame.deserialize(_io.BytesIO(serialized_lf))
    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        svc = CatalogService(repo)
        return svc.resolve_virtual_flow_table(catalog_table_id, run_location=run_location, node_logger=node_logger)


class CatalogSqlTables(NamedTuple):
    """Resolved catalog tables for SQL execution."""

    table_paths: dict[str, str]
    virtual_tables: dict[str, tuple[bool, bytes | None, int, str | None]]


def ml_flow_model_path(flow_id: int, train_node_id: int | str) -> Path:
    """Path on the shared cache where a Train Model node writes its model JSON.

    Stable across runs (keyed off the train node's id), so an Apply Model node
    elsewhere in the flow can read the model without a catalog round-trip.
    """
    return storage.get_flow_cache_directory(flow_id) / "ml_models" / f"{train_node_id}.json"


def _resolve_catalog_sql_tables(node_id: int | str) -> CatalogSqlTables:
    """Resolve all catalog tables (physical Delta + virtual) for a SQL query node."""
    table_paths: dict[str, str] = {}
    virtual_tables: dict[str, tuple[bool, bytes | None, int, str | None]] = {}
    try:
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            seen_names: set[str] = set()
            for t in repo.list_tables():
                if t.name in seen_names:
                    logger.warning(
                        "Duplicate table name %r in catalog SQL context for node %s; "
                        "later entry will overwrite the earlier one",
                        t.name,
                        node_id,
                    )
                seen_names.add(t.name)
                if t.table_type == "virtual":
                    virtual_tables[t.name] = (
                        t.is_optimized,
                        t.serialized_lazy_frame,
                        t.id,
                        t.source_table_versions,
                    )
                elif t.file_path and is_delta_table(Path(t.file_path)):
                    table_paths[t.name] = t.file_path

    except Exception:
        logger.warning(
            "Could not resolve catalog tables for SQL node %s",
            node_id,
            exc_info=True,
        )
    return CatalogSqlTables(table_paths=table_paths, virtual_tables=virtual_tables)


class CatalogTableInfo(NamedTuple):
    """Resolved catalog table info for a single-table reader."""

    file_path: str | None
    table_type: str
    serialized_lf: bytes | None
    is_optimized: bool
    source_table_versions: str | None = None


def _resolve_catalog_table_info(node_catalog_reader: "input_schema.NodeCatalogReader") -> CatalogTableInfo:
    """Resolve a single catalog table (physical or virtual) for a table reader node."""
    file_path: str | None = None
    table_type: str = "physical"
    serialized_lf: bytes | None = None
    is_optimized: bool = False
    source_table_versions: str | None = None
    try:
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)

            table_record = None
            if node_catalog_reader.catalog_table_id:
                table_record = repo.get_table(node_catalog_reader.catalog_table_id)
            else:
                reference = node_catalog_reader.catalog_full_table_name or node_catalog_reader.catalog_table_name
                if reference:
                    try:
                        table_record = svc.resolve_table(
                            reference,
                            default_namespace_id=node_catalog_reader.catalog_namespace_id,
                        )
                    except Exception:
                        logger.warning(
                            "Could not resolve catalog table reference %r (ns=%s) for node %s",
                            reference,
                            node_catalog_reader.catalog_namespace_id,
                            node_catalog_reader.node_id,
                            exc_info=True,
                        )
            if table_record is not None:
                table_type = table_record.table_type
                if table_type == "virtual":
                    is_optimized = table_record.is_optimized
                    serialized_lf = table_record.serialized_lazy_frame
                    source_table_versions = table_record.source_table_versions
                else:
                    file_path = table_record.file_path
            else:
                file_path = svc.resolve_table_file_path(
                    table_id=node_catalog_reader.catalog_table_id,
                    table_name=node_catalog_reader.catalog_table_name,
                    namespace_id=node_catalog_reader.catalog_namespace_id,
                )
    except Exception:
        logger.warning("Could not resolve catalog table for node %s", node_catalog_reader.node_id, exc_info=True)
    return CatalogTableInfo(
        file_path=file_path,
        table_type=table_type,
        serialized_lf=serialized_lf,
        is_optimized=is_optimized,
        source_table_versions=source_table_versions,
    )


def _write_catalog_delta_local(
    df: FlowDataEngine,
    dest_path: Path,
    delta_mode: str,
    merge_keys: list[str] | None,
) -> TableWriteMetadata | None:
    """Write a Delta table locally. Returns metadata dict, or ``None`` when the write was skipped."""
    dest = str(dest_path)
    if delta_mode in ("upsert", "update", "delete"):
        wrote = merge_into_delta(df.data_frame.collect(), dest, merge_mode=delta_mode, merge_keys=merge_keys)
    else:
        wrote = _write_delta(df.data_frame, dest, mode=delta_mode)
    if not wrote:
        return None
    return {
        "schema": [{"name": c.column_name, "dtype": c.data_type} for c in df.schema],
        "row_count": df.count(),
        "column_count": df.number_of_fields,
        "size_bytes": get_delta_size_bytes(dest_path),
    }


def _write_catalog_delta_remote(
    flow_id: int,
    node: FlowNode,
    df: FlowDataEngine,
    op_type: str,
    op_kwargs: dict,
    table_name: str,
) -> TableWriteMetadata | None:
    """Write a Delta table via the worker service. Returns metadata dict, or ``None`` when skipped."""
    fetcher = ExternalDfFetcher(
        flow_id=flow_id,
        node_id=node.node_id,
        lf=df.data_frame,
        wait_on_completion=False,
        operation_type=op_type,
        kwargs=op_kwargs,
    )
    node._fetch_cached_df = fetcher
    try:
        result = fetcher.get_result()
    except Exception as e:
        raise RuntimeError(f"Worker failed to write delta table '{table_name}': {e}") from e
    if isinstance(result, dict) and result.get("skipped"):
        return None
    meta: TableWriteMetadata = {}
    if isinstance(result, dict):
        meta = {k: result.get(k) for k in ("schema", "row_count", "column_count", "size_bytes")}
    return meta


def _register_catalog_table(
    existing,
    dest_path: Path,
    settings: input_schema.CatalogWriteSettings,
    source_registration_id: int | None,
    user_id: int,
    meta_kwargs: TableWriteMetadata,
) -> None:
    """Register or update the catalog table entry, cleaning up orphaned storage on failure for new tables."""
    try:
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            if existing is not None:
                svc.overwrite_table_data(
                    table_id=existing.id,
                    table_path=str(dest_path),
                    source_registration_id=source_registration_id,
                    description=settings.description,
                    storage_format="delta",
                    **meta_kwargs,
                )
            else:
                svc.register_table_from_data(
                    name=settings.table_name,
                    table_path=str(dest_path),
                    owner_id=user_id,
                    namespace_id=settings.namespace_id,
                    description=settings.description,
                    source_registration_id=source_registration_id,
                    storage_format="delta",
                    **meta_kwargs,
                )
    except Exception:
        if existing is None and dest_path.exists():
            try:
                delete_table_storage(dest_path)
            except OSError:
                logger.warning("Failed to clean up orphan table %s", dest_path, exc_info=True)
        raise


def _collect_source_table_versions(graph: "FlowGraph") -> str | None:
    """Collect delta versions of upstream physical catalog tables used by this flow.

    For each catalog_reader node that reads a physical delta table, records
    the current delta version. For optimized virtual table sources, includes
    their transitive source_table_versions.

    Returns a JSON string of SourceTableVersion entries, or None if no sources found.
    """
    from deltalake import DeltaTable as _DeltaTable

    from shared.delta_models import SourceTableVersion

    versions: list[SourceTableVersion] = []
    seen_table_ids: set[int] = set()

    table_ids: list[int] = []
    for node in graph.nodes:
        if node.node_type != "catalog_reader":
            continue
        setting = node.setting_input
        table_id = getattr(setting, "catalog_table_id", None)
        if not table_id or table_id in seen_table_ids:
            continue
        seen_table_ids.add(table_id)
        table_ids.append(table_id)

    # Single DB session for all lookups
    try:
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            for table_id in table_ids:
                table_record = repo.get_table(table_id)
                if table_record is None:
                    continue
                if table_record.table_type == "virtual":
                    # Include transitive versions from optimized virtual sources
                    if table_record.source_table_versions:
                        existing = json.loads(table_record.source_table_versions)
                        for entry in existing:
                            sv = SourceTableVersion(**entry)
                            if sv.table_id not in seen_table_ids:
                                seen_table_ids.add(sv.table_id)
                                versions.append(sv)
                elif table_record.file_path and is_delta_table(table_record.file_path):
                    try:
                        current_version = _DeltaTable(table_record.file_path, without_files=True).version()
                        versions.append(
                            SourceTableVersion(
                                table_id=table_id,
                                file_path=table_record.file_path,
                                version=current_version,
                            )
                        )
                    except Exception:
                        logger.warning("Could not read delta version for source table %d", table_id, exc_info=True)
    except Exception:
        logger.warning("Could not collect source table versions", exc_info=True)

    if not versions:
        return None
    return json.dumps([v.model_dump() for v in versions])


def _handle_virtual_table_write(
    graph: "FlowGraph",
    node_catalog_writer: input_schema.NodeCatalogWriter,
    df: FlowDataEngine,
) -> FlowDataEngine:
    """Handle virtual-mode catalog write: register a virtual table without materializing data."""
    settings = node_catalog_writer.catalog_write_settings
    reg_id = graph._flow_settings.source_registration_id
    if not reg_id:
        # Python-built flows have no catalog registration on creation. Try to
        # auto-register under "General > Python Editor" so the user does not
        # need to manually save+register before calling write_mode='virtual'.
        try:
            from flowfile_core.flowfile.catalog_helpers import register_python_editor_flow

            reg_id = register_python_editor_flow(
                graph,
                user_id=node_catalog_writer.user_id,
            )
        except Exception:
            import traceback

            graph.flow_logger.warning(f"Auto-registration for virtual catalog write failed:\n{traceback.format_exc()}")
            reg_id = None
    if not reg_id:
        raise ValueError(
            "Cannot create a virtual table: this flow is not linked to a catalog registration. "
            "Open the flow from the catalog, or register it first via "
            "flowfile_frame.register_flow_with_catalog(...)."
        )

    serialized_lf: bytes | None = None
    polars_plan: str | None = None
    source_table_versions: str | None = None
    changed_execution_mode = False

    writer_node = graph.get_node(node_catalog_writer.node_id)
    is_lazy, _reasons = writer_node.check_upstream_laziness()
    if is_lazy:
        if graph.execution_mode != "performance":
            graph.execution_mode = "performance"
            graph.reset()
            changed_execution_mode = True
            incoming_node = graph.get_node(node_catalog_writer.node_id).node_inputs.main_inputs[0]
            df = incoming_node.get_resulting_data()
        polars_plan = df.data_frame.explain()
        graph.flow_logger.info(f"creating a virtual table with: {polars_plan}")
        buf = _io.BytesIO()
        df.data_frame.serialize(buf)
        serialized_lf = buf.getvalue()
        source_table_versions = _collect_source_table_versions(graph)
    else:
        graph.flow_logger.info("creating a virtual table from workflow")

    schema_json = json.dumps([{"name": c.column_name, "dtype": c.data_type} for c in df.schema])

    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        svc = CatalogService(repo)
        existing = repo.get_table_by_name(settings.table_name, settings.namespace_id)
        if existing is not None and getattr(existing, "table_type", "physical") != "virtual":
            raise ValueError(
                f"Cannot write virtual table '{settings.table_name}': a non-virtual "
                f"catalog table with that name already exists in this namespace."
            )
        if existing is not None:
            svc.update_virtual_flow_table(
                table_id=existing.id,
                name=settings.table_name or None,
                producer_registration_id=reg_id,
                description=settings.description,
                serialized_lazy_frame=serialized_lf,
                is_optimized=is_lazy,
                schema_json=schema_json,
                polars_plan=polars_plan,
                source_table_versions=source_table_versions,
            )
        else:
            svc.create_virtual_flow_table(
                name=settings.table_name,
                owner_id=node_catalog_writer.user_id or 1,
                producer_registration_id=reg_id,
                namespace_id=settings.namespace_id,
                description=settings.description,
                serialized_lazy_frame=serialized_lf,
                is_optimized=is_lazy,
                schema_json=schema_json,
                polars_plan=polars_plan,
                source_table_versions=source_table_versions,
            )

    if changed_execution_mode:
        graph.execution_mode = "Development"
    return df


def _handle_physical_table_write(
    graph: "FlowGraph",
    node_catalog_writer: input_schema.NodeCatalogWriter,
    df: FlowDataEngine,
) -> FlowDataEngine:
    """Handle physical-mode catalog write: materialize data as a Delta table and register it."""
    settings = node_catalog_writer.catalog_write_settings

    catalog_dir = storage.catalog_tables_directory
    catalog_dir.mkdir(parents=True, exist_ok=True)

    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        svc = CatalogService(repo)
        existing, dest_path, delta_mode = svc.resolve_write_destination(
            table_name=settings.table_name,
            namespace_id=settings.namespace_id,
            write_mode=settings.write_mode,
            catalog_dir=catalog_dir,
        )

    if delta_mode in ("upsert", "update", "delete"):
        op_type = "merge_delta"
        op_kwargs = {
            "output_path": str(dest_path),
            "merge_mode": delta_mode,
            "merge_keys": settings.merge_keys,
        }
    else:
        op_type = "write_delta"
        op_kwargs = {"output_path": str(dest_path), "mode": delta_mode}

    if graph.flow_settings.execution_location == "local":
        meta_kwargs = _write_catalog_delta_local(df, dest_path, delta_mode, settings.merge_keys)
    else:
        meta_kwargs = _write_catalog_delta_remote(
            flow_id=graph.flow_id,
            node=graph.get_node(node_catalog_writer.node_id),
            df=df,
            op_type=op_type,
            op_kwargs=op_kwargs,
            table_name=settings.table_name,
        )

    if meta_kwargs is None:
        return df

    _register_catalog_table(
        existing=existing,
        dest_path=dest_path,
        settings=settings,
        source_registration_id=graph._flow_settings.source_registration_id,
        user_id=node_catalog_writer.user_id or 1,
        meta_kwargs=meta_kwargs,
    )
    return df


def _resolve_database_credentials(
    database_settings,
    user_id: int,
) -> tuple:
    """Resolve database connection and encrypted password from settings.

    Returns:
        (database_connection, encrypted_password, database_reference_settings)
        where database_reference_settings is the stored connection (or None for inline).
    """
    is_sqlite = (
        database_settings.connection_mode == "inline"
        and database_settings.database_connection is not None
        and database_settings.database_connection.database_type == "sqlite"
    )
    if database_settings.connection_mode == "inline" and not is_sqlite:
        database_connection = database_settings.database_connection
        encrypted_password = get_encrypted_secret(current_user_id=user_id, secret_name=database_connection.password_ref)
        if encrypted_password is None:
            raise HTTPException(status_code=400, detail="Password not found")
        return database_connection, encrypted_password, None
    elif is_sqlite:
        return database_settings.database_connection, None, None
    else:
        ref_settings = get_local_database_connection(database_settings.database_connection_name, user_id)
        if ref_settings is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Database connection '{database_settings.database_connection_name}' not found "
                    "or not accessible for this user"
                ),
            )
        encrypted_password = ref_settings.password.get_secret_value()
        return ref_settings, encrypted_password, ref_settings


class FlowGraph:
    """A class representing a Directed Acyclic Graph (DAG) for data processing pipelines.

    It manages nodes, connections, and the execution of the entire flow.
    """

    uuid: str
    depends_on: dict[
        int,
        Union[
            ParquetFile,
            FlowDataEngine,
            "FlowGraph",
            pl.DataFrame,
        ],
    ]
    _flow_id: int
    _input_data: Union[ParquetFile, FlowDataEngine, "FlowGraph"]
    _input_cols: list[str]
    _output_cols: list[str]
    _node_db: dict[str | int, FlowNode]
    _node_ids: list[str | int]
    _results: FlowDataEngine | None = None
    cache_results: bool = False
    schema: list[FlowfileColumn] | None = None
    has_over_row_function: bool = False
    _flow_starts: list[int | str] = None
    latest_run_info: RunInformation | None = None
    start_datetime: datetime = None
    end_datetime: datetime = None
    _flow_settings: schemas.FlowSettings = None
    flow_logger: FlowLogger

    def __init__(
        self,
        flow_settings: schemas.FlowSettings | schemas.FlowGraphConfig,
        name: str = None,
        input_cols: list[str] = None,
        output_cols: list[str] = None,
        path_ref: str = None,
        input_flow: Union[ParquetFile, FlowDataEngine, "FlowGraph"] = None,
        cache_results: bool = False,
    ):
        """Initializes a new FlowGraph instance.

        Args:
            flow_settings: The configuration settings for the flow.
            name: The name of the flow.
            input_cols: A list of input column names.
            output_cols: A list of output column names.
            path_ref: An optional path to an initial data source.
            input_flow: An optional existing data object to start the flow with.
            cache_results: A global flag to enable or disable result caching.
        """
        if isinstance(flow_settings, schemas.FlowGraphConfig):
            flow_settings = schemas.FlowSettings.from_flow_settings_input(flow_settings)

        self._flow_settings = flow_settings
        self.uuid = str(uuid1())
        self.start_datetime = None
        self.end_datetime = None
        self.latest_run_info = None
        self._flow_id = flow_settings.flow_id
        self.flow_logger = FlowLogger(flow_settings.flow_id)
        self._flow_starts: list[FlowNode] = []
        self._results = None
        self.schema = None
        self.has_over_row_function = False
        self._input_cols = [] if input_cols is None else input_cols
        self._output_cols = [] if output_cols is None else output_cols
        self._node_ids = []
        self._node_db = {}
        # Visual node groups: organizational only, never read by the executor.
        # Membership lives on each node's setting_input.group_id; this is the box registry.
        self._groups: dict[int, schemas.GroupInformation] = {}
        self._group_id_seq: int = 0  # monotonic group-id allocator; never reuses a freed id
        self._active_group_id: int | None = None
        self.cache_results = cache_results
        self.__name__ = name if name else "flow_" + str(id(self))
        self.depends_on = {}
        self.artifact_context = ArtifactContext()
        # Last user_id seen on any node settings (stamped by the editor routes /
        # open_flow). Lets restore_from_snapshot re-stamp the owner even when the
        # live graph is empty at undo time (snapshots intentionally omit user_id).
        self._owner_user_id: int | None = None

        from flowfile_core.flowfile.history_manager import HistoryManager
        from flowfile_core.schemas.history_schema import HistoryConfig

        history_config = HistoryConfig(enabled=flow_settings.track_history)
        self._history_manager = HistoryManager(config=history_config)

        if path_ref is not None:
            self.add_datasource(input_schema.NodeDatasource(file_path=path_ref))
        elif input_flow is not None:
            self.add_datasource(input_file=input_flow)

        # Mark the empty initial state as the saved baseline so an unmodified
        # flow is not considered dirty.
        self._history_manager.mark_saved(self)

    @property
    def flow_settings(self) -> schemas.FlowSettings:
        return self._flow_settings

    @flow_settings.setter
    def flow_settings(self, flow_settings: schemas.FlowSettings):
        if (self._flow_settings.execution_location != flow_settings.execution_location) or (
            self._flow_settings.execution_mode != flow_settings.execution_mode
        ):
            self.reset()
        else:
            old_params = {p.name: p.default_value for p in self._flow_settings.parameters}
            new_params = {p.name: p.default_value for p in flow_settings.parameters}
            if old_params != new_params:
                for node in self.nodes:
                    if node.setting_input is not None and find_unresolved_in_model(node.setting_input):
                        node.reset(deep=True)
        self._flow_settings = flow_settings

    # ==================== History Management Methods ====================

    def capture_history_snapshot(
        self,
        action_type: HistoryActionType,
        description: str,
        node_id: int = None,
    ) -> bool:
        """Capture the current state before a change for undo support.

        Args:
            action_type: The type of action being performed.
            description: Human-readable description of the action.
            node_id: Optional ID of the affected node.

        Returns:
            True if snapshot was captured, False if skipped.
        """
        return self._history_manager.capture_snapshot(self, action_type, description, node_id)

    def capture_history_if_changed(
        self,
        pre_snapshot: schemas.FlowfileData,
        action_type: HistoryActionType,
        description: str,
        node_id: int = None,
    ) -> bool:
        """Capture history only if the flow state actually changed.

        Use this for settings updates where the change might be a no-op.
        Call this AFTER the change is applied.

        Args:
            pre_snapshot: The FlowfileData captured BEFORE the change.
            action_type: The type of action that was performed.
            description: Human-readable description of the action.
            node_id: Optional ID of the affected node.

        Returns:
            True if a change was detected and snapshot was captured.
        """
        return self._history_manager.capture_if_changed(self, pre_snapshot, action_type, description, node_id)

    def undo(self) -> UndoRedoResult:
        """Undo the last action by restoring to the previous state.

        Returns:
            UndoRedoResult indicating success or failure.
        """
        return self._history_manager.undo(self)

    def redo(self) -> UndoRedoResult:
        """Redo the last undone action.

        Returns:
            UndoRedoResult indicating success or failure.
        """
        return self._history_manager.redo(self)

    def get_history_state(self) -> HistoryState:
        """Get the current state of the history system.

        Returns:
            HistoryState with information about available undo/redo operations.
        """
        return self._history_manager.get_state()

    def mark_as_saved(self) -> None:
        """Mark the current flow state as the saved baseline (for dirty tracking)."""
        self._history_manager.mark_saved(self)

    def has_unsaved_changes(self) -> bool:
        """Return True if the flow has changed since the last save point."""
        return self._history_manager.has_unsaved_changes(self)

    def _execute_with_history(
        self,
        operation: Callable[[], Any],
        action_type: HistoryActionType,
        description: str,
        node_id: int = None,
    ) -> Any:
        """Execute an operation with automatic history capture.

        This helper captures the state before the operation, executes it,
        and records history only if the state actually changed.

        Args:
            operation: A callable that performs the actual operation.
            action_type: The type of action being performed.
            description: Human-readable description of the action.
            node_id: Optional ID of the affected node.

        Returns:
            The result of the operation (if any).
        """
        # Skip history capture if tracking is disabled for this flow
        if not self.flow_settings.track_history:
            return operation()

        pre_snapshot = self.get_flowfile_data()
        result = operation()
        self._history_manager.capture_if_changed(self, pre_snapshot, action_type, description, node_id)
        return result

    def restore_from_snapshot(self, snapshot: schemas.FlowfileData) -> None:
        """Clear current state and rebuild from a snapshot.

        This method is used internally by undo/redo to restore a previous state.

        Args:
            snapshot: The FlowfileData snapshot to restore from.
        """
        from flowfile_core.flowfile.manage.io_flowfile import (
            _flowfile_data_to_flow_information,
            determine_insertion_order,
        )

        # Preserve the current flow_id and source_registration_id
        original_flow_id = self._flow_id
        original_source_registration_id = self._flow_settings.source_registration_id

        flow_info = _flowfile_data_to_flow_information(snapshot)

        # The history snapshot intentionally omits ``user_id`` (so on-disk flows stay
        # portable across users). Capture it from the live graph BEFORE clearing so we
        # can re-stamp it onto the replayed settings — otherwise connection-backed
        # nodes would be restored with ``user_id=None`` and fail to resolve their
        # owner's connection at run time. Same idea as ``open_flow``'s re-stamp, but
        # sourced from the prior graph state instead of the opening user. If the live
        # graph holds no stamped nodes (e.g. undo right after deleting every node),
        # fall back to the session owner remembered by ``with_history_capture``.
        prior_user_ids = {
            node.node_id: uid
            for node in self.nodes
            if (uid := getattr(node.setting_input, "user_id", None)) is not None
        }
        fallback_uid = next(iter(prior_user_ids.values()), None)
        if fallback_uid is None:
            fallback_uid = self._owner_user_id

        self._node_db.clear()
        self._node_ids.clear()
        self._flow_starts.clear()
        self._groups.clear()
        self._results = None

        # Restore flow settings (preserve original flow_id and source_registration_id)
        self._flow_settings = flow_info.flow_settings
        self._flow_settings.flow_id = original_flow_id
        self._flow_id = original_flow_id
        if self._flow_settings.source_registration_id is None:
            self._flow_settings.source_registration_id = original_source_registration_id
        self.__name__ = flow_info.flow_name or self.__name__

        ingestion_order = determine_insertion_order(flow_info)

        for node_id in ingestion_order:
            node_info = flow_info.data[node_id]
            node_promise = input_schema.NodePromise(
                flow_id=original_flow_id,
                node_id=node_info.id,
                pos_x=node_info.x_position or 0,
                pos_y=node_info.y_position or 0,
                node_type=node_info.type,
            )
            if hasattr(node_info.setting_input, "cache_results"):
                node_promise.cache_results = node_info.setting_input.cache_results
            self.add_node_promise(node_promise)

        for node_id in ingestion_order:
            node_info = flow_info.data[node_id]
            if node_info.is_setup and node_info.setting_input is not None:
                if hasattr(node_info.setting_input, "flow_id"):
                    node_info.setting_input.flow_id = original_flow_id

                # Re-stamp the owning user_id (dropped from the snapshot) so the
                # replayed add_<type> and any deferred connection resolution run
                # under the correct user. Prefer the node's prior id; fall back to
                # the session owner (shared by all nodes in a session).
                if hasattr(node_info.setting_input, "user_id"):
                    node_info.setting_input.user_id = prior_user_ids.get(node_id, fallback_uid)

                if hasattr(node_info.setting_input, "is_user_defined") and node_info.setting_input.is_user_defined:
                    if node_info.type in CUSTOM_NODE_STORE:
                        user_defined_node_class = CUSTOM_NODE_STORE[node_info.type]
                        self.add_user_defined_node(
                            custom_node=user_defined_node_class.from_settings(node_info.setting_input.settings),
                            user_defined_node_settings=node_info.setting_input,
                        )
                else:
                    add_method = getattr(self, "add_" + node_info.type, None)
                    if add_method:
                        add_method(node_info.setting_input)

        for node_id in ingestion_order:
            node_info = flow_info.data[node_id]
            from_node = self.get_node(node_id)
            if from_node is None:
                continue

            for output_node_id in node_info.outputs or []:
                to_node = self.get_node(output_node_id)
                if to_node is None:
                    continue

                output_node_info = flow_info.data.get(output_node_id)
                if output_node_info is None:
                    continue

                is_left_input = (output_node_info.left_input_id == node_id) and (
                    to_node.left_input is None or to_node.left_input.node_id != node_id
                )
                is_right_input = (output_node_info.right_input_id == node_id) and (
                    to_node.right_input is None or to_node.right_input.node_id != node_id
                )
                is_main_input = node_id in (output_node_info.input_ids or [])

                if is_left_input:
                    insert_type = "left"
                elif is_right_input:
                    insert_type = "right"
                elif is_main_input:
                    insert_type = "main"
                else:
                    continue

                to_node.add_node_connection(from_node, insert_type)

        # Member group_ids were re-applied above via add_<type>(setting_input);
        # repopulate the box registry (name/color/bounds) from the snapshot.
        self.restore_groups(flow_info.groups)

        logger.info(f"Restored flow from snapshot with {len(self._node_db)} nodes")

    # ==================== End History Management Methods ====================

    # ==================== Group Management Methods ====================
    # Groups are purely visual containers. They never affect execution; the only
    # link to a node is that node's setting_input.group_id. The group box props
    # (name/color/bounds) live in self._groups and ride along in FlowfileData.

    def _next_group_id(self) -> int:
        """Allocate a monotonically increasing group id (never reuses a freed id this session)."""
        self._group_id_seq = max([self._group_id_seq, *self._groups]) + 1
        return self._group_id_seq

    def _member_node_ids(self, group_id: int) -> list[int]:
        """Derive a group's members by scanning node group_id (single source of truth)."""
        return [node.node_id for node in self.nodes if getattr(node.setting_input, "group_id", None) == group_id]

    def _set_node_group(self, node_id: int, group_id: int | None) -> None:
        node = self.get_node(node_id)
        if node is not None and node.setting_input is not None and hasattr(node.setting_input, "group_id"):
            node.setting_input.group_id = group_id

    def _child_group_ids(self, group_id: int) -> list[int]:
        """Sub-groups whose immediate parent is this group."""
        return [gid for gid, g in self._groups.items() if g.parent_group_id == group_id]

    def _group_depth(self, group_id: int) -> int:
        """Nesting depth (0 = top-level); cycle-safe."""
        depth, seen, current = 0, set(), self._groups.get(group_id)
        while current is not None and current.parent_group_id is not None and current.id not in seen:
            seen.add(current.id)
            depth += 1
            current = self._groups.get(current.parent_group_id)
        return depth

    def _is_ancestor_group(self, ancestor_id: int, group_id: int) -> bool:
        """True if ancestor_id equals group_id or one of its ancestors (cycle-safe)."""
        seen, current = set(), self._groups.get(group_id)
        while current is not None and current.id not in seen:
            if current.id == ancestor_id:
                return True
            seen.add(current.id)
            current = self._groups.get(current.parent_group_id) if current.parent_group_id is not None else None
        return False

    def _recompute_group_bounds(self, group_id: int | None = None) -> None:
        """Refit one or all group boxes around their member nodes and child groups.

        Uses nominal node dimensions since the backend doesn't know rendered sizes;
        the frontend refines bounds on first user interaction. Groups with no members
        keep their current bounds. When refitting all groups, deepest first so a parent
        unions already-fitted child-group boxes.
        """
        node_width, node_height, padding, header = 180.0, 80.0, 40.0, 36.0
        if group_id is not None:
            target_ids = [group_id]
        else:
            target_ids = sorted(self._groups, key=self._group_depth, reverse=True)
        for gid in target_ids:
            group = self._groups.get(gid)
            if group is None:
                continue
            boxes: list[tuple[float, float, float, float]] = []  # (x, y, w, h)
            for nid in self._member_node_ids(gid):
                node = self.get_node(nid)
                if node is not None and node.setting_input is not None:
                    nx = float(node.setting_input.pos_x or 0)
                    ny = float(node.setting_input.pos_y or 0)
                    boxes.append((nx, ny, node_width, node_height))
            for cid in self._child_group_ids(gid):
                child = self._groups.get(cid)
                if child is not None:
                    boxes.append((child.x_position, child.y_position, child.width, child.height))
            if not boxes:
                continue
            min_x = min(b[0] for b in boxes) - padding
            min_y = min(b[1] for b in boxes) - padding - header
            max_x = max(b[0] + b[2] for b in boxes) + padding
            max_y = max(b[1] + b[3] for b in boxes) + padding
            group.x_position = min_x
            group.y_position = min_y
            group.width = max_x - min_x
            group.height = max_y - min_y

    def create_group(
        self,
        name: str,
        node_ids: list[int],
        *,
        color: schemas.GroupColor | None = None,
        bounds: schemas.GroupBounds | None = None,
        parent_group_id: int | None = None,
        child_group_ids: list[int] | None = None,
    ) -> schemas.GroupInformation:
        """Create a visual group. Organizational only.

        Members are the given nodes (group_id) and child groups (their parent_group_id).
        The new group itself nests under parent_group_id. Bounds are computed when not supplied.
        """

        def _do() -> schemas.GroupInformation:
            group_id = self._next_group_id()
            group = schemas.GroupInformation(id=group_id, name=name, color=color, parent_group_id=parent_group_id)
            if bounds is not None:
                group.x_position, group.y_position, group.width, group.height = bounds
            self._groups[group_id] = group
            for node_id in node_ids:
                self._set_node_group(node_id, group_id)
            for cid in child_group_ids or []:
                child = self._groups.get(cid)
                if child is not None and not self._is_ancestor_group(cid, group_id):
                    child.parent_group_id = group_id
            if bounds is None:
                self._recompute_group_bounds(group_id)
            return group

        return self._execute_with_history(_do, HistoryActionType.CREATE_GROUP, f"Create group '{name}'")

    def update_group(
        self,
        group_id: int,
        *,
        name: str | None = None,
        color: schemas.GroupColor | None = None,
        bounds: schemas.GroupBounds | None = None,
        collapsed: bool | None = None,
    ) -> schemas.GroupInformation:
        """Rename / recolor / move / resize / collapse a group box."""
        group = self._groups.get(group_id)
        if group is None:
            raise ValueError(f"Group {group_id} does not exist")

        def _do() -> schemas.GroupInformation:
            if name is not None:
                group.name = name
            if color is not None:
                group.color = color
            if bounds is not None:
                group.x_position, group.y_position, group.width, group.height = bounds
            if collapsed is not None:
                group.collapsed = collapsed
            return group

        return self._execute_with_history(_do, HistoryActionType.UPDATE_GROUP, f"Update group '{group.name}'")

    def delete_group(self, group_id: int) -> None:
        """Remove a group box (ungroup). Members and sub-groups lift up one level."""
        group = self._groups.get(group_id)
        if group is None:
            return
        new_parent = group.parent_group_id

        def _do() -> None:
            for node_id in self._member_node_ids(group_id):
                self._set_node_group(node_id, new_parent)
            for cid in self._child_group_ids(group_id):
                child = self._groups.get(cid)
                if child is not None:
                    child.parent_group_id = new_parent
            self._groups.pop(group_id, None)

        self._execute_with_history(_do, HistoryActionType.DELETE_GROUP, f"Delete group '{group.name}'")

    def add_nodes_to_group(self, group_id: int, node_ids: list[int]) -> schemas.GroupInformation:
        """Add nodes to an existing group and refit its bounds."""
        group = self._groups.get(group_id)
        if group is None:
            raise ValueError(f"Group {group_id} does not exist")

        def _do() -> schemas.GroupInformation:
            for node_id in node_ids:
                self._set_node_group(node_id, group_id)
            self._recompute_group_bounds(group_id)
            return group

        return self._execute_with_history(_do, HistoryActionType.UPDATE_GROUP_MEMBERSHIP, "Add nodes to group")

    def remove_nodes_from_group(self, node_ids: list[int]) -> None:
        """Remove nodes from whatever group they belong to; prune groups left empty."""

        def _do() -> None:
            affected: set[int] = set()
            for node_id in node_ids:
                node = self.get_node(node_id)
                current = getattr(node.setting_input, "group_id", None) if node is not None else None
                if current is not None:
                    affected.add(current)
                    self._set_node_group(node_id, None)
            for gid in affected:
                if not self._member_node_ids(gid) and not self._child_group_ids(gid):
                    self._groups.pop(gid, None)
                else:
                    self._recompute_group_bounds(gid)

        self._execute_with_history(_do, HistoryActionType.UPDATE_GROUP_MEMBERSHIP, "Remove nodes from group")

    def assign_node_to_named_group(
        self, node_id: int, name: str, *, color: schemas.GroupColor | None = None
    ) -> schemas.GroupInformation:
        """Assign a node to a group identified by name, creating it if absent (find-or-create)."""
        existing = next((group for group in self._groups.values() if group.name == name), None)
        if existing is not None:
            return self.add_nodes_to_group(existing.id, [node_id])
        return self.create_group(name, [node_id], color=color)

    def set_node_positions(self, updates: list[schemas.NodePositionUpdate]) -> None:
        """Persist dragged node positions (absolute canvas coordinates) onto setting_input.

        Plain mutator: the caller (update_layout route) captures history once for the
        whole drag-end batch so node moves and group-bounds changes share one snapshot.
        """
        for update in updates:
            node = self.get_node(update.node_id)
            if node is not None and node.setting_input is not None and hasattr(node.setting_input, "pos_x"):
                node.setting_input.pos_x = update.pos_x
                node.setting_input.pos_y = update.pos_y

    def set_group_bounds(self, updates: list[schemas.GroupBoundsUpdate]) -> None:
        """Persist group box bounds (used together with set_node_positions on drag/resize)."""
        for update in updates:
            group = self._groups.get(update.group_id)
            if group is not None:
                group.x_position = update.x_position
                group.y_position = update.y_position
                group.width = update.width
                group.height = update.height

    def restore_groups(self, groups: list[schemas.GroupInformation]) -> None:
        """Replace the runtime group registry (used by open_flow and restore_from_snapshot)."""
        self._groups = {group.id: group for group in groups}
        self._group_id_seq = max(self._groups, default=0)  # next id resumes above the highest restored
        for group in self._groups.values():
            if group.width <= 0 or group.height <= 0:
                self._recompute_group_bounds(group.id)

    # ==================== End Group Management Methods ====================

    def add_node_to_starting_list(self, node: FlowNode) -> None:
        """Adds a node to the list of starting nodes for the flow if not already present.

        Args:
            node: The FlowNode to add as a starting node.
        """
        if node.node_id not in {self_node.node_id for self_node in self._flow_starts}:
            self._flow_starts.append(node)

    def add_node_promise(self, node_promise: input_schema.NodePromise, track_history: bool = True):
        """Adds a placeholder node to the graph that is not yet fully configured.

        Useful for building the graph structure before all settings are available.
        Automatically captures history for undo/redo support.

        Args:
            node_promise: A promise object containing basic node information.
            track_history: Whether to track this change in history (default True).
        """

        def _do_add():
            def placeholder(n: FlowNode = None):
                if n is None:
                    return FlowDataEngine()
                return n

            self.add_node_step(
                node_id=node_promise.node_id,
                node_type=node_promise.node_type,
                function=placeholder,
                setting_input=node_promise,
            )
            if node_promise.is_user_defined:
                node_needs_settings: bool
                custom_node = CUSTOM_NODE_STORE.get(node_promise.node_type)
                if custom_node is None:
                    raise Exception(f"Custom node type '{node_promise.node_type}' not found in registry.")
                settings_schema = custom_node.model_fields["settings_schema"].default
                node_needs_settings = settings_schema is not None and not settings_schema.is_empty()
                if not node_needs_settings:
                    user_defined_node_settings = input_schema.UserDefinedNode(settings={}, **node_promise.model_dump())
                    initialized_model = custom_node()
                    self.add_user_defined_node(
                        custom_node=initialized_model, user_defined_node_settings=user_defined_node_settings
                    )

        if track_history:
            self._execute_with_history(
                _do_add,
                HistoryActionType.ADD_NODE,
                f"Add {node_promise.node_type} node",
                node_id=node_promise.node_id,
            )
        else:
            _do_add()

    def apply_layout(self, y_spacing: int = 150, x_spacing: int = 200, initial_y: int = 100):
        """Calculates and applies a layered layout to all nodes in the graph.

        This updates their x and y positions for UI rendering.

        Args:
            y_spacing: The vertical spacing between layers.
            x_spacing: The horizontal spacing between nodes in the same layer.
            initial_y: The initial y-position for the first layer.
        """
        self.flow_logger.info("Applying layered layout...")
        start_time = time()
        try:
            new_positions = calculate_layered_layout(
                self, y_spacing=y_spacing, x_spacing=x_spacing, initial_y=initial_y
            )

            if not new_positions:
                self.flow_logger.warning("Layout calculation returned no positions.")
                return

            updated_count = 0
            for node_id, (pos_x, pos_y) in new_positions.items():
                node = self.get_node(node_id)
                if node and hasattr(node, "setting_input"):
                    setting = node.setting_input
                    if hasattr(setting, "pos_x") and hasattr(setting, "pos_y"):
                        setting.pos_x = pos_x
                        setting.pos_y = pos_y
                        updated_count += 1
                    else:
                        self.flow_logger.warning(
                            f"Node {node_id} setting_input ({type(setting)}) lacks pos_x/pos_y attributes."
                        )
                elif node:
                    self.flow_logger.warning(f"Node {node_id} lacks setting_input attribute.")
                # else: Node not found, already warned by calculate_layered_layout

            # Reflowed node positions invalidate group boxes — refit them.
            self._recompute_group_bounds()

            end_time = time()
            self.flow_logger.info(
                f"Layout applied to {updated_count}/{len(self.nodes)} nodes in {end_time - start_time:.2f} seconds."
            )

        except Exception as e:
            self.flow_logger.error(f"Error applying layout: {e}")
            raise  # Optional: re-raise the exception

    @property
    def flow_id(self) -> int:
        """Gets the unique identifier of the flow."""
        return self._flow_id

    @flow_id.setter
    def flow_id(self, new_id: int):
        """Sets the unique identifier for the flow and updates all child nodes.

        Args:
            new_id: The new flow ID.
        """
        self._flow_id = new_id
        for node in self.nodes:
            if hasattr(node.setting_input, "flow_id"):
                node.setting_input.flow_id = new_id
        self.flow_settings.flow_id = new_id

    def __repr__(self):
        """Provides the official string representation of the FlowGraph instance."""
        settings_str = "  -" + "\n  -".join(f"{k}: {v}" for k, v in self.flow_settings)
        return f"FlowGraph(\nNodes: {self._node_db}\n\nSettings:\n{settings_str}"

    def print_tree(self):
        """Print flow_graph as a visual tree structure, showing the DAG relationships with ASCII art."""
        if not self._node_db:
            self.flow_logger.info("Empty flow graph")
            return

        node_info = build_node_info(self.nodes)

        for node_id in node_info:
            calculate_depth(node_id, node_info)

        depth_groups, max_depth = group_nodes_by_depth(node_info)

        for depth in depth_groups:
            depth_groups[depth].sort()

        lines = ["=" * 80, "Flow Graph Visualization", "=" * 80, ""]

        merge_points = define_node_connections(node_info)

        max_label_length = {}
        for depth in range(max_depth + 1):
            if depth in depth_groups:
                max_len = max(len(node_info[nid].label) for nid in depth_groups[depth])
                max_label_length[depth] = max_len

        drawn_nodes = set()
        merge_drawn = set()

        paths_by_merge = {}
        standalone_paths = []

        paths = build_flow_paths(node_info, self._flow_starts, merge_points)

        for path in paths:
            if len(path) > 1 and path[-1] in merge_points and len(merge_points[path[-1]]) > 1:
                merge_id = path[-1]
                if merge_id not in paths_by_merge:
                    paths_by_merge[merge_id] = []
                paths_by_merge[merge_id].append(path)
            else:
                standalone_paths.append(path)

        draw_merged_paths(node_info, merge_points, paths_by_merge, merge_drawn, drawn_nodes, lines)

        draw_standalone_paths(drawn_nodes, standalone_paths, lines, node_info)

        add_un_drawn_nodes(drawn_nodes, node_info, lines)

        try:
            execution_plan = compute_execution_plan(
                nodes=self.nodes, flow_starts=self._flow_starts + self.get_implicit_starter_nodes()
            )
            ordered_nodes = execution_plan.all_nodes
            if ordered_nodes:
                for i, node in enumerate(ordered_nodes, 1):
                    lines.append(f"  {i:3d}. {node_info[node.node_id].label}")
        except Exception as e:
            lines.append(f"  Could not determine execution order: {e}")

        output = "\n".join(lines)

        print(output)

    def get_nodes_overview(self):
        """Gets a list of dictionary representations for all nodes in the graph."""
        output = []
        for v in self._node_db.values():
            output.append(v.get_repr())
        return output

    def remove_from_output_cols(self, columns: list[str]):
        """Removes specified columns from the list of expected output columns.

        Args:
            columns: A list of column names to remove.
        """
        cols = set(columns)
        self._output_cols = [c for c in self._output_cols if c not in cols]

    def get_node(self, node_id: int | str = None) -> FlowNode | None:
        """Retrieves a node from the graph by its ID.

        Args:
            node_id: The ID of the node to retrieve. If None, retrieves the last added node.

        Returns:
            The FlowNode object, or None if not found.
        """
        if node_id is None:
            node_id = self._node_ids[-1]
        node = self._node_db.get(node_id)
        if node is not None:
            return node

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
        if custom_node.requires_kernel and not user_defined_node_settings.kernel_id:
            raise ValueError("Kernel selection is required to execute this custom node.")

        kernel_id = user_defined_node_settings.kernel_id or custom_node.kernel_id
        output_names = user_defined_node_settings.output_names or custom_node.output_names

        if kernel_id:
            _func = self._make_kernel_user_defined_func(
                custom_node=custom_node,
                user_defined_node_settings=user_defined_node_settings,
                kernel_id=kernel_id,
                output_names=output_names,
            )
        else:
            _func = self._make_local_user_defined_func(
                custom_node=custom_node,
                user_defined_node_settings=user_defined_node_settings,
            )

        self.add_node_step(
            node_id=user_defined_node_settings.node_id,
            function=_func,
            setting_input=user_defined_node_settings,
            input_node_ids=user_defined_node_settings.depending_on_ids,
            node_type=custom_node.item,
        )
        if custom_node.number_of_inputs == 0:
            node = self.get_node(user_defined_node_settings.node_id)
            self.add_node_to_starting_list(node)

    def _make_local_user_defined_func(
        self, *, custom_node: CustomNodeBase, user_defined_node_settings: input_schema.UserDefinedNode
    ) -> Callable:
        """Create the execution function for a locally-executed custom node."""

        def _func(*flow_data_engine: FlowDataEngine) -> FlowDataEngine | None:
            user_id = user_defined_node_settings.user_id
            if user_id is not None:
                custom_node.set_execution_context(user_id)
                if custom_node.settings_schema:
                    custom_node.settings_schema.set_secret_context(user_id, custom_node.accessed_secrets)

            output = custom_node.process(*(fde.data_frame for fde in flow_data_engine))

            accessed_secrets = custom_node.get_accessed_secrets()
            if accessed_secrets:
                logger.info(f"Node '{user_defined_node_settings.node_id}' accessed secrets: {accessed_secrets}")
            if isinstance(output, pl.LazyFrame | pl.DataFrame):
                return FlowDataEngine(output)
            return None

        return _func

    def _execute_on_kernel(
        self,
        *,
        node_id: int,
        kernel_id: str,
        code: str,
        output_names: list[str],
        flow_data_engine: tuple[FlowDataEngine, ...],
    ) -> FlowDataEngine | None:
        """Execute code on a kernel container and return the primary output.

        Shared logic for both custom-node kernel execution and python_script nodes.
        Handles artifact context, directory setup, input writing, kernel execution,
        log forwarding, artifact recording, and output reading.
        """
        manager = get_kernel_manager()
        flow_id = self.flow_id
        node_logger = self.flow_logger.get_node_logger(node_id)

        self.artifact_context.clear_nodes({node_id})

        self.artifact_context.compute_available(
            node_id=node_id,
            kernel_id=kernel_id,
            upstream_node_ids=self._get_upstream_node_ids(node_id),
        )

        shared_base = manager.shared_volume_path
        input_dir = os.path.join(shared_base, str(flow_id), str(node_id), "inputs")
        output_dir = os.path.join(shared_base, str(flow_id), str(node_id), "outputs")
        os.makedirs(input_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)
        # Stale outputs from a prior run would mask missing publishes in read_kernel_outputs
        for stale in os.listdir(output_dir):
            if stale.endswith(".parquet"):
                os.remove(os.path.join(output_dir, stale))

        node = self.get_node(node_id)
        input_names = self._resolve_input_names(node, len(flow_data_engine))
        input_paths = write_inputs_to_parquet(
            flow_data_engine, manager, input_dir, flow_id, node_id, input_names=input_names
        )

        request = build_execute_request(
            node_id=node_id,
            code=code,
            input_paths=input_paths,
            output_dir=output_dir,
            flow_id=flow_id,
            manager=manager,
            source_registration_id=self._flow_settings.source_registration_id,
        )

        cancel_event = threading.Event()
        if node is not None:
            node._kernel_cancel_context = (kernel_id, manager)
            node._kernel_cancel_event = cancel_event
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
                artifacts=[{"name": n} for n in result.artifacts_published],
            )
        if result.artifacts_deleted:
            self.artifact_context.record_deleted(
                node_id=node_id,
                kernel_id=kernel_id,
                artifact_names=result.artifacts_deleted,
            )

        primary_result = read_kernel_outputs(
            output_dir=output_dir, output_names=output_names, result=result, node=node
        )

        if primary_result is not None:
            return primary_result
        return flow_data_engine[0] if flow_data_engine else FlowDataEngine(pl.LazyFrame())

    def _make_kernel_user_defined_func(
        self,
        *,
        custom_node: CustomNodeBase,
        user_defined_node_settings: input_schema.UserDefinedNode,
        kernel_id: str,
        output_names: list[str],
    ) -> Callable:
        """Create the execution function for a kernel-executed custom node."""

        def _func(*flow_data_engine: FlowDataEngine) -> FlowDataEngine | None:
            return self._execute_on_kernel(
                node_id=user_defined_node_settings.node_id,
                kernel_id=kernel_id,
                code=custom_node.generate_kernel_code(),
                output_names=output_names,
                flow_data_engine=flow_data_engine,
            )

        return _func

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_pivot(self, pivot_settings: input_schema.NodePivot):
        """Adds a pivot node to the graph.

        Args:
            pivot_settings: The settings for the pivot operation.
        """

        def _func(fl: FlowDataEngine):
            return fl.do_pivot(pivot_settings.pivot_input, self.flow_logger.get_node_logger(pivot_settings.node_id))

        self.add_node_step(
            node_id=pivot_settings.node_id,
            function=_func,
            node_type="pivot",
            setting_input=pivot_settings,
            input_node_ids=[pivot_settings.depending_on_id],
        )

        node = self.get_node(pivot_settings.node_id)

        def schema_callback():
            input_data = node.singular_main_input.get_resulting_data()
            input_data.lazy = True
            input_lf = input_data.data_frame
            return pre_calculate_pivot_schema(input_data.schema, pivot_settings.pivot_input, input_lf=input_lf)

        node.schema_callback = schema_callback

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_unpivot(self, unpivot_settings: input_schema.NodeUnpivot):
        """Adds an unpivot node to the graph.

        Args:
            unpivot_settings: The settings for the unpivot operation.
        """

        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.unpivot(unpivot_settings.unpivot_input)

        self.add_node_step(
            node_id=unpivot_settings.node_id,
            function=_func,
            node_type="unpivot",
            setting_input=unpivot_settings,
            input_node_ids=[unpivot_settings.depending_on_id],
        )

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_union(self, union_settings: input_schema.NodeUnion):
        """Adds a union node to combine multiple data streams.

        Args:
            union_settings: The settings for the union operation.
        """

        def _func(*flowfile_tables: FlowDataEngine):
            dfs: list[pl.LazyFrame] | list[pl.DataFrame] = [flt.data_frame for flt in flowfile_tables]
            return FlowDataEngine(pl.concat(dfs, how="diagonal_relaxed"))

        self.add_node_step(
            node_id=union_settings.node_id,
            function=_func,
            node_type="union",
            setting_input=union_settings,
            input_node_ids=union_settings.depending_on_ids,
        )

    def add_initial_node_analysis(self, node_promise: input_schema.NodePromise, track_history: bool = True):
        """Adds a data exploration/analysis node based on a node promise.

        Automatically captures history for undo/redo support.

        Args:
            node_promise: The promise representing the node to be analyzed.
            track_history: Whether to track this change in history (default True).
        """

        def _do_add():
            node_analysis = create_graphic_walker_node_from_node_promise(node_promise)
            self.add_explore_data(node_analysis)

        if track_history:
            self._execute_with_history(
                _do_add,
                HistoryActionType.ADD_NODE,
                f"Add {node_promise.node_type} node",
                node_id=node_promise.node_id,
            )
        else:
            _do_add()

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_explore_data(self, node_analysis: input_schema.NodeExploreData):
        """Adds a specialized node for data exploration and visualization.

        Args:
            node_analysis: The settings for the data exploration node.
        """
        sample_size: int = 10000

        def analysis_preparation(flowfile_table: FlowDataEngine):
            if flowfile_table.number_of_records <= 0:
                calculate_in_worker = self.execution_location != "local"
                number_of_records = flowfile_table.get_number_of_records(
                    calculate_in_worker_process=calculate_in_worker
                )
            else:
                number_of_records = flowfile_table.number_of_records
            if number_of_records > sample_size:
                flowfile_table = flowfile_table.get_sample(sample_size, random=True)
            if self.execution_location == "local":
                collected = flowfile_table.data_frame.collect()
                pa_table = collected.to_arrow()
                node.results.analysis_data_generator = lambda: pa_table
            else:
                external_sampler = ExternalDfFetcher(
                    lf=flowfile_table.data_frame,
                    file_ref="__gf_walker" + node.hash,
                    wait_on_completion=False,
                    node_id=node.node_id,
                    flow_id=self.flow_id,
                )
                node._fetch_cached_df = external_sampler
                external_sampler.get_result()
                node.results.analysis_data_generator = get_read_top_n(
                    external_sampler.status.file_ref, n=min(sample_size, number_of_records)
                )
            return flowfile_table

        def schema_callback():
            node = self.get_node(node_analysis.node_id)
            if len(node.all_inputs) == 1:
                input_node = node.all_inputs[0]
                return input_node.schema
            else:
                return [FlowfileColumn.from_input("col_1", "na")]

        self.add_node_step(
            node_id=node_analysis.node_id,
            node_type="explore_data",
            function=analysis_preparation,
            setting_input=node_analysis,
            schema_callback=schema_callback,
        )
        node = self.get_node(node_analysis.node_id)

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_group_by(self, group_by_settings: input_schema.NodeGroupBy):
        """Adds a group-by aggregation node to the graph.

        Args:
            group_by_settings: The settings for the group-by operation.
        """

        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.do_group_by(group_by_settings.groupby_input, False)

        self.add_node_step(
            node_id=group_by_settings.node_id,
            function=_func,
            node_type="group_by",
            setting_input=group_by_settings,
            input_node_ids=[group_by_settings.depending_on_id],
        )

        node = self.get_node(group_by_settings.node_id)

        def schema_callback():
            output_columns = [(c.old_name, c.new_name, c.output_type) for c in group_by_settings.groupby_input.agg_cols]
            depends_on = node.node_inputs.main_inputs[0]
            input_schema_dict: dict[str, str] = {s.name: s.data_type for s in depends_on.schema}
            output_schema = []
            for old_name, new_name, data_type in output_columns:
                data_type = input_schema_dict[old_name] if data_type is None else data_type
                output_schema.append(FlowfileColumn.from_input(data_type=data_type, column_name=new_name))
            return output_schema

        node.schema_callback = schema_callback

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_filter(self, filter_settings: input_schema.NodeFilter):
        """Adds a filter node to the graph.

        Args:
            filter_settings: The settings for the filter operation.
        """

        def _func(fl: FlowDataEngine):
            is_advanced = filter_settings.filter_input.is_advanced()

            if is_advanced:
                expression = filter_settings.filter_input.advanced_filter
            else:
                basic_filter = filter_settings.filter_input.basic_filter
                if basic_filter is None:
                    logger.warning("Basic filter is None, returning unfiltered data")
                    return fl

                try:
                    field_data_type = fl.get_schema_column(basic_filter.field).generic_datatype()
                except Exception:
                    field_data_type = None

                expression = build_filter_expression(basic_filter, field_data_type)
                filter_settings.filter_input.advanced_filter = expression

            if filter_settings.split_mode:
                return fl.filter_split(expression)
            return fl.do_filter(expression)

        self.add_node_step(
            filter_settings.node_id,
            _func,
            node_type="filter",
            renew_schema=False,
            setting_input=filter_settings,
            input_node_ids=[filter_settings.depending_on_id],
        )

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_record_count(self, node_number_of_records: input_schema.NodeRecordCount):
        """Adds a filter node to the graph.

        Args:
            node_number_of_records: The settings for the record count operation.
        """

        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.get_record_count()

        self.add_node_step(
            node_id=node_number_of_records.node_id,
            function=_func,
            node_type="record_count",
            setting_input=node_number_of_records,
            input_node_ids=[node_number_of_records.depending_on_id],
        )

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_polars_code(self, node_polars_code: input_schema.NodePolarsCode):
        """Adds a node that executes custom Polars code.

        Args:
            node_polars_code: The settings for the Polars code node.
        """

        def _func(*flowfile_tables: FlowDataEngine) -> FlowDataEngine:
            return execute_polars_code(*flowfile_tables, code=node_polars_code.polars_code_input.polars_code)

        self.add_node_step(
            node_id=node_polars_code.node_id,
            function=_func,
            node_type="polars_code",
            setting_input=node_polars_code,
            input_node_ids=node_polars_code.depending_on_ids,
        )

        try:
            polars_code_parser.validate_code(node_polars_code.polars_code_input.polars_code)
        except Exception as e:
            node = self.get_node(node_id=node_polars_code.node_id)
            node.results.errors = str(e)

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_sql_query(self, node_sql_query: input_schema.NodeSqlQuery):
        """Adds a node that executes a SQL query against connected data sources.

        Args:
            node_sql_query: The settings for the SQL query node.
        """

        def _func(*flowfile_tables: FlowDataEngine) -> FlowDataEngine:
            return execute_sql_query(*flowfile_tables, sql_code=node_sql_query.sql_query_input.sql_code)

        self.add_node_step(
            node_id=node_sql_query.node_id,
            function=_func,
            node_type="sql_query",
            setting_input=node_sql_query,
            input_node_ids=node_sql_query.depending_on_ids,
        )

        try:
            validate_sql_query(node_sql_query.sql_query_input.sql_code)
        except Exception as e:
            node = self.get_node(node_id=node_sql_query.node_id)
            node.results.errors = str(e)

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_python_script(self, node_python_script: input_schema.NodePythonScript):
        """Adds a node that executes Python code on a kernel container."""

        def _func(*flowfile_tables: FlowDataEngine) -> FlowDataEngine:
            kernel_id = node_python_script.python_script_input.kernel_id
            if not kernel_id:
                raise ValueError("No kernel selected for python_script node")
            result = self._execute_on_kernel(
                node_id=node_python_script.node_id,
                kernel_id=kernel_id,
                code=node_python_script.python_script_input.code,
                output_names=node_python_script.output_names,
                flow_data_engine=flowfile_tables,
            )
            return result or (flowfile_tables[0] if flowfile_tables else FlowDataEngine(pl.LazyFrame()))

        def schema_callback():
            """Best-effort schema prediction for python_script nodes.

            Returns the input node(s) schema as a reasonable default
            (most python_script nodes transform and pass through).
            If nothing is available, returns [] — never raises.
            """
            try:
                node = self.get_node(node_python_script.node_id)
                if node is None:
                    return []

                main_inputs = node.node_inputs.main_inputs
                if main_inputs:
                    first_input = main_inputs[0]
                    input_node_schema = first_input.schema
                    if input_node_schema:
                        return input_node_schema
                return []
            except Exception:
                return []

        self.add_node_step(
            node_id=node_python_script.node_id,
            function=_func,
            node_type="python_script",
            setting_input=node_python_script,
            input_node_ids=node_python_script.depending_on_ids,
            schema_callback=schema_callback,
        )

        output_names = node_python_script.output_names
        if len(output_names) > 1:
            node = self.get_node(node_python_script.node_id)
            if node is not None:
                node.node_template = node.node_template.model_copy(update={"output": len(output_names)})

    def add_dependency_on_polars_lazy_frame(self, lazy_frame: pl.LazyFrame, node_id: int):
        """Adds a special node that directly injects a Polars LazyFrame into the graph.

        Note: This is intended for backend use and will not work in the UI editor.

        Args:
            lazy_frame: The Polars LazyFrame to inject.
            node_id: The ID for the new node.
        """

        def _func():
            return FlowDataEngine(lazy_frame)

        node_promise = input_schema.NodePromise(
            flow_id=self.flow_id, node_id=node_id, node_type="polars_lazy_frame", is_setup=True
        )
        self.add_node_step(
            node_id=node_promise.node_id, node_type=node_promise.node_type, function=_func, setting_input=node_promise
        )

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_unique(self, unique_settings: input_schema.NodeUnique):
        """Adds a node to find and remove duplicate rows.

        Args:
            unique_settings: The settings for the unique operation.
        """

        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.make_unique(unique_settings.unique_input)

        self.add_node_step(
            node_id=unique_settings.node_id,
            function=_func,
            input_columns=[],
            node_type="unique",
            setting_input=unique_settings,
            input_node_ids=[unique_settings.depending_on_id],
        )

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_graph_solver(self, graph_solver_settings: input_schema.NodeGraphSolver):
        """Adds a node that solves graph-like problems within the data.

        This node can be used for operations like finding network paths,
        calculating connected components, or performing other graph algorithms
        on relational data that represents nodes and edges.

        Args:
            graph_solver_settings: The settings object defining the graph inputs
                and the specific algorithm to apply.
        """

        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.solve_graph(graph_solver_settings.graph_solver_input)

        self.add_node_step(
            node_id=graph_solver_settings.node_id,
            function=_func,
            node_type="graph_solver",
            setting_input=graph_solver_settings,
            input_node_ids=[graph_solver_settings.depending_on_id],
        )

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_formula(self, function_settings: input_schema.NodeFormula):
        """Adds a node that applies a formula to create or modify a column.

        Args:
            function_settings: The settings for the formula operation.
        """

        error = ""
        if function_settings.function.field.data_type not in (None, transform_schema.AUTO_DATA_TYPE):
            output_type = cast_str_to_polars_type(function_settings.function.field.data_type)
        else:
            output_type = None
        if output_type not in (None, transform_schema.AUTO_DATA_TYPE):
            new_col = [
                FlowfileColumn.from_input(column_name=function_settings.function.field.name, data_type=str(output_type))
            ]
        else:
            new_col = [FlowfileColumn.from_input(function_settings.function.field.name, "String")]

        def _func(fl: FlowDataEngine):
            return fl.apply_sql_formula(
                func=function_settings.function.function,
                col_name=function_settings.function.field.name,
                output_data_type=output_type,
            )

        self.add_node_step(
            function_settings.node_id,
            _func,
            output_schema=new_col,
            node_type="formula",
            renew_schema=False,
            setting_input=function_settings,
            input_node_ids=[function_settings.depending_on_id],
        )
        if error != "":
            node = self.get_node(function_settings.node_id)
            node.results.errors = error
            return False, error
        else:
            return True, ""

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_cross_join(self, cross_join_settings: input_schema.NodeCrossJoin) -> "FlowGraph":
        """Adds a cross join node to the graph.

        Args:
            cross_join_settings: The settings for the cross join operation.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(main: FlowDataEngine, right: FlowDataEngine) -> FlowDataEngine:
            for left_select in cross_join_settings.cross_join_input.left_select.renames:
                left_select.is_available = True if left_select.old_name in main.schema else False
            for right_select in cross_join_settings.cross_join_input.right_select.renames:
                right_select.is_available = True if right_select.old_name in right.schema else False
            return main.do_cross_join(
                cross_join_input=cross_join_settings.cross_join_input,
                auto_generate_selection=cross_join_settings.auto_generate_selection,
                verify_integrity=False,
                other=right,
            )

        def schema_callback():
            cj_copy = CrossJoinInputManager(cross_join_settings.cross_join_input)
            node = self.get_node(node_id=cross_join_settings.node_id)
            return calculate_cross_join_schema(
                cj_copy,
                left_schema=node.node_inputs.main_inputs[0].schema,
                right_schema=node.node_inputs.right_input.schema,
            )

        self.add_node_step(
            node_id=cross_join_settings.node_id,
            function=_func,
            input_columns=[],
            node_type="cross_join",
            setting_input=cross_join_settings,
            input_node_ids=cross_join_settings.depending_on_ids,
            schema_callback=schema_callback,
        )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_join(self, join_settings: input_schema.NodeJoin) -> "FlowGraph":
        """Adds a join node to combine two data streams based on key columns.

        Args:
            join_settings: The settings for the join operation.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(main: FlowDataEngine, right: FlowDataEngine) -> FlowDataEngine:
            join_input = deepcopy(join_settings.join_input)
            for left_select in join_input.left_select.renames:
                left_select.is_available = True if left_select.old_name in main.schema else False
            for right_select in join_input.right_select.renames:
                right_select.is_available = True if right_select.old_name in right.schema else False
            return main.join(
                join_input=join_input,
                auto_generate_selection=join_settings.auto_generate_selection,
                verify_integrity=False,
                other=right,
            )

        def schema_callback():
            j_copy = JoinInputManager(join_settings.join_input)
            node = self.get_node(node_id=join_settings.node_id)
            return calculate_join_schema(
                j_copy,
                left_schema=node.node_inputs.main_inputs[0].schema,
                right_schema=node.node_inputs.right_input.schema,
                auto_generate_selection=join_settings.auto_generate_selection,
            )

        self.add_node_step(
            node_id=join_settings.node_id,
            function=_func,
            input_columns=[],
            node_type="join",
            setting_input=join_settings,
            input_node_ids=join_settings.depending_on_ids,
            schema_callback=schema_callback,
        )
        return self

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
            # Imports are deferred to runtime: importing flowfile_core.artifacts
            # at module top would trigger Alembic migrations and SQLAlchemy
            # engine setup (~3.5s), slowing every flow_graph import — including
            # CLI startup. Keep these inside _func.
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

            # Validate model_type and hyperparameters early so the user gets
            # a clear error from core, not a worker-side stack trace.
            trainer = get_trainer(settings.model_type)
            try:
                trainer.params_class(**settings.params)
            except Exception as e:
                raise ValueError(f"Train Model: invalid params for model_type={settings.model_type!r}: {e}") from e

            # Always write the model to a flow-scoped path keyed off this node's id;
            # downstream Apply Model nodes in this flow read from there, no catalog
            # required. The same path is used as the staging path when publishing
            # so we only fit once.
            flow_path = ml_flow_model_path(self.flow_id, train_settings.node_id)
            flow_path.parent.mkdir(parents=True, exist_ok=True)

            prepared = None
            owner_id = train_settings.user_id or get_local_user_id() or 1
            staging_path = flow_path
            storage_backend = get_storage_backend()

            if settings.publish_to_catalog:
                # If the flow has a path on disk but no registration yet,
                # auto-register it (idempotently — same mechanism the open/save
                # routes use). This routes scratch flows under "General >
                # Unnamed Flows" / "Local Flows" so artifacts have a stable
                # lineage without forcing the user to explicitly register first.
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
                prepare_request = PrepareUploadRequest(
                    name=settings.model_name,
                    source_registration_id=registration_id,
                    namespace_id=settings.namespace_id,
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
                    # v1 only supports the shared-filesystem backend; S3 needs a
                    # presigned-URL path on the worker which we haven't wired yet.
                    with get_db_context() as db:
                        ArtifactService(db, storage_backend).delete_artifact(prepared.artifact_id)
                    raise ValueError(
                        "Train Model currently requires the filesystem artifact backend "
                        "(FLOWFILE_ARTIFACT_STORAGE=filesystem). S3 support is not implemented."
                    )
                # Train into the catalog staging path; we'll copy to the flow
                # path after success so finalize_upload (which moves the
                # staging file to the permanent location) still works.
                staging_path = Path(prepared.path)

            node = self.get_node(node_id=train_settings.node_id)
            flow_path_written = False
            try:
                fetcher = MLTrainFetcher(
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
                    # The staging file is also our flow-scoped copy. Atomically
                    # replace flow_path (write to .tmp, then os.replace) so a
                    # concurrent Apply Model reader can't see a half-written
                    # file. Done before finalize_upload (which moves the
                    # staging file away).
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
                    # Roll back the pending row on any failure so the user
                    # doesn't see ghost artifacts; subsequent re-runs auto-clean
                    # pending rows too.
                    with get_db_context() as db:
                        try:
                            ArtifactService(db, storage_backend).delete_artifact(prepared.artifact_id)
                        except Exception:
                            logger.exception("Failed to roll back pending artifact %s", prepared.artifact_id)
                    # Also roll back the flow_path copy if we wrote it; otherwise
                    # the next Apply Model run could quietly use the artifact
                    # whose catalog row we just deleted.
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

            # Surface the trained model in the node's Artifacts tab + canvas badge.
            # Re-runs replace any prior entry rather than accumulating duplicates.
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
                    artifact = ArtifactService(db, storage_backend).get_artifact_by_name(
                        name=settings.model_name,
                        namespace_id=settings.namespace_id,
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
            fetcher = MLApplyFetcher(
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
            # source='upstream' lets us read the trainer's declared output_dtype
            # so a future non-Float64 trainer (e.g. classification) gets the
            # right schema. source='catalog' falls back to Float64 — resolving
            # via the catalog DB at schema-resolve time would be too eager.
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

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_wait_for(self, settings: input_schema.NodeWaitFor) -> "FlowGraph":
        """Adds a Wait For node — passes the left input through and waits on the right.

        Two distinct input handles like Join: connect the data path to the left
        and the dependency node (e.g. Train Model) to the right. The right
        input's data is discarded; only its completion gates this node.
        """

        def _func(main: FlowDataEngine, right: FlowDataEngine) -> FlowDataEngine:
            # *right* is intentionally unused — its only job is to make sure
            # the framework waits for the dependency node to finish.
            del right
            return main

        def schema_callback():
            node = self.get_node(settings.node_id)
            if node.node_inputs.main_inputs:
                return node.node_inputs.main_inputs[0].schema
            return []

        self.add_node_step(
            node_id=settings.node_id,
            function=_func,
            input_columns=[],
            node_type="wait_for",
            setting_input=settings,
            schema_callback=schema_callback,
            input_node_ids=settings.depending_on_ids,
        )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_fuzzy_match(self, fuzzy_settings: input_schema.NodeFuzzyMatch) -> "FlowGraph":
        """Adds a fuzzy matching node to join data on approximate string matches.

        Args:
            fuzzy_settings: The settings for the fuzzy match operation.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(main: FlowDataEngine, right: FlowDataEngine) -> FlowDataEngine:
            node = self.get_node(node_id=fuzzy_settings.node_id)
            if self.execution_location == "local":
                return main.fuzzy_join(
                    fuzzy_match_input=deepcopy(fuzzy_settings.join_input),
                    other=right,
                    node_logger=self.flow_logger.get_node_logger(fuzzy_settings.node_id),
                )

            f = main.start_fuzzy_join(
                fuzzy_match_input=deepcopy(fuzzy_settings.join_input),
                other=right,
                file_ref=node.hash,
                flow_id=self.flow_id,
                node_id=fuzzy_settings.node_id,
            )
            logger.info("Started the fuzzy match action")
            node._fetch_cached_df = f  # Add to the node so it can be cancelled and fetch later if needed
            return FlowDataEngine(f.get_result())

        def schema_callback():
            fm_input_copy = FuzzyMatchInputManager(
                fuzzy_settings.join_input
            )  # Deepcopy create an unique object per func
            node = self.get_node(node_id=fuzzy_settings.node_id)
            return calculate_fuzzy_match_schema(
                fm_input_copy,
                left_schema=node.node_inputs.main_inputs[0].schema,
                right_schema=node.node_inputs.right_input.schema,
            )

        self.add_node_step(
            node_id=fuzzy_settings.node_id,
            function=_func,
            input_columns=[],
            node_type="fuzzy_match",
            setting_input=fuzzy_settings,
            input_node_ids=fuzzy_settings.depending_on_ids,
            schema_callback=schema_callback,
        )

        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_text_to_rows(self, node_text_to_rows: input_schema.NodeTextToRows) -> "FlowGraph":
        """Adds a node that splits cell values into multiple rows.

        This is useful for un-nesting data where a single field contains multiple
        values separated by a delimiter.

        Args:
            node_text_to_rows: The settings object that specifies the column to split
                and the delimiter to use.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(table: FlowDataEngine) -> FlowDataEngine:
            return table.split(node_text_to_rows.text_to_rows_input)

        self.add_node_step(
            node_id=node_text_to_rows.node_id,
            function=_func,
            node_type="text_to_rows",
            setting_input=node_text_to_rows,
            input_node_ids=[node_text_to_rows.depending_on_id],
        )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_window_functions(self, settings: input_schema.NodeWindowFunctions) -> "FlowGraph":
        """Adds a window-functions node (rolling, cumulative, rank, tile).

        Args:
            settings: The settings for the window-functions operation.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return fl.do_window_functions(settings.window_input, False)

        self.add_node_step(
            node_id=settings.node_id,
            function=_func,
            node_type="window_functions",
            setting_input=settings,
            input_node_ids=[settings.depending_on_id],
        )

        node = self.get_node(settings.node_id)

        def schema_callback():
            depends_on = node.node_inputs.main_inputs[0]
            input_schema_list = list(depends_on.schema)
            input_types = {s.name: s.data_type for s in depends_on.schema}
            output_schema = list(input_schema_list)
            for w in settings.window_input.window_functions:
                src_type = input_types.get(w.column) if w.column else None
                out_type = w.output_type or transform_schema.get_window_output_type(w.function, src_type)
                if out_type is None:
                    out_type = src_type or "Float64"
                output_schema.append(
                    FlowfileColumn.from_input(data_type=out_type, column_name=w.new_column_name)
                )
            return output_schema

        node.schema_callback = schema_callback
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_sort(self, sort_settings: input_schema.NodeSort) -> "FlowGraph":
        """Adds a node to sort the data based on one or more columns.

        Args:
            sort_settings: The settings for the sort operation.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(table: FlowDataEngine) -> FlowDataEngine:
            return table.do_sort(sort_settings.sort_input)

        self.add_node_step(
            node_id=sort_settings.node_id,
            function=_func,
            node_type="sort",
            setting_input=sort_settings,
            input_node_ids=[sort_settings.depending_on_id],
        )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_sample(self, sample_settings: input_schema.NodeSample) -> "FlowGraph":
        """Adds a node to take a random or top-N sample of the data.

        Args:
            sample_settings: The settings object specifying the size of the sample.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(table: FlowDataEngine) -> FlowDataEngine:
            return table.get_sample(sample_settings.sample_size)

        self.add_node_step(
            node_id=sample_settings.node_id,
            function=_func,
            node_type="sample",
            setting_input=sample_settings,
            input_node_ids=[sample_settings.depending_on_id],
        )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_random_split(self, settings: input_schema.NodeRandomSplit) -> "FlowGraph":
        """Adds a node that randomly partitions rows into N labeled outputs.

        Returns a ``NamedOutputs``; the framework unpacks it into
        ``_named_outputs`` so each split is reachable via its own output handle.

        Args:
            settings: The settings object specifying the splits and optional seed.

        Returns:
            The `FlowGraph` instance for method chaining.
        """
        from flowfile_core.flowfile.flow_node.multi_output import NamedOutputs

        def _func(table: FlowDataEngine) -> NamedOutputs:
            split_pairs = [(s.name, s.percentage) for s in settings.splits]
            if self.execution_location == "local":
                return table.random_split(split_pairs, settings.seed)
            return table.random_split_external(
                split_pairs,
                settings.seed,
                flow_id=self.flow_id,
                node_id=settings.node_id,
            )

        self.add_node_step(
            node_id=settings.node_id,
            function=_func,
            node_type="random_split",
            setting_input=settings,
            input_node_ids=[settings.depending_on_id],
        )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_record_id(self, record_id_settings: input_schema.NodeRecordId) -> "FlowGraph":
        """Adds a node to create a new column with a unique ID for each record.

        Args:
            record_id_settings: The settings object specifying the name of the
                new record ID column.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(table: FlowDataEngine) -> FlowDataEngine:
            return table.add_record_id(record_id_settings.record_id_input)

        self.add_node_step(
            node_id=record_id_settings.node_id,
            function=_func,
            node_type="record_id",
            setting_input=record_id_settings,
            input_node_ids=[record_id_settings.depending_on_id],
        )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_dynamic_rename(self, settings: input_schema.NodeDynamicRename) -> "FlowGraph":
        """Adds a node that renames many columns at once via a single rule.

        Supports prefix, suffix, formula-based, and first-row renaming across all
        columns, a specific list of columns, or every column of a given data type.
        In `first_row` mode the first row is dropped from the output after its
        values are promoted to column headers.

        Args:
            settings: The dynamic rename configuration.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        def _func(table: FlowDataEngine) -> FlowDataEngine:
            return table.apply_dynamic_rename(settings.dynamic_rename_input)

        self.add_node_step(
            node_id=settings.node_id,
            function=_func,
            node_type="dynamic_rename",
            setting_input=settings,
            input_node_ids=[settings.depending_on_id],
        )
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_select(self, select_settings: input_schema.NodeSelect) -> "FlowGraph":
        """Adds a node to select, rename, reorder, or drop columns.

        Args:
            select_settings: The settings for the select operation.

        Returns:
            The `FlowGraph` instance for method chaining.
        """

        select_cols = select_settings.select_input
        drop_cols = tuple(s.old_name for s in select_settings.select_input)

        def _func(table: FlowDataEngine) -> FlowDataEngine:
            input_cols = set(f.name for f in table.schema)
            ids_to_remove = []
            for i, select_col in enumerate(select_cols):
                if select_col.data_type is None:
                    select_col.data_type = table.get_schema_column(select_col.old_name).data_type
                if select_col.old_name not in input_cols:
                    select_col.is_available = False
                    if not select_col.keep:
                        ids_to_remove.append(i)
                else:
                    select_col.is_available = True
            ids_to_remove.reverse()
            for i in ids_to_remove:
                v = select_cols.pop(i)
                del v
            return table.do_select(
                select_inputs=transform_schema.SelectInputs(select_cols), keep_missing=select_settings.keep_missing
            )

        self.add_node_step(
            node_id=select_settings.node_id,
            function=_func,
            input_columns=[],
            node_type="select",
            drop_columns=list(drop_cols),
            setting_input=select_settings,
            input_node_ids=[select_settings.depending_on_id],
        )
        return self

    @property
    def graph_has_functions(self) -> bool:
        """Checks if the graph has any nodes."""
        return len(self._node_ids) > 0

    def delete_node(self, node_id: int | str):
        """Deletes a node from the graph and updates all its connections.

        Args:
            node_id: The ID of the node to delete.

        Raises:
            Exception: If the node with the given ID does not exist.
        """
        logger.info(f"Starting deletion of node with ID: {node_id}")

        node = self._node_db.get(node_id)
        if node:
            logger.info(f"Found node: {node_id}, processing deletion")
            group_id = getattr(node.setting_input, "group_id", None)

            lead_to_steps: list[FlowNode] = node.leads_to_nodes
            logger.debug(f"Node {node_id} leads to {len(lead_to_steps)} other nodes")

            if len(lead_to_steps) > 0:
                for lead_to_step in lead_to_steps:
                    logger.debug(f"Deleting input node {node_id} from dependent node {lead_to_step}")
                    lead_to_step.delete_input_node(node_id, complete=True)

            if not node.is_start:
                depends_on: list[FlowNode] = node.node_inputs.get_all_inputs()
                logger.debug(f"Node {node_id} depends on {len(depends_on)} other nodes")

                for depend_on in depends_on:
                    logger.debug(f"Removing lead_to reference {node_id} from node {depend_on}")
                    depend_on.delete_lead_to_node(node_id)

            self._node_db.pop(node_id)
            logger.debug(f"Successfully removed node {node_id} from node_db")
            del node
            logger.info("Node object deleted")
            # Drop a group that just lost its last member (keep it if it still holds sub-groups).
            if (
                group_id is not None
                and group_id in self._groups
                and not self._member_node_ids(group_id)
                and not self._child_group_ids(group_id)
            ):
                self._groups.pop(group_id, None)
        else:
            logger.error(f"Failed to find node with id {node_id}")
            raise Exception(f"Node with id {node_id} does not exist")

    @property
    def graph_has_input_data(self) -> bool:
        """Checks if the graph has an initial input data source."""
        return self._input_data is not None

    def add_node_step(
        self,
        node_id: int | str,
        function: Callable,
        input_columns: list[str] = None,
        output_schema: list[FlowfileColumn] = None,
        node_type: str = None,
        drop_columns: list[str] = None,
        renew_schema: bool = True,
        setting_input: Any = None,
        cache_results: bool = None,
        schema_callback: Callable = None,
        input_node_ids: list[int] = None,
    ) -> FlowNode:
        """The core method for adding or updating a node in the graph.

        Args:
            node_id: The unique ID for the node.
            function: The core processing function for the node.
            input_columns: A list of input column names required by the function.
            output_schema: A predefined schema for the node's output.
            node_type: A string identifying the type of node (e.g., 'filter', 'join').
            drop_columns: A list of columns to be dropped after the function executes.
            renew_schema: If True, the schema is recalculated after execution.
            setting_input: A configuration object containing settings for the node.
            cache_results: If True, the node's results are cached for future runs.
            schema_callback: A function that dynamically calculates the output schema.
            input_node_ids: A list of IDs for the nodes that this node depends on.

        Returns:
            The created or updated FlowNode object.
        """
        output_field_config = getattr(setting_input, "output_field_config", None) if setting_input else None

        logger.info(
            f"add_node_step: node_id={node_id}, node_type={node_type}, "
            f"has_setting_input={setting_input is not None}, "
            f"has_output_field_config={output_field_config is not None}, "
            f"config_enabled={output_field_config.enabled if output_field_config else False}, "
            f"has_schema_callback={schema_callback is not None}"
        )

        # IMPORTANT: Always create wrapped callback if output_field_config exists (even if enabled=False)
        # This ensures nodes like PolarsCode get a schema callback when output_field_config is defined
        if output_field_config:
            if output_field_config.enabled:
                logger.info(
                    f"add_node_step: Creating/wrapping schema_callback for node {node_id} with output_field_config "
                    f"(validation_mode={output_field_config.validation_mode_behavior}, "
                    f"{len(output_field_config.fields)} fields, "
                    f"base_callback={'present' if schema_callback else 'None'})"
                )
            else:
                logger.debug(f"add_node_step: output_field_config present for node {node_id} but disabled")

            schema_callback = create_schema_callback_with_output_config(schema_callback, output_field_config)
            logger.info(
                f"add_node_step: schema_callback {'created' if schema_callback else 'failed'} for node {node_id}"
            )

        existing_node = self.get_node(node_id)
        if existing_node is not None:
            if existing_node.node_type != node_type:
                self.delete_node(existing_node.node_id)
                existing_node = None
        if existing_node:
            input_nodes = existing_node.all_inputs
        elif input_node_ids is not None:
            input_nodes = [self.get_node(node_id) for node_id in input_node_ids]
        else:
            input_nodes = None
        if isinstance(input_columns, str):
            input_columns = [input_columns]
        if (
            input_nodes is not None
            or function.__name__ in ("placeholder", "analysis_preparation")
            or node_type in ("cloud_storage_reader", "catalog_reader", "polars_lazy_frame", "input_data")
        ):
            if not existing_node:
                node = FlowNode(
                    node_id=node_id,
                    function=function,
                    output_schema=output_schema,
                    input_columns=input_columns,
                    drop_columns=drop_columns,
                    renew_schema=renew_schema,
                    setting_input=setting_input,
                    node_type=node_type,
                    name=function.__name__,
                    schema_callback=schema_callback,
                    parent_uuid=self.uuid,
                )
            else:
                existing_node.update_node(
                    function=function,
                    output_schema=output_schema,
                    input_columns=input_columns,
                    drop_columns=drop_columns,
                    setting_input=setting_input,
                    schema_callback=schema_callback,
                )
                node = existing_node
        else:
            raise Exception("No data initialized")
        self._node_db[node_id] = node
        self._node_ids.append(node_id)
        # Give the node a callable that returns the current flow parameters so
        # that lazy schema prediction (_predicted_data_getter) can substitute
        # ${...} refs. Using a callable (rather than a copy of the dict) means
        # the node always reads the LATEST parameters, whether they were set via
        # the flow_settings.setter or mutated directly on flow_settings.parameters.
        _graph = self

        def _get_params() -> dict[str, str]:
            return {p.name: p.default_value for p in (_graph.flow_settings.parameters or [])}

        node._params_getter = _get_params
        return node

    def add_include_cols(self, include_columns: list[str]):
        """Adds columns to both the input and output column lists.

        Args:
            include_columns: A list of column names to include.
        """
        for column in include_columns:
            if column not in self._input_cols:
                self._input_cols.append(column)
            if column not in self._output_cols:
                self._output_cols.append(column)
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_output(self, output_file: input_schema.NodeOutput):
        """Adds an output node to write the final data to a destination.

        Args:
            output_file: The settings for the output file.
        """

        def _func(df: FlowDataEngine):
            if self.execution_location == "local":
                df.output(
                    output_fs=output_file.output_settings,
                    flow_id=self.flow_id,
                    node_id=output_file.node_id,
                    execute_remote=False,
                )
                return df
            output_fs = output_file.output_settings
            node = self.get_node(output_file.node_id)
            writer = ExternalOutputWriter(
                lf=df.data_frame,
                data_type=output_fs.file_type,
                path=output_fs.abs_file_path,
                write_mode=output_fs.write_mode,
                sheet_name=output_fs.sheet_name,
                delimiter=output_fs.delimiter,
                flow_id=self.flow_id,
                node_id=output_file.node_id,
                wait_on_completion=False,
            )
            node._fetch_cached_df = writer
            writer.get_result()
            return df

        def schema_callback():
            input_node: FlowNode = self.get_node(output_file.node_id).node_inputs.main_inputs[0]

            return input_node.schema

        input_node_id = output_file.depending_on_id if hasattr(output_file, "depending_on_id") else None
        self.add_node_step(
            node_id=output_file.node_id,
            function=_func,
            input_columns=[],
            node_type="output",
            setting_input=output_file,
            schema_callback=schema_callback,
            input_node_ids=[input_node_id],
        )

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_api_response(self, api_response: input_schema.NodeApiResponse):
        """Adds an API-response sink node.

        The node is a pass-through marker: its result equals its input. When the flow
        is published as an HTTP API endpoint, the endpoint reads this node's result
        and serializes it as the response body. Behaves like an output node so its
        result is always materialized locally.

        Args:
            api_response: The settings for the API-response node.
        """

        def _func(df: FlowDataEngine):
            return df

        def schema_callback():
            input_node: FlowNode = self.get_node(api_response.node_id).node_inputs.main_inputs[0]
            return input_node.schema

        input_node_id = api_response.depending_on_id if hasattr(api_response, "depending_on_id") else None
        self.add_node_step(
            node_id=api_response.node_id,
            function=_func,
            input_columns=[],
            node_type="api_response",
            setting_input=api_response,
            schema_callback=schema_callback,
            input_node_ids=[input_node_id],
        )

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_catalog_reader(self, node_catalog_reader: input_schema.NodeCatalogReader):
        """Adds a node that reads a table from the catalog.

        Resolves the catalog table by ID (or name + namespace) and reads
        the materialized Parquet file.  When ``sql_query`` is set, executes
        the SQL against all catalog Delta tables instead.
        """

        if node_catalog_reader.sql_query:
            is_virtual_optimized = self._add_catalog_sql_reader(node_catalog_reader)
        else:
            is_virtual_optimized = self._add_catalog_table_reader(node_catalog_reader)
        node_catalog_reader.is_virtual_optimized = is_virtual_optimized

    def _add_catalog_sql_reader(self, node_catalog_reader: input_schema.NodeCatalogReader) -> bool | None:
        """Execute a SQL query against all catalog tables (physical + virtual).

        Returns:
            Whether all referenced virtual tables are optimized, or None if no virtual tables.
        """

        sql_code = node_catalog_reader.sql_query
        resolved = _resolve_catalog_sql_tables(node_catalog_reader.node_id)
        table_paths = resolved.table_paths
        virtual_tables = resolved.virtual_tables

        def _func() -> FlowDataEngine:
            if not table_paths and not virtual_tables:
                raise ValueError("No catalog tables available to query")
            ctx = pl.SQLContext()
            for name, path in table_paths.items():
                ctx.register(name, pl.scan_delta(path))
            for name, (is_opt, ser_lf, tid, stv) in virtual_tables.items():
                ctx.register(
                    name,
                    _resolve_virtual_table(
                        is_opt,
                        ser_lf,
                        tid,
                        node_logger=self.flow_logger.get_node_logger(node_catalog_reader.node_id),
                        run_location=self.execution_location,
                        source_table_versions=stv,
                    ),
                )
            return FlowDataEngine(ctx.execute(sql_code))

        # todo: There are quite some round-trips happening here because the Flowgraph tries to predict the schema.
        is_virtual_optimized: bool | None = None
        if virtual_tables:
            is_virtual_optimized = all(is_opt for is_opt, _, _, _ in virtual_tables.values())

        self.add_node_step(
            node_id=node_catalog_reader.node_id,
            function=_func,
            input_columns=[],
            node_type="catalog_reader",
            setting_input=node_catalog_reader,
        )
        node = self.get_node(node_catalog_reader.node_id)
        self.add_node_to_starting_list(node)

        try:
            validate_sql_query(sql_code)
        except Exception as e:
            node.results.errors = str(e)

        return is_virtual_optimized

    def _add_catalog_table_reader(self, node_catalog_reader: input_schema.NodeCatalogReader) -> bool | None:
        """Read a single table from the catalog (physical or virtual).

        Returns:
            Whether the virtual table is optimized, or None if not a virtual table.
        """

        info = _resolve_catalog_table_info(node_catalog_reader)

        is_virtual_optimized: bool | None = info.is_optimized if info.table_type == "virtual" else None

        resolved_path = info.file_path
        delta_version = node_catalog_reader.delta_version
        _table_type = info.table_type
        _serialized_lf = info.serialized_lf
        _is_optimized = info.is_optimized
        _catalog_table_id = node_catalog_reader.catalog_table_id
        _source_table_versions = info.source_table_versions

        def _func() -> FlowDataEngine:
            if _table_type == "virtual":
                return FlowDataEngine(
                    _resolve_virtual_table(
                        _is_optimized,
                        _serialized_lf,
                        _catalog_table_id,
                        node_logger=self.flow_logger.get_node_logger(node_catalog_reader.node_id),
                        run_location=self.execution_location,
                        source_table_versions=_source_table_versions,
                    )
                )

            if not resolved_path:
                raise ValueError("Catalog table could not be resolved — no file path found")
            if is_delta_table(resolved_path):
                scan_kwargs = {}
                if delta_version is not None:
                    scan_kwargs["version"] = delta_version
                return FlowDataEngine(pl.scan_delta(resolved_path, **scan_kwargs))
            return FlowDataEngine(pl.scan_parquet(resolved_path))

        self.add_node_step(
            node_id=node_catalog_reader.node_id,
            function=_func,
            input_columns=[],
            node_type="catalog_reader",
            setting_input=node_catalog_reader,
        )
        node = self.get_node(node_catalog_reader.node_id)
        self.add_node_to_starting_list(node)
        return is_virtual_optimized

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_catalog_writer(self, node_catalog_writer: input_schema.NodeCatalogWriter):
        """Adds a node that writes its input to the catalog as a Delta table or virtual table."""

        def _func(df: FlowDataEngine) -> FlowDataEngine:
            settings = node_catalog_writer.catalog_write_settings
            if not settings.table_name:
                raise ValueError("Catalog writer requires a table name")
            if settings.write_mode == "virtual":
                return _handle_virtual_table_write(self, node_catalog_writer, df)
            return _handle_physical_table_write(self, node_catalog_writer, df)

        def schema_callback():
            input_node: FlowNode = self.get_node(node_catalog_writer.node_id).node_inputs.main_inputs[0]
            return input_node.schema

        input_node_id = node_catalog_writer.depending_on_id if hasattr(node_catalog_writer, "depending_on_id") else None
        self.add_node_step(
            node_id=node_catalog_writer.node_id,
            function=_func,
            input_columns=[],
            node_type="catalog_writer",
            setting_input=node_catalog_writer,
            schema_callback=schema_callback,
            input_node_ids=[input_node_id],
        )

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_database_writer(self, node_database_writer: input_schema.NodeDatabaseWriter):
        """Adds a node to write data to a database.

        Args:
            node_database_writer: The settings for the database writer node.
        """

        node_type = "database_writer"
        database_settings: input_schema.DatabaseWriteSettings = node_database_writer.database_write_settings

        def _func(df: FlowDataEngine):
            # Resolve the connection lazily so opening/undoing a flow never requires
            # the current session to own the connection (runs under the node's
            # ``user_id`` — the flow owner at execution time).
            _, encrypted_password, database_reference_settings = _resolve_database_credentials(
                database_settings, node_database_writer.user_id
            )
            df.lazy = True
            database_external_write_settings = (
                sql_models.DatabaseExternalWriteSettings.create_from_from_node_database_writer(
                    node_database_writer=node_database_writer,
                    password=encrypted_password,
                    table_name=(
                        database_settings.schema_name + "." + database_settings.table_name
                        if database_settings.schema_name
                        else database_settings.table_name
                    ),
                    database_reference_settings=(
                        database_reference_settings if database_settings.connection_mode == "reference" else None
                    ),
                    lf=df.data_frame,
                )
            )
            external_database_writer = ExternalDatabaseWriter(
                database_external_write_settings, wait_on_completion=False
            )
            node._fetch_cached_df = external_database_writer
            external_database_writer.get_result()
            return df

        def schema_callback():
            input_node: FlowNode = self.get_node(node_database_writer.node_id).node_inputs.main_inputs[0]
            return input_node.schema

        self.add_node_step(
            node_id=node_database_writer.node_id,
            function=_func,
            input_columns=[],
            node_type=node_type,
            setting_input=node_database_writer,
            schema_callback=schema_callback,
        )
        node = self.get_node(node_database_writer.node_id)

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_database_reader(self, node_database_reader: input_schema.NodeDatabaseReader):
        """Adds a node to read data from a database.

        Args:
            node_database_reader: The settings for the database reader node.
        """

        logger.info("Adding database reader")
        node_type = "database_reader"
        database_settings: input_schema.DatabaseSettings = node_database_reader.database_settings

        # Resolve the connection lazily so opening/undoing a flow never requires
        # the current session to own the connection. Memoized so ``_func`` and
        # ``schema_callback`` share a single lookup; the lock matters because the
        # schema callback runs on a background thread (``SingleExecutionFuture``)
        # while ``_func`` runs on the execution thread. Runs under the node's
        # ``user_id`` (the flow owner at execution time).
        _creds: dict = {}
        _creds_lock = threading.Lock()

        def _get_creds():
            with _creds_lock:
                if "v" not in _creds:
                    _creds["v"] = _resolve_database_credentials(database_settings, node_database_reader.user_id)
                return _creds["v"]

        def _func():
            database_connection, encrypted_password, database_reference_settings = _get_creds()
            sql_source = BaseSqlSource(
                query=None if database_settings.query_mode == "table" else database_settings.query,
                table_name=database_settings.table_name,
                schema_name=database_settings.schema_name,
                fields=node_database_reader.fields,
            )

            # TODO: centralize this local SQL read with flowfile_worker's
            # /store_database_read_result path — both call pl.read_database_uri
            # and have drifted in shape (see schema_callback below too).
            if self.execution_location == "local":
                local_source = SqlSource(
                    connection_string=sql_utils.construct_sql_uri(
                        database_type=database_connection.database_type,
                        host=database_connection.host,
                        port=database_connection.port,
                        database=database_connection.database,
                        username=database_connection.username,
                        password=decrypt_secret(encrypted_password) if encrypted_password else None,
                    ),
                    query=None if database_settings.query_mode == "table" else database_settings.query,
                    table_name=database_settings.table_name,
                    schema_name=database_settings.schema_name,
                    fields=node_database_reader.fields,
                )
                fl = FlowDataEngine(local_source.get_pl_df())
                fl.lazy = True
                node_database_reader.fields = [c.get_minimal_field_info() for c in fl.schema]
                return fl

            database_external_read_settings = (
                sql_models.DatabaseExternalReadSettings.create_from_from_node_database_reader(
                    node_database_reader=node_database_reader,
                    password=encrypted_password,
                    query=sql_source.query,
                    database_reference_settings=(
                        database_reference_settings if database_settings.connection_mode == "reference" else None
                    ),
                )
            )

            external_database_fetcher = ExternalDatabaseFetcher(
                database_external_read_settings, wait_on_completion=False
            )
            node._fetch_cached_df = external_database_fetcher
            fl = FlowDataEngine(external_database_fetcher.get_result())
            node_database_reader.fields = [c.get_minimal_field_info() for c in fl.schema]
            return fl

        def schema_callback():
            # Prefer the schema cached on the node so opening a saved flow renders
            # columns without a live connection. Fall back to the connection only
            # when fields were never captured (failures here are caught per-node).
            if node_database_reader.fields:
                return [FlowfileColumn.from_input(f.name, f.data_type) for f in node_database_reader.fields]
            database_connection, encrypted_password, _ = _get_creds()
            sql_source = SqlSource(
                connection_string=sql_utils.construct_sql_uri(
                    database_type=database_connection.database_type,
                    host=database_connection.host,
                    port=database_connection.port,
                    database=database_connection.database,
                    username=database_connection.username,
                    password=decrypt_secret(encrypted_password) if encrypted_password else None,
                ),
                query=None if database_settings.query_mode == "table" else database_settings.query,
                table_name=database_settings.table_name,
                schema_name=database_settings.schema_name,
                fields=node_database_reader.fields,
            )
            return sql_source.get_schema()

        node = self.get_node(node_database_reader.node_id)
        if node:
            # Persist so the lightweight callback survives the reset() that setting_input triggers.
            node.user_provided_schema_callback = schema_callback
            node.schema_callback = schema_callback
            node.node_type = node_type
            node.name = node_type
            node.function = _func
            node.setting_input = node_database_reader
            node.node_settings.cache_results = node_database_reader.cache_results
            self.add_node_to_starting_list(node)
        else:
            node = FlowNode(
                node_database_reader.node_id,
                function=_func,
                setting_input=node_database_reader,
                name=node_type,
                node_type=node_type,
                parent_uuid=self.uuid,
                schema_callback=schema_callback,
            )
            self._node_db[node_database_reader.node_id] = node
            self.add_node_to_starting_list(node)
            self._node_ids.append(node_database_reader.node_id)

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_kafka_source(self, node_kafka_source: input_schema.NodeKafkaSource):
        """Adds a node to read data from a Kafka or Redpanda topic.

        Follows the same pattern as add_database_reader: offloads consumption
        to the worker, which writes an IPC temp file and returns a serialized
        LazyFrame reference. Offset tracking is handled by Kafka consumer groups.

        Args:
            node_kafka_source: The settings for the Kafka source node.
        """

        logger.info("Adding kafka source")
        node_type = "kafka_source"
        kafka_settings = node_kafka_source.kafka_settings

        # Settings updates may echo back ``fields`` cached from a previous topic /
        # format / connection (the UI clears them, but programmatic callers may
        # not). Stale fields would make ``schema_callback`` report the old topic's
        # columns, so drop them whenever a schema-affecting setting changed.
        # Open/undo replays keep their fields: the replayed settings match the
        # node's previous ones (or the prior node is just a promise).
        prior_settings = getattr(self.get_node(node_kafka_source.node_id), "setting_input", None)
        if node_kafka_source.fields and isinstance(prior_settings, input_schema.NodeKafkaSource):
            prior_kafka = prior_settings.kafka_settings
            if (
                prior_kafka.topic_name != kafka_settings.topic_name
                or prior_kafka.value_format != kafka_settings.value_format
                or prior_kafka.kafka_connection_id != kafka_settings.kafka_connection_id
                or prior_kafka.kafka_connection_name != kafka_settings.kafka_connection_name
            ):
                node_kafka_source.fields = None

        # Resolve the connection lazily so opening/undoing a flow never requires
        # the current session to own the connection. Memoized so ``_func`` and
        # ``schema_callback`` share a single lookup; the lock matters because the
        # schema callback runs on a background thread (``SingleExecutionFuture``)
        # while ``_func`` runs on the execution thread. Runs under the node's
        # ``user_id`` (the flow owner at execution time).
        _read_settings: dict = {}
        _read_settings_lock = threading.Lock()

        def _get_kafka_read_settings() -> KafkaReadSettings:
            with _read_settings_lock:
                if "v" not in _read_settings:
                    with get_db_context() as db:
                        db_conn = get_kafka_connection(
                            db, kafka_settings.kafka_connection_id, node_kafka_source.user_id
                        )
                        if db_conn is None:
                            if kafka_settings.kafka_connection_name:
                                db_conn = get_kafka_connection_by_name(
                                    db, kafka_settings.kafka_connection_name, node_kafka_source.user_id
                                )
                            if db_conn is None:
                                raise HTTPException(status_code=400, detail="Kafka connection not found")
                        consumer_config = build_consumer_config(db, db_conn, node_kafka_source.user_id)
                    _read_settings["v"] = KafkaReadSettings.from_consumer_config(
                        consumer_config,
                        topic=kafka_settings.topic_name,
                        value_format=kafka_settings.value_format,
                        group_id=kafka_settings.sync_name
                        or f"flowfile-{node_kafka_source.flow_id}-node-{node_kafka_source.node_id}",
                        start_offset=kafka_settings.start_offset,
                        max_messages=kafka_settings.max_messages,
                        poll_timeout_seconds=kafka_settings.poll_timeout_seconds,
                        flowfile_flow_id=node_kafka_source.flow_id,
                        flowfile_node_id=node_kafka_source.node_id,
                    )
                return _read_settings["v"]

        def _func():
            kafka_read_settings = _get_kafka_read_settings()
            if self.execution_location == "local":
                # Local execution — consume directly in-process with spill-to-IPC
                import tempfile

                fd, spill_file = tempfile.mkstemp(suffix=".arrow", prefix="kafka_")
                os.close(fd)
                result, kafka_result = read_kafka_source(
                    kafka_read_settings,
                    commit=False,
                    decrypt_fn=_decrypt_fn,
                    spill_path=spill_file,
                )
                lf = result if isinstance(result, pl.LazyFrame) else result.lazy()
                fl = FlowDataEngine(lf)
                if kafka_result.messages_consumed > 0:
                    node._on_flow_complete = make_kafka_commit_callback(
                        kafka_read_settings,
                        kafka_result.new_offsets,
                        node_kafka_source.node_id,
                        self.flow_logger,
                        _decrypt_fn,
                    )
            else:
                # Remote execution — offload to worker (worker uses commit=False + sidecar)
                external_kafka_fetcher = ExternalKafkaFetcher(kafka_read_settings, wait_on_completion=False)
                node._fetch_cached_df = external_kafka_fetcher
                fl = FlowDataEngine(external_kafka_fetcher.get_result())
                offsets_data = fetch_kafka_offsets(external_kafka_fetcher.file_ref)
                if offsets_data and offsets_data.get("messages_consumed", 0) > 0:
                    node._on_flow_complete = make_kafka_commit_callback(
                        kafka_read_settings,
                        offsets_data["new_offsets"],
                        node_kafka_source.node_id,
                        self.flow_logger,
                        _decrypt_fn,
                    )
            # The worker DataFrame may have fewer columns than the inferred
            # schema (e.g. empty topic or starting at "latest"). Align to
            # the schema_callback result so downstream nodes see stable columns.
            expected_columns = schema_callback()
            fl = fl.align_to_schema(expected_columns)
            node_kafka_source.fields = [c.get_minimal_field_info() for c in fl.schema]
            return fl

        def _decrypt_fn(encrypted: str) -> str:
            return decrypt_secret(encrypted).get_secret_value()

        def schema_callback():
            # Prefer the schema cached on the node so opening a saved flow renders
            # columns without sampling the topic (a live connection). Sampling only
            # runs when fields were never captured (failures are caught per-node).
            if node_kafka_source.fields:
                return [FlowfileColumn.from_input(f.name, f.data_type) for f in node_kafka_source.fields]
            schema_pairs = infer_topic_schema(_get_kafka_read_settings(), sample_size=10, decrypt_fn=_decrypt_fn)
            # Since the schema callback takes quite some time, we only run the function once.
            if not schema_pairs:
                result = [
                    FlowfileColumn.from_input(column_name="_kafka_key", data_type="String"),
                    FlowfileColumn.from_input(column_name="_kafka_partition", data_type="Int64"),
                    FlowfileColumn.from_input(column_name="_kafka_offset", data_type="Int64"),
                    FlowfileColumn.from_input(column_name="_kafka_timestamp", data_type="Datetime"),
                ]
            else:
                result = [FlowfileColumn.create_from_polars_dtype(column_name=n, data_type=t) for n, t in schema_pairs]
            return result

        node = self.get_node(node_kafka_source.node_id)
        if node:
            node.user_provided_schema_callback = schema_callback
            node.schema_callback = schema_callback
            node.node_type = node_type
            node.name = node_type
            node.function = _func
            node.setting_input = node_kafka_source
            node.node_settings.cache_results = node_kafka_source.cache_results
            self.add_node_to_starting_list(node)
        else:
            node = FlowNode(
                node_kafka_source.node_id,
                function=_func,
                setting_input=node_kafka_source,
                name=node_type,
                node_type=node_type,
                parent_uuid=self.uuid,
                schema_callback=schema_callback,
            )
            self._node_db[node_kafka_source.node_id] = node
            self.add_node_to_starting_list(node)
            self._node_ids.append(node_kafka_source.node_id)

    def add_sql_source(self, external_source_input: input_schema.NodeExternalSource):
        """Adds a node that reads data from a SQL source.

        This is a convenience alias for `add_external_source`.

        Args:
            external_source_input: The settings for the external SQL source node.
        """
        logger.info("Adding sql source")
        self.add_external_source(external_source_input)

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_google_analytics_reader(self, node_ga_reader: input_schema.NodeGoogleAnalyticsReader) -> None:
        """Adds a node that reads from a Google Analytics 4 property.

        The actual API fetch (OAuth token refresh, ``run_report`` calls,
        pagination) is offloaded to the worker via ``ExternalGoogleAnalyticsFetcher``,
        so the core's event loop stays responsive. The ``schema_callback`` is
        derived locally from the selected metrics/dimensions — no network call
        is made during schema prediction, keeping downstream nodes lazy.
        """
        logger.info("Adding google analytics reader")
        node_type = "google_analytics_reader"
        ga_settings = node_ga_reader.google_analytics_settings

        def _build_worker_settings() -> WorkerGoogleAnalyticsReadSettings:
            # Connection resolution is deferred to run time so that *opening* or
            # *undoing* a flow never requires the current session to own the
            # connection (mirrors ``add_cloud_storage_reader``). It runs under
            # ``node_ga_reader.user_id`` — the flow owner at execution time.
            with get_db_context() as db:
                db_conn = get_ga_connection(db, ga_settings.ga_connection_name, node_ga_reader.user_id)
                if db_conn is None:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            f"Google Analytics connection '{ga_settings.ga_connection_name}' not found "
                            "or has not completed sign-in"
                        ),
                    )
                auth_method = db_conn.auth_method
                encrypted_credential = get_encrypted_credential(
                    db, ga_settings.ga_connection_name, node_ga_reader.user_id
                )
                if encrypted_credential is None:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            f"Google Analytics connection '{ga_settings.ga_connection_name}' has no stored credential"
                        ),
                    )
                # OAuth needs the per-instance client config; service accounts don't.
                oauth_cfg = get_google_oauth_config(db, node_ga_reader.user_id) if auth_method == "oauth" else None

            common_kwargs = dict(
                property_id=ga_settings.property_id,
                start_date=ga_settings.start_date,
                end_date=ga_settings.end_date,
                metrics=ga_settings.metrics,
                dimensions=ga_settings.dimensions,
                limit=ga_settings.limit,
                filters=[
                    WorkerGoogleAnalyticsFilter(
                        field=f.field,
                        operator=f.operator,
                        value=f.value,
                        case_sensitive=f.case_sensitive,
                    )
                    for f in ga_settings.filters
                ],
                order_bys=[
                    WorkerGoogleAnalyticsOrderBy(field=ob.field, descending=ob.descending)
                    for ob in ga_settings.order_bys
                ],
                flowfile_flow_id=node_ga_reader.flow_id,
                flowfile_node_id=node_ga_reader.node_id,
            )

            if auth_method == "service_account":
                return WorkerGoogleAnalyticsReadSettings(
                    auth_method="service_account",
                    service_account_key_encrypted=encrypted_credential,
                    **common_kwargs,
                )
            # ``oauth_cfg`` is only fetched for the oauth auth method; guard against
            # an unknown auth_method value reaching this branch with ``None``.
            if not oauth_cfg or not oauth_cfg["client_id"] or not oauth_cfg["client_secret"]:
                raise HTTPException(
                    status_code=500,
                    detail=(
                        "Google OAuth is not configured on this instance. Open Admin → Google OAuth "
                        "and paste your OAuth client credentials before running this flow."
                    ),
                )
            return WorkerGoogleAnalyticsReadSettings(
                auth_method="oauth",
                refresh_token_encrypted=encrypted_credential,
                oauth_client_id=oauth_cfg["client_id"],
                oauth_client_secret_encrypted=_encrypt_with_master_key(oauth_cfg["client_secret"]),
                **common_kwargs,
            )

        # Stamp the predicted schema onto the setting object now, so downstream
        # nodes can introspect columns without ever invoking ``_func`` (which
        # would trigger a worker → Google round-trip). ``derive_schema`` is
        # pure-Python and runs against the chosen metrics/dimensions only — no DB,
        # so it stays eager and keeps flow-open connection-free.
        predicted_columns = derive_schema(metrics=ga_settings.metrics, dimensions=ga_settings.dimensions)
        node_ga_reader.fields = [c.get_minimal_field_info() for c in predicted_columns]

        def _func() -> FlowDataEngine:
            fetcher = ExternalGoogleAnalyticsFetcher(_build_worker_settings(), wait_on_completion=False)
            node._fetch_cached_df = fetcher
            # ``get_result()`` returns a ``pl.LazyFrame`` deserialised from the
            # worker's Arrow IPC file — never collect on the core service.
            fl = FlowDataEngine(fetcher.get_result())
            # Align to the predicted schema so downstream nodes see stable columns
            # even when the report is empty. ``align_to_schema`` lowers to lazy
            # ``with_columns``/``select`` calls, so this stays lazy.
            return fl.align_to_schema(schema_callback())

        def schema_callback() -> list[FlowfileColumn]:
            # Prefer the cached placeholder so repeated schema lookups don't
            # re-walk the heuristic table. ``derive_schema`` is the fallback
            # for the (rare) case where ``fields`` got cleared.
            if node_ga_reader.fields:
                return [FlowfileColumn.from_input(f.name, f.data_type) for f in node_ga_reader.fields]
            return derive_schema(metrics=ga_settings.metrics, dimensions=ga_settings.dimensions)

        node = self.get_node(node_ga_reader.node_id)
        if node:
            node.schema_callback = schema_callback
            node.user_provided_schema_callback = schema_callback
            node.node_type = node_type
            node.name = node_type
            node.function = _func
            node.setting_input = node_ga_reader
            node.node_settings.cache_results = node_ga_reader.cache_results
            self.add_node_to_starting_list(node)
        else:
            node = FlowNode(
                node_ga_reader.node_id,
                function=_func,
                setting_input=node_ga_reader,
                name=node_type,
                node_type=node_type,
                parent_uuid=self.uuid,
                schema_callback=schema_callback,
            )
            node.user_provided_schema_callback = schema_callback
            self._node_db[node_ga_reader.node_id] = node
            self.add_node_to_starting_list(node)
            self._node_ids.append(node_ga_reader.node_id)

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_rest_api_reader(self, node_rest_api_reader: input_schema.NodeRestApiReader) -> None:
        """Adds a node that reads from a REST API.

        All network I/O (HTTP round-trips, pagination, retries) is offloaded to
        the worker via ``ExternalRestApiFetcher`` — the core never makes the
        external call. The credential is resolved to an encrypted token here
        (from the user's secret store, or an inline plaintext) and the worker
        decrypts it just-in-time. A generic API's columns are unknown until a
        response is fetched, so ``schema_callback`` returns the columns cached on
        the node by the "Fetch sample" action — empty until the user samples or
        runs, in which case the fetched frame defines the schema.
        """
        logger.info("Adding rest api reader")
        node_type = "rest_api_reader"
        auth = node_rest_api_reader.rest_api_settings.auth

        # Encrypt any *inline* plaintext credential eagerly and null it out so it is
        # never persisted on the node (a security guarantee, independent of who owns
        # the flow). The *by-name* secret-store lookup is deferred to run time so
        # opening/undoing a flow never requires the current session to own the
        # secret — it resolves under the node's ``user_id`` (the flow owner).
        _inline_encrypted = _encrypt_with_master_key(auth.secret) if (auth.secret and not auth.secret_name) else None
        auth.secret = None

        def _resolve_secret_encrypted() -> str | None:
            if _inline_encrypted is not None:
                return _inline_encrypted
            return resolve_auth_secret_encrypted(auth, node_rest_api_reader.user_id)

        def _func() -> FlowDataEngine:
            # All fetching happens in the worker; the core only orchestrates.
            fetcher = ExternalRestApiFetcher(
                build_rest_api_worker_settings(node_rest_api_reader, _resolve_secret_encrypted()),
                wait_on_completion=False,
            )
            node._fetch_cached_df = fetcher
            # ``get_result()`` returns a ``pl.LazyFrame`` deserialised from the
            # worker's Arrow IPC file — never collect on the core service.
            fl = FlowDataEngine(fetcher.get_result())
            cols = schema_callback()
            # Align to the sampled schema (if any) so downstream nodes see stable
            # columns; with no sample yet, the fetched frame defines the schema.
            if cols:
                return fl.align_to_schema(cols)
            node_rest_api_reader.fields = [c.get_minimal_field_info() for c in fl.schema]
            return fl

        def schema_callback() -> list[FlowfileColumn]:
            if node_rest_api_reader.fields:
                return [FlowfileColumn.from_input(f.name, f.data_type) for f in node_rest_api_reader.fields]
            return []

        node = self.get_node(node_rest_api_reader.node_id)
        if node:
            node.schema_callback = schema_callback
            node.user_provided_schema_callback = schema_callback
            node.node_type = node_type
            node.name = node_type
            node.function = _func
            node.setting_input = node_rest_api_reader
            node.node_settings.cache_results = node_rest_api_reader.cache_results
            self.add_node_to_starting_list(node)
        else:
            node = FlowNode(
                node_rest_api_reader.node_id,
                function=_func,
                setting_input=node_rest_api_reader,
                name=node_type,
                node_type=node_type,
                parent_uuid=self.uuid,
                schema_callback=schema_callback,
            )
            node.user_provided_schema_callback = schema_callback
            self._node_db[node_rest_api_reader.node_id] = node
            self.add_node_to_starting_list(node)
            self._node_ids.append(node_rest_api_reader.node_id)

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_cloud_storage_writer(self, node_cloud_storage_writer: input_schema.NodeCloudStorageWriter) -> None:
        """Adds a node to write data to a cloud storage provider.

        Args:
            node_cloud_storage_writer: The settings for the cloud storage writer node.
        """

        node_type = "cloud_storage_writer"

        def _func(df: FlowDataEngine):
            df.lazy = True
            execute_remote = self.execution_location != "local"
            cloud_connection_settings = get_cloud_connection_settings(
                connection_name=node_cloud_storage_writer.cloud_storage_settings.connection_name,
                user_id=node_cloud_storage_writer.user_id,
                auth_mode=node_cloud_storage_writer.cloud_storage_settings.auth_mode,
            )
            full_cloud_storage_connection = cloud_connection_settings
            if execute_remote:
                settings = get_cloud_storage_write_settings_worker_interface(
                    write_settings=node_cloud_storage_writer.cloud_storage_settings,
                    connection=full_cloud_storage_connection,
                    lf=df.data_frame,
                    user_id=node_cloud_storage_writer.user_id,
                    flowfile_node_id=node_cloud_storage_writer.node_id,
                    flowfile_flow_id=self.flow_id,
                )
                external_database_writer = ExternalCloudWriter(settings, wait_on_completion=False)
                node._fetch_cached_df = external_database_writer
                external_database_writer.get_result()
            else:
                cloud_storage_write_settings_internal = CloudStorageWriteSettingsInternal(
                    connection=full_cloud_storage_connection,
                    write_settings=node_cloud_storage_writer.cloud_storage_settings,
                )
                df.to_cloud_storage_obj(cloud_storage_write_settings_internal)
            return df

        def schema_callback():
            logger.info("Starting to run the schema callback for cloud storage writer")
            if self.get_node(node_cloud_storage_writer.node_id).is_correct:
                return self.get_node(node_cloud_storage_writer.node_id).node_inputs.main_inputs[0].schema
            else:
                return [FlowfileColumn.from_input(column_name="__error__", data_type="String")]

        self.add_node_step(
            node_id=node_cloud_storage_writer.node_id,
            function=_func,
            input_columns=[],
            node_type=node_type,
            setting_input=node_cloud_storage_writer,
            schema_callback=schema_callback,
            input_node_ids=[node_cloud_storage_writer.depending_on_id],
        )

        node = self.get_node(node_cloud_storage_writer.node_id)

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_cloud_storage_reader(self, node_cloud_storage_reader: input_schema.NodeCloudStorageReader) -> None:
        """Adds a cloud storage read node to the flow graph.

        Args:
            node_cloud_storage_reader: The settings for the cloud storage read node.
        """
        node_type = "cloud_storage_reader"
        logger.info("Adding cloud storage reader")
        cloud_storage_read_settings = node_cloud_storage_reader.cloud_storage_settings

        def _func():
            logger.info("Starting to run the schema callback for cloud storage reader")
            self.flow_logger.info("Starting to run the schema callback for cloud storage reader")
            settings = CloudStorageReadSettingsInternal(
                read_settings=cloud_storage_read_settings,
                connection=get_cloud_connection_settings(
                    connection_name=cloud_storage_read_settings.connection_name,
                    user_id=node_cloud_storage_reader.user_id,
                    auth_mode=cloud_storage_read_settings.auth_mode,
                ),
            )
            fl = FlowDataEngine.from_cloud_storage_obj(settings)
            return fl

        node = self.add_node_step(
            node_id=node_cloud_storage_reader.node_id,
            function=_func,
            cache_results=node_cloud_storage_reader.cache_results,
            setting_input=node_cloud_storage_reader,
            node_type=node_type,
        )
        self.add_node_to_starting_list(node)

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_external_source(self, external_source_input: input_schema.NodeExternalSource):
        """Adds a node for a custom external data source.

        Args:
            external_source_input: The settings for the external source node.
        """

        node_type = "external_source"
        external_source_script = getattr(external_sources.custom_external_sources, external_source_input.identifier)
        source_settings = getattr(
            input_schema, snake_case_to_camel_case(external_source_input.identifier)
        ).model_validate(external_source_input.source_settings)
        if hasattr(external_source_script, "initial_getter"):
            initial_getter = external_source_script.initial_getter(source_settings)
        else:
            initial_getter = None
        data_getter = external_source_script.getter(source_settings)
        external_source = data_source_factory(
            source_type="custom",
            data_getter=data_getter,
            initial_data_getter=initial_getter,
            orientation=external_source_input.source_settings.orientation,
            schema=None,
        )

        def _func():
            logger.info("Calling external source")
            fl = FlowDataEngine.create_from_external_source(external_source=external_source)
            external_source_input.source_settings.fields = [c.get_minimal_field_info() for c in fl.schema]
            return fl

        node = self.get_node(external_source_input.node_id)
        if node:
            node.node_type = node_type
            node.name = node_type
            node.function = _func
            node.setting_input = external_source_input
            node.node_settings.cache_results = external_source_input.cache_results
            self.add_node_to_starting_list(node)

        else:
            node = FlowNode(
                external_source_input.node_id,
                function=_func,
                setting_input=external_source_input,
                name=node_type,
                node_type=node_type,
                parent_uuid=self.uuid,
            )
            self._node_db[external_source_input.node_id] = node
            self.add_node_to_starting_list(node)
            self._node_ids.append(external_source_input.node_id)
        if external_source_input.source_settings.fields and len(external_source_input.source_settings.fields) > 0:
            logger.info("Using provided schema in the node")

            def schema_callback():
                return [
                    FlowfileColumn.from_input(f.name, f.data_type) for f in external_source_input.source_settings.fields
                ]

            node.schema_callback = schema_callback
            node.user_provided_schema_callback = schema_callback
        else:
            logger.warning("Removing schema")
            node._schema_callback = None
        self.add_node_step(
            node_id=external_source_input.node_id,
            function=_func,
            input_columns=[],
            node_type=node_type,
            setting_input=external_source_input,
        )

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_read(self, input_file: input_schema.NodeRead):
        """Adds a node to read data from a local file (e.g., CSV, Parquet, Excel).

        Args:
            input_file: The settings for the read operation.
        """
        if (
            input_file.received_file.file_type in ("xlsx", "excel")
            and input_file.received_file.table_settings.sheet_name == ""
        ):
            sheet_name = fastexcel.read_excel(input_file.received_file.path).sheet_names[0]
            input_file.received_file.table_settings.sheet_name = sheet_name

        received_file = input_file.received_file
        input_file.received_file.set_absolute_filepath()

        def _func():
            input_file.received_file.set_absolute_filepath()
            if self.execution_location == "local":
                input_data = FlowDataEngine.create_from_path(input_file.received_file)
            elif input_file.received_file.file_type == "parquet":
                input_data = FlowDataEngine.create_from_path(input_file.received_file)
            elif (
                input_file.received_file.file_type == "csv"
                and "utf" in input_file.received_file.table_settings.encoding
            ):
                input_data = FlowDataEngine.create_from_path(input_file.received_file)
            else:
                input_data = FlowDataEngine.create_from_path_worker(
                    input_file.received_file, node_id=input_file.node_id, flow_id=self.flow_id
                )
            input_data.name = input_file.received_file.name
            return input_data

        node = self.get_node(input_file.node_id)
        schema_callback = None
        if node:
            start_hash = node.hash
            node.node_type = "read"
            node.name = "read"
            node.function = _func
            node.setting_input = input_file
            self.add_node_to_starting_list(node)

            if start_hash != node.hash:
                logger.info("Hash changed, updating schema")
                if len(received_file.fields) > 0:

                    def schema_callback():
                        return [FlowfileColumn.from_input(f.name, f.data_type) for f in received_file.fields]

                elif input_file.received_file.file_type in ("csv", "json", "parquet"):

                    def schema_callback():
                        input_data = FlowDataEngine.create_from_path(input_file.received_file)
                        return input_data.schema

                elif input_file.received_file.file_type in ("xlsx", "excel"):
                    schema_callback = get_xlsx_schema_callback(
                        engine="openpyxl",
                        file_path=received_file.file_path,
                        sheet_name=received_file.table_settings.sheet_name,
                        start_row=received_file.table_settings.start_row,
                        end_row=received_file.table_settings.end_row,
                        start_column=received_file.table_settings.start_column,
                        end_column=received_file.table_settings.end_column,
                        has_headers=received_file.table_settings.has_headers,
                    )
                else:
                    schema_callback = None
        else:
            node = FlowNode(
                input_file.node_id,
                function=_func,
                setting_input=input_file,
                name="read",
                node_type="read",
                parent_uuid=self.uuid,
            )
            self._node_db[input_file.node_id] = node
            self.add_node_to_starting_list(node)
            self._node_ids.append(input_file.node_id)

        if schema_callback is not None:
            node.schema_callback = schema_callback
            node.user_provided_schema_callback = schema_callback
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_datasource(self, input_file: input_schema.NodeDatasource | input_schema.NodeManualInput) -> "FlowGraph":
        """Adds a data source node to the graph.

        This method serves as a factory for creating starting nodes, handling both
        file-based sources and direct manual data entry.

        Args:
            input_file: The configuration object for the data source.

        Returns:
            The `FlowGraph` instance for method chaining.
        """
        if isinstance(input_file, input_schema.NodeManualInput):
            input_data = FlowDataEngine(input_file.raw_data_format)
            ref = "manual_input"
        else:
            input_data = FlowDataEngine(path_ref=input_file.file_ref)
            ref = "datasource"
        node = self.get_node(input_file.node_id)
        if node:
            node.node_type = ref
            node.name = ref
            node.function = input_data
            node.setting_input = input_file
            self.add_node_to_starting_list(node)

        else:
            input_data.collect()
            node = FlowNode(
                input_file.node_id,
                function=input_data,
                setting_input=input_file,
                name=ref,
                node_type=ref,
                parent_uuid=self.uuid,
            )
            self._node_db[input_file.node_id] = node
            self.add_node_to_starting_list(node)
            self._node_ids.append(input_file.node_id)
        return self

    def add_manual_input(self, input_file: input_schema.NodeManualInput):
        """Adds a node for manual data entry.

        This is a convenience alias for `add_datasource`.

        Args:
            input_file: The settings and data for the manual input node.
        """
        self.add_datasource(input_file)

    @property
    def nodes(self) -> list[FlowNode]:
        """Gets a list of all FlowNode objects in the graph."""

        return list(self._node_db.values())

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

    def trigger_fetch_node(self, node_id: int) -> RunInformation | None:
        """Executes a specific node in the graph by its ID."""
        if self.flow_settings.is_running:
            raise Exception("Flow is already running")
        flow_node = self.get_node(node_id)
        self.flow_settings.is_running = True
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
        try:
            self.latest_run_info.node_step_result.append(node_result)
            flow_node.execute_node(
                run_location=self.flow_settings.execution_location,
                performance_mode=False,
                node_logger=node_logger,
                optimize_for_downstream=False,
                reset_cache=True,
            )
            node_result.error = str(flow_node.results.errors)
            if self.flow_settings.is_canceled:
                node_result.success = None
                node_result.success = None
                node_result.is_running = False
            node_result.success = flow_node.results.errors is None
            node_result.end_timestamp = time()
            node_result.run_time_ms = int((node_result.end_timestamp - node_result.start_timestamp) * 1000)
            node_result.is_running = False
            self.latest_run_info.nodes_completed += 1
            self.latest_run_info.end_time = datetime.datetime.now()
            self.flow_settings.is_running = False
            return self.get_run_info()
        except Exception as e:
            node_result.error = "Node did not run"
            node_result.success = False
            node_result.end_timestamp = time()
            node_result.run_time_ms = int((node_result.end_timestamp - node_result.start_timestamp) * 1000)
            node_result.is_running = False
            node_logger.error(f"Error in node {flow_node.node_id}: {e}")
        finally:
            self.flow_settings.is_running = False

    # Artifact helpers

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
        params: dict[str, str] | None = None,
    ) -> tuple[NodeResult, FlowNode]:
        """Executes a single node, records its result, and returns both.

        Thread-safe: uses run_info_lock when mutating shared run information.

        Args:
            node: The node to execute.
            performance_mode: Whether to run in performance mode.
            run_info_lock: Lock protecting shared RunInformation state.
            params: Optional parameter dict for ${name} substitution in node settings.

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

        # Temporarily substitute parameters into node settings (in-place so closures see the values)
        restorations = []
        # Save the node's hash before substitution. executor.execute() calls node.reset()
        # while setting_input is mutated, which recomputes _hash from the resolved path.
        # After restore_parameters the path returns to the original ${...} form but _hash
        # still holds the resolved-path hash → needs_reset() returns True on the next
        # setting_input write → clears example_data_generator / has_completed_last_run.
        # Restoring _hash after restore_parameters keeps the hash consistent with the
        # restored setting_input and prevents that spurious reset.
        saved_hash = node._hash
        if params:
            try:
                restorations = apply_parameters_in_place(node.setting_input, params)
            except ValueError as e:
                node_result.error = str(e)
                node_result.success = False
                node_result.end_timestamp = time()
                node_result.run_time_ms = 0
                node_result.is_running = False
                node_logger.error(f"Parameter resolution failed for node {node.node_id}: {e}")
                return node_result, node

        logger.info(f"Starting to run: node {node.node_id}, start time: {node_result.start_timestamp}")
        try:
            node.execute_node(
                run_location=self.flow_settings.execution_location,
                performance_mode=performance_mode,
                node_logger=node_logger,
            )
        finally:
            # Restore original ${...} refs so the saved flow is unchanged
            if restorations:
                restore_parameters(restorations)
            # Restore the hash to match the restored setting_input so that
            # subsequent get_node_data / setting_input writes don't trigger
            # a spurious reset (and lose example_data_generator / has_completed_last_run).
            node._hash = saved_hash
        try:
            node_result.error = str(node.results.errors)
            if self.flow_settings.is_canceled:
                node_result.success = None
                node_result.is_running = False
                return node_result, node
            node_result.success = node.results.errors is None
            node_result.end_timestamp = time()
            node_result.run_time_ms = int((node_result.end_timestamp - node_result.start_timestamp) * 1000)
            node_result.is_running = False
        except Exception as e:
            node_result.error = "Node did not run"
            node_result.success = False
            node_result.end_timestamp = time()
            node_result.run_time_ms = int((node_result.end_timestamp - node_result.start_timestamp) * 1000)
            node_result.is_running = False
            node_logger.error(f"Error in node {node.node_id}: {e}")

        node_logger.info(f"Completed node with success: {node_result.success}")
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
                    manager = get_kernel_manager()
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
        params: dict[str, str],
        skip_node_ids: set[str | int],
    ) -> set[str | int]:
        """Execute all stages in the plan, running independent nodes in parallel.

        Iterates through stages sequentially. Within each stage, independent
        nodes are executed in parallel (or sequentially if parallelism is
        disabled). Failed nodes cause their dependents to be skipped.

        Returns:
            Set of node IDs that failed during execution.
        """
        run_info_lock = threading.Lock()
        failed_node_ids: set[str | int] = set()

        for stage in execution_plan.stages:
            if self.flow_settings.is_canceled:
                self.flow_logger.info("Flow canceled")
                break

            nodes_to_run = [n for n in stage.nodes if n.node_id not in skip_node_ids]

            for skipped in stage.nodes:
                if skipped.node_id in skip_node_ids:
                    node_logger = self.flow_logger.get_node_logger(skipped.node_id)
                    node_logger.info(f"Skipping node {skipped.node_id}")

            if not nodes_to_run:
                continue

            is_local = self.flow_settings.execution_location == "local"
            max_workers = 1 if is_local else self.flow_settings.max_parallel_workers
            if len(nodes_to_run) == 1 or max_workers == 1:
                stage_results = [
                    self._execute_single_node(node, performance_mode, run_info_lock, params or None)
                    for node in nodes_to_run
                ]
            else:
                stage_results: list[tuple[NodeResult, FlowNode]] = []
                workers = min(max_workers, len(nodes_to_run))
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    futures = {
                        executor.submit(
                            self._execute_single_node, node, performance_mode, run_info_lock, params or None
                        ): node
                        for node in nodes_to_run
                    }
                    for future in as_completed(futures):
                        stage_results.append(future.result())

            for node_result, node in stage_results:
                if not node_result.success:
                    failed_node_ids.add(node.node_id)
                    skip_node_ids.add(node.node_id)
                    for dep in node.get_all_dependent_nodes():
                        skip_node_ids.add(dep.node_id)

        return failed_node_ids

    def _run_post_execution_callbacks(
        self,
        failed_node_ids: set[str | int],
        skip_node_ids: set[str | int],
    ) -> None:
        """Invoke _on_flow_complete callbacks registered by source nodes.

        Each callback receives ``success=True`` when the node and all its
        downstream dependents completed without failure or skip.
        Used e.g. by Kafka sources to commit offsets only on full success.

        Note: the caller must guard against cancellation — this method is
        only invoked when ``is_canceled`` is False.
        """
        incomplete_node_ids = failed_node_ids | skip_node_ids

        for n in self.nodes:
            callback = n._on_flow_complete
            if callback is None:
                continue
            downstream_incomplete = n.node_id in incomplete_node_ids or any(
                dep.node_id in incomplete_node_ids for dep in n.get_all_dependent_nodes()
            )
            success = not downstream_incomplete
            try:
                callback(success)
            except Exception as e:
                self.flow_logger.error(f"Post-execution callback failed for node {n.node_id}: {e}")
            n._on_flow_complete = None

    def run_graph(self) -> RunInformation | None:
        """Executes the entire data flow graph from start to finish.

        Independent nodes within the same execution stage are run in parallel
        using threads. Stages are processed sequentially so that all dependencies
        are satisfied before a stage begins.

        Returns:
            A RunInformation object summarizing the execution results.

        Raises:
            Exception: If the flow is already running.
        """
        if self.flow_settings.is_running:
            raise Exception("Flow is already running")
        try:
            self.flow_settings.is_running = True
            self.flow_settings.is_canceled = False
            self.flow_logger.clear_log_file()
            self.flow_logger.info("Starting to run flowfile flow...")

            execution_plan = compute_execution_plan(
                nodes=self.nodes, flow_starts=self._flow_starts + self.get_implicit_starter_nodes()
            )

            plan_skip_ids: set[str | int] = {n.node_id for n in execution_plan.skip_nodes}
            self._prepare_rerun_artifacts(plan_skip_ids)

            self.latest_run_info = self.create_initial_run_information(execution_plan.node_count, "full_run")
            skip_node_message(self.flow_logger, execution_plan.skip_nodes)
            execution_order_message(self.flow_logger, execution_plan.stages)

            performance_mode = self.flow_settings.execution_mode == "Performance"
            params: dict[str, str] = {p.name: p.default_value for p in self.flow_settings.parameters}

            failed_node_ids = self._execute_stages(execution_plan, performance_mode, params, plan_skip_ids)
            if not self.flow_settings.is_canceled:
                self._run_post_execution_callbacks(failed_node_ids, plan_skip_ids)

            self.latest_run_info.end_time = datetime.datetime.now()
            self.flow_logger.info("Flow completed!")
            self.end_datetime = datetime.datetime.now()
            self.flow_settings.is_running = False
            if self.flow_settings.is_canceled:
                self.flow_logger.info("Flow canceled")
            return self.get_run_info()
        except Exception as e:
            raise e
        finally:
            self.flow_settings.is_running = False

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

    @property
    def node_connections(self) -> list[tuple[int, int]]:
        """Computes and returns a list of all connections in the graph.

        Returns:
            A list of tuples, where each tuple is a (source_id, target_id) pair.
        """
        connections = set()
        for node in self.nodes:
            outgoing_connections = [(node.node_id, ltn.node_id) for ltn in node.leads_to_nodes]
            incoming_connections = [(don.node_id, node.node_id) for don in node.all_inputs]
            node_connections = [
                c for c in outgoing_connections + incoming_connections if (c[0] is not None and c[1] is not None)
            ]
            for node_connection in node_connections:
                if node_connection not in connections:
                    connections.add(node_connection)
        return list(connections)

    def get_node_data(self, node_id: int, include_example: bool = True) -> NodeData:
        """Retrieves all data needed to render a node in the UI.

        Args:
            node_id: The ID of the node.
            include_example: Whether to include data samples in the result.

        Returns:
            A NodeData object, or None if the node is not found.
        """
        node = self._node_db[node_id]
        return node.get_node_data(flow_id=self.flow_id, include_example=include_example)

    def get_flowfile_data(self) -> schemas.FlowfileData:
        start_node_ids = {v.node_id for v in self._flow_starts}

        nodes = []
        for node in self.nodes:
            node_info = node.get_node_information()
            flowfile_node = schemas.FlowfileNode(
                id=node_info.id,
                type=node_info.type,
                is_start_node=node.node_id in start_node_ids,
                description=node_info.description,
                node_reference=node_info.node_reference,
                x_position=int(node_info.x_position),
                y_position=int(node_info.y_position),
                group_id=node_info.group_id,
                left_input_id=node_info.left_input_id,
                right_input_id=node_info.right_input_id,
                input_ids=node_info.input_ids,
                outputs=node_info.outputs,
                output_handles=node_info.output_handles,
                setting_input=node_info.setting_input,
            )
            nodes.append(flowfile_node)

        settings = schemas.FlowfileSettings(
            description=self.flow_settings.description,
            execution_mode=self.flow_settings.execution_mode,
            execution_location=self.flow_settings.execution_location,
            auto_save=self.flow_settings.auto_save,
            show_detailed_progress=self.flow_settings.show_detailed_progress,
            max_parallel_workers=self.flow_settings.max_parallel_workers,
            source_registration_id=self.flow_settings.source_registration_id,
            parameters=self.flow_settings.parameters,
        )
        # Persist only groups that still have members (prune orphans).
        groups = [
            schemas.FlowfileGroup(**self._groups[group_id].model_dump())
            for group_id in self._groups
            if self._member_node_ids(group_id) or self._child_group_ids(group_id)
        ]
        return schemas.FlowfileData(
            flowfile_version=__version__,
            flowfile_id=self.flow_id,
            flowfile_name=self.__name__,
            flowfile_settings=settings,
            nodes=nodes,
            groups=groups,
        )

    def get_node_storage(self) -> schemas.FlowInformation:
        """Serializes the entire graph's state into a storable format.

        Returns:
            A FlowInformation object representing the complete graph.
        """
        node_information = {
            node.node_id: node.get_node_information() for node in self.nodes if node.is_setup and node.is_correct
        }

        return schemas.FlowInformation(
            flow_id=self.flow_id,
            flow_name=self.__name__,
            flow_settings=self.flow_settings,
            data=node_information,
            node_starts=[v.node_id for v in self._flow_starts],
            node_connections=self.node_connections,
        )

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

    def _handle_flow_renaming(self, new_name: str, new_path: Path):
        """
        Handle the rename of a flow when it is being saved.
        """
        if (
            self.flow_settings
            and self.flow_settings.path
            and Path(self.flow_settings.path).absolute() != new_path.absolute()
        ):
            self.__name__ = new_name
            self.flow_settings.save_location = str(new_path.absolute())
            self.flow_settings.name = new_name
        if self.flow_settings and not self.flow_settings.save_location:
            self.flow_settings.save_location = str(new_path.absolute())
            self.__name__ = new_name
            self.flow_settings.name = new_name

    def save_flow(self, flow_path: str):
        """Saves the current state of the flow graph to a file.

        Supports multiple formats based on file extension:
        - .yaml / .yml: New YAML format
        - .json: JSON format

        Args:
            flow_path: The path where the flow file will be saved.
        """
        logger.info("Saving flow to %s", flow_path)
        path = Path(flow_path)
        os.makedirs(path.parent, exist_ok=True)
        suffix = path.suffix.lower()
        new_flow_name = path.name.replace(suffix, "")
        self._handle_flow_renaming(new_flow_name, path)
        self.flow_settings.modified_on = datetime.datetime.now().timestamp()
        try:
            if suffix == ".flowfile":
                raise DeprecationWarning(
                    "The .flowfile format is deprecated. Please use .yaml or .json formats.\n\n"
                    "Or stay on.1 if you still need .flowfile support.\n\n"
                )
            elif suffix in (".yaml", ".yml"):
                flowfile_data = self.get_flowfile_data()
                data = flowfile_data.model_dump(mode="json")
                with open(flow_path, "w", encoding="utf-8") as f:
                    yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
            elif suffix == ".json":
                flowfile_data = self.get_flowfile_data()
                data = flowfile_data.model_dump(mode="json")
                with open(flow_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)

            else:
                flowfile_data = self.get_flowfile_data()
                logger.warning(f"Unknown file extension {suffix}. Defaulting to YAML format.")
                data = flowfile_data.model_dump(mode="json")
                with open(flow_path, "w", encoding="utf-8") as f:
                    yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

        except Exception as e:
            logger.error(f"Error saving flow: {e}")
            raise

        self.flow_settings.path = flow_path
        self._sync_catalog_read_links()
        # Record the current state as the clean baseline for dirty tracking
        self.mark_as_saved()

    def _sync_catalog_read_links(self):
        """Record which catalog tables this flow reads from.

        Scans all nodes for catalog_reader types and upserts read links
        in the catalog database.  Runs at save time so that
        source_registration_id is guaranteed to be set.
        """
        registration_id = self._flow_settings.source_registration_id
        logger.debug("Found registration_id %s", registration_id)
        if not registration_id:
            return

        table_ids = []
        for node in self.nodes:
            if node.node_type != "catalog_reader":
                continue
            setting = node.setting_input
            table_id = getattr(setting, "catalog_table_id", None)
            if table_id:
                table_ids.append(table_id)

        if not table_ids:
            return

        try:
            with get_db_context() as db:
                repo = SQLAlchemyCatalogRepository(db)
                for table_id in table_ids:
                    repo.upsert_read_link(table_id, registration_id)
        except Exception:
            logger.warning(
                "Failed to record catalog read links for tables %s",
                table_ids,
                exc_info=True,
            )

    def get_frontend_data(self) -> dict:
        """Formats the graph structure into a JSON-like dictionary for a specific legacy frontend.

        This method transforms the graph's state into a format compatible with the
        Drawflow.js library.

        Returns:
            A dictionary representing the graph in Drawflow format.
        """
        result = {"Home": {"data": {}}}
        flow_info: schemas.FlowInformation = self.get_node_storage()

        for node_id, node_info in flow_info.data.items():
            if node_info.is_setup:
                try:
                    pos_x = node_info.data.pos_x
                    pos_y = node_info.data.pos_y
                    result["Home"]["data"][str(node_id)] = {
                        "id": node_info.id,
                        "name": node_info.type,
                        "data": {},
                        "class": node_info.type,
                        "html": node_info.type,
                        "typenode": "vue",
                        "inputs": {},
                        "outputs": {},
                        "pos_x": pos_x,
                        "pos_y": pos_y,
                    }
                except Exception as e:
                    logger.error(e)
            if node_info.outputs:
                outputs = {o: 0 for o in node_info.outputs}
                for o in node_info.outputs:
                    outputs[o] += 1
                connections = []
                for output_node_id, _n_connections in outputs.items():
                    leading_to_node = self.get_node(output_node_id)
                    input_types = leading_to_node.get_input_type(node_info.id)
                    for input_type in input_types:
                        if input_type == "main":
                            input_frontend_id = "input_1"
                        elif input_type == "right":
                            input_frontend_id = "input_2"
                        elif input_type == "left":
                            input_frontend_id = "input_3"
                        else:
                            input_frontend_id = "input_1"
                        connection = {"node": str(output_node_id), "input": input_frontend_id}
                        connections.append(connection)

                result["Home"]["data"][str(node_id)]["outputs"]["output_1"] = {"connections": connections}
            else:
                result["Home"]["data"][str(node_id)]["outputs"] = {"output_1": {"connections": []}}

            if (
                node_info.left_input_id is not None
                or node_info.right_input_id is not None
                or node_info.input_ids is not None
            ):
                main_inputs = node_info.main_input_ids
                result["Home"]["data"][str(node_id)]["inputs"]["input_1"] = {
                    "connections": [{"node": str(main_node_id), "input": "output_1"} for main_node_id in main_inputs]
                }
                if node_info.right_input_id is not None:
                    result["Home"]["data"][str(node_id)]["inputs"]["input_2"] = {
                        "connections": [{"node": str(node_info.right_input_id), "input": "output_1"}]
                    }
                if node_info.left_input_id is not None:
                    result["Home"]["data"][str(node_id)]["inputs"]["input_3"] = {
                        "connections": [{"node": str(node_info.left_input_id), "input": "output_1"}]
                    }
        return result

    def get_vue_flow_input(self) -> schemas.VueFlowInput:
        """Formats the graph's nodes and edges into a schema suitable for the VueFlow frontend.

        Returns:
            A VueFlowInput object.
        """
        edges: list[schemas.NodeEdge] = []
        nodes: list[schemas.NodeInput] = []
        for node in self.nodes:
            nodes.append(node.get_node_input())
            edges.extend(node.get_edge_input())
        groups = [
            schemas.FlowfileGroup(**self._groups[group_id].model_dump())
            for group_id in self._groups
            if self._member_node_ids(group_id) or self._child_group_ids(group_id)
        ]
        return schemas.VueFlowInput(node_edges=edges, node_inputs=nodes, groups=groups)

    def reset(self):
        """Forces a deep reset on all nodes in the graph."""

        for node in self.nodes:
            node.reset(True)

    def copy_node(
        self, new_node_settings: input_schema.NodePromise, existing_setting_input: Any, node_type: str
    ) -> None:
        """Creates a copy of an existing node.

        Args:
            new_node_settings: The promise containing new settings (like ID and position).
            existing_setting_input: The settings object from the node being copied.
            node_type: The type of the node being copied.
        """
        self.add_node_promise(new_node_settings)

        if isinstance(existing_setting_input, input_schema.NodePromise):
            return

        combined_settings = combine_existing_settings_and_new_settings(existing_setting_input, new_node_settings)
        getattr(self, f"add_{node_type}")(combined_settings)

    def generate_code(self):
        """Generates code for the flow graph.
        This method exports the flow graph to a Polars-compatible format.
        """
        from flowfile_core.flowfile.code_generator.code_generator import export_flow_to_polars

        print(export_flow_to_polars(self))


def combine_existing_settings_and_new_settings(setting_input: Any, new_settings: input_schema.NodePromise) -> Any:
    """Merges settings from an existing object with new settings from a NodePromise.

    Typically used when copying a node to apply a new ID and position.

    Args:
        setting_input: The original settings object.
        new_settings: The NodePromise with new positional and ID data.

    Returns:
        A new settings object with the merged properties.
    """
    copied_setting_input = deepcopy(setting_input)

    fields_to_update = ("node_id", "pos_x", "pos_y", "description", "flow_id")

    for field in fields_to_update:
        if hasattr(new_settings, field) and getattr(new_settings, field) is not None:
            setattr(copied_setting_input, field, getattr(new_settings, field))

    # Reset node_reference to None when copying (so it defaults to df_{node_id})
    if hasattr(copied_setting_input, "node_reference"):
        copied_setting_input.node_reference = None

    return copied_setting_input


def _would_create_cycle(from_node: "FlowNode", to_node: "FlowNode") -> bool:
    """True if connecting from_node -> to_node would introduce a cycle.

    A cycle exists if from_node is already reachable downstream of to_node via
    existing `leads_to_nodes` edges, or if the caller is trying to create a
    self-loop.
    """
    if from_node.node_id == to_node.node_id:
        return True
    visited: set = {to_node.node_id}
    stack = [to_node]
    while stack:
        current = stack.pop()
        for downstream in current.leads_to_nodes:
            if downstream.node_id == from_node.node_id:
                return True
            if downstream.node_id not in visited:
                visited.add(downstream.node_id)
                stack.append(downstream)
    return False


def add_connection(flow: FlowGraph, node_connection: input_schema.NodeConnection) -> None:
    """Adds a connection between two nodes in the flow graph.

    Args:
        flow: The FlowGraph instance to modify.
        node_connection: An object defining the source and target of the connection.
    """
    logger.info("adding a connection")
    from_node = flow.get_node(node_connection.output_connection.node_id)
    to_node = flow.get_node(node_connection.input_connection.node_id)
    logger.info(f"from_node={from_node}, to_node={to_node}")
    if not (from_node and to_node):
        missing = [
            str(nc.node_id)
            for nc, n in (
                (node_connection.output_connection, from_node),
                (node_connection.input_connection, to_node),
            )
            if not n
        ]
        raise HTTPException(404, f"Node(s) not found: {', '.join(missing)}")
    if _would_create_cycle(from_node, to_node):
        raise HTTPException(
            422,
            f"Connecting node {from_node.node_id} -> {to_node.node_id} would create a cycle",
        )
    to_node.add_node_connection(
        from_node,
        node_connection.input_connection.get_node_input_connection_type(),
        output_handle=node_connection.output_connection.connection_class,
    )


def delete_connection(graph, node_connection: input_schema.NodeConnection):
    """Deletes a connection between two nodes in the flow graph.

    Args:
        graph: The FlowGraph instance to modify.
        node_connection: An object defining the connection to be removed.
    """
    from_node = graph.get_node(node_connection.output_connection.node_id)
    to_node = graph.get_node(node_connection.input_connection.node_id)
    # Without these guards a stale delete (e.g. after the target node was
    # already removed) surfaces as an AttributeError → 500, which also drops
    # CORS headers and shows up as a CORS error in the browser.
    if from_node is None or to_node is None:
        raise HTTPException(422, "Connection does not exist on the input node")
    connection_valid = to_node.node_inputs.validate_if_input_connection_exists(
        node_input_id=from_node.node_id,
        connection_name=node_connection.input_connection.get_node_input_connection_type(),
    )
    if not connection_valid:
        raise HTTPException(422, "Connection does not exist on the input node")
    from_node.delete_lead_to_node(node_connection.input_connection.node_id)
    to_node.delete_input_node(
        node_connection.output_connection.node_id,
        connection_type=node_connection.input_connection.connection_class,
    )
