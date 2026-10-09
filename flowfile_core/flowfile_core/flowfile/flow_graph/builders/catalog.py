"""Catalog reader and writer nodes."""

import json

import polars as pl

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.delta_utils import (
    is_delta_table,
    is_legacy_parquet,
)
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.storage_backend import _is_cloud_uri, resolve_for_namespace
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.catalog_cdc import (
    make_cdc_commit_callback,
    read_cursor,
    resolve_change_window,
    resolve_consumer_key,
)
from flowfile_core.flowfile.flow_data_engine.cloud_storage_reader import CloudStorageReader
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import (
    FlowDataEngine,
)
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_graph._base import GraphMixinBase
from flowfile_core.flowfile.flow_graph.catalog_resolution import (
    _CDF_COLUMN_DTYPES,
    _resolve_catalog_sql_tables,
    _resolve_catalog_table_info,
    _resolve_virtual_table,
)
from flowfile_core.flowfile.flow_graph.catalog_write import (
    _handle_physical_table_write,
    _handle_virtual_table_write,
    _scd2_row_filter,
    _scd2_system_column_dtypes,
)
from flowfile_core.flowfile.flow_graph.history import with_history_capture
from flowfile_core.flowfile.flow_node.flow_node import (
    FlowNode,
)
from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import (
    validate_sql_query,
)
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.history_schema import (
    HistoryActionType,
)
from shared.delta_utils import (
    get_delta_head_version,
    scan_delta_changes,
)


class CatalogBuildersMixin(GraphMixinBase):
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
        resolved = _resolve_catalog_sql_tables(node_catalog_reader.node_id, node_catalog_reader.user_id)
        table_paths = resolved.table_paths
        virtual_tables = resolved.virtual_tables
        table_namespaces = resolved.table_namespaces

        # Memoized per namespace: one SQL query can join tables from catalogs with different storage.
        storage_options_by_name: dict[str, dict | None] = {}
        _opts_by_namespace: dict[int | None, dict | None] = {}
        for _name, _path in table_paths.items():
            if not _is_cloud_uri(_path):
                continue
            _ns = table_namespaces.get(_name)
            if _ns not in _opts_by_namespace:
                _opts_by_namespace[_ns] = resolve_for_namespace(_ns).storage_options or None
            storage_options_by_name[_name] = _opts_by_namespace[_ns]

        def _func() -> FlowDataEngine:
            if not table_paths and not virtual_tables:
                raise ValueError("No catalog tables available to query")
            ctx = pl.SQLContext()
            for name, path in table_paths.items():
                if _is_cloud_uri(path):
                    scan_kwargs = CloudStorageReader.get_secure_scan_kwargs(
                        storage_options_by_name.get(name), node_catalog_reader.user_id
                    )
                    ctx.register(name, pl.scan_delta(path, **scan_kwargs))
                else:
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
                        user_id=node_catalog_reader.user_id,
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

        # Back-fill the id from a name-only reference; the form and read lineage key on catalog_table_id.
        if node_catalog_reader.catalog_table_id is None and info.table_id is not None:
            node_catalog_reader.catalog_table_id = info.table_id
            if node_catalog_reader.catalog_namespace_id is None:
                node_catalog_reader.catalog_namespace_id = info.namespace_id
            if node_catalog_reader.catalog_table_name is None:
                node_catalog_reader.catalog_table_name = info.table_name

        is_virtual_optimized: bool | None = info.is_optimized if info.table_type == "virtual" else None

        resolved_path = info.file_path
        delta_version = node_catalog_reader.delta_version
        _table_type = info.table_type
        _serialized_lf = info.serialized_lf
        _is_optimized = info.is_optimized
        _catalog_table_id = node_catalog_reader.catalog_table_id
        _source_table_versions = info.source_table_versions
        _authorized = info.authorized
        _user_id = node_catalog_reader.user_id

        # Resolve cloud storage options once at wiring time; local tables don't touch the DB.
        _reader_storage_options = None
        if resolved_path and _is_cloud_uri(resolved_path):
            _reader_namespace_id = info.namespace_id or node_catalog_reader.catalog_namespace_id
            _reader_storage_options = resolve_for_namespace(_reader_namespace_id).storage_options or None

        _scd2_filter = _scd2_row_filter(
            info.scd2_config,
            node_catalog_reader.scd2_view,
            node_catalog_reader.scd2_as_of,
            node_logger=self.flow_logger.get_node_logger(node_catalog_reader.node_id),
        )

        _cdc_mode = node_catalog_reader.cdc_mode
        _table_schema_json = info.schema_json
        if _cdc_mode != "off":
            if _table_type == "virtual":
                raise ValueError("Change tracking is not available for virtual catalog tables")
            if resolved_path and not _is_cloud_uri(resolved_path) and is_legacy_parquet(resolved_path):
                raise ValueError("Change tracking is not available for legacy parquet catalog tables")

        def _apply_scd2_filter(lf: pl.LazyFrame) -> FlowDataEngine:
            return FlowDataEngine(lf if _scd2_filter is None else lf.filter(_scd2_filter))

        def _resolve_table_row(db):
            repo = SQLAlchemyCatalogRepository(db)
            table = repo.get_table_fresh(_catalog_table_id) if _catalog_table_id else None
            if table is not None:
                return table
            reference = node_catalog_reader.catalog_full_table_name or node_catalog_reader.catalog_table_name
            if not reference:
                return None
            return CatalogService(repo).resolve_table(
                reference, default_namespace_id=node_catalog_reader.catalog_namespace_id
            )

        def _read_changes() -> FlowDataEngine:
            """Read the table's change feed for this run.

            Head and the cursor are resolved here, at execution time: the designer keeps one
            FlowGraph across runs, so a window pinned while wiring would go stale after the first
            one. Strict by design — an untracked table is an actionable error, never a silent
            full read.
            """
            with get_db_context() as db:
                table = _resolve_table_row(db)
                if table is None:
                    raise ValueError("Catalog table could not be resolved — no change feed to read")
                table_id, table_name = table.id, table.name
                table_path, cdc_enabled, floor = table.file_path, table.cdc_enabled, table.cdc_enabled_version
            if not cdc_enabled:
                raise ValueError(
                    f"Change tracking is not enabled on '{table_name}'. Enable change tracking on "
                    f"'{table_name}' first, then re-run."
                )
            if not table_path:
                raise ValueError(f"Catalog table '{table_name}' has no storage to read changes from")

            head = get_delta_head_version(table_path, storage_options=_reader_storage_options)
            cursor_consumer: tuple[str, str | None] | None = None
            last_version: int | None = None
            if _cdc_mode == "since_last_run":
                consumer_key, label = resolve_consumer_key(self, node_catalog_reader)
                with get_db_context() as db:
                    cursor = read_cursor(db, table_id, consumer_key, table_path)
                last_version = cursor.last_version if cursor is not None else None
                cursor_consumer = (consumer_key, label)
            starting_version = resolve_change_window(
                _cdc_mode,
                node_catalog_reader.cdc_from_version,
                node_catalog_reader.cdc_from_timestamp,
                head=head,
                floor=floor,
                path=table_path,
                storage_options=_reader_storage_options,
                last_version=last_version,
                cdc_start=node_catalog_reader.cdc_start,
            )

            lf = scan_delta_changes(
                table_path,
                starting_version,
                head,
                **CloudStorageReader.get_secure_scan_kwargs(_reader_storage_options, _user_id),
                include_preimage=node_catalog_reader.cdc_include_preimage,
            )
            if cursor_consumer is not None:
                consumer_key, label = cursor_consumer
                self.get_node(node_catalog_reader.node_id)._on_flow_complete = make_cdc_commit_callback(
                    table_id,
                    consumer_key,
                    head,
                    node_catalog_reader.node_id,
                    self.flow_logger,
                    owner_id=node_catalog_reader.user_id,
                    label=label,
                    table_path=table_path,
                )
            self.flow_logger.get_node_logger(node_catalog_reader.node_id).info(
                f"Reading changes from '{table_name}' (v{starting_version}..v{head})"
            )
            return FlowDataEngine(lf)

        def _cdc_schema_callback() -> list[FlowfileColumn]:
            """Predicted schema of a change read: the table's own columns plus the three feed columns."""
            columns = [
                FlowfileColumn.from_input(column_name=entry["name"], data_type=entry["dtype"])
                for entry in json.loads(_table_schema_json or "[]")
            ]
            columns.extend(
                FlowfileColumn.from_input(column_name=name, data_type=dtype) for name, dtype in _CDF_COLUMN_DTYPES
            )
            return columns

        def _func() -> FlowDataEngine:
            if not _authorized:
                raise PermissionError(
                    f"Not authorized to read the catalog table for node {node_catalog_reader.node_id}"
                )
            if _cdc_mode != "off":
                return _read_changes()
            if _table_type == "virtual":
                return FlowDataEngine(
                    _resolve_virtual_table(
                        _is_optimized,
                        _serialized_lf,
                        _catalog_table_id,
                        node_logger=self.flow_logger.get_node_logger(node_catalog_reader.node_id),
                        run_location=self.execution_location,
                        source_table_versions=_source_table_versions,
                        user_id=_user_id,
                    )
                )

            if not resolved_path:
                raise ValueError("Catalog table could not be resolved — no file path found")
            scan_kwargs = {}
            if delta_version is not None:
                scan_kwargs["version"] = delta_version
            if _is_cloud_uri(resolved_path):
                # Cloud catalog table: scan directly (stays lazy ⇒ no collect in core).
                scan_kwargs.update(CloudStorageReader.get_secure_scan_kwargs(_reader_storage_options, _user_id))
                return _apply_scd2_filter(pl.scan_delta(resolved_path, **scan_kwargs))
            if is_delta_table(resolved_path):
                return _apply_scd2_filter(pl.scan_delta(resolved_path, **scan_kwargs))
            return _apply_scd2_filter(pl.scan_parquet(resolved_path))

        self.add_node_step(
            node_id=node_catalog_reader.node_id,
            function=_func,
            input_columns=[],
            node_type="catalog_reader",
            setting_input=node_catalog_reader,
            schema_callback=_cdc_schema_callback if _cdc_mode != "off" else None,
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
            schema = input_node.schema
            settings = node_catalog_writer.catalog_write_settings
            if settings.write_mode != "scd2":
                return schema
            # SCD2 adds four generated columns; a name already upstream is left out (the write rejects it anyway).
            upstream = {c.column_name for c in schema}
            generated = _scd2_system_column_dtypes(settings.scd2 or input_schema.Scd2Settings())
            return [
                *schema,
                *(
                    FlowfileColumn.create_from_polars_dtype(name, dtype)
                    for name, dtype in generated.items()
                    if name not in upstream
                ),
            ]

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
