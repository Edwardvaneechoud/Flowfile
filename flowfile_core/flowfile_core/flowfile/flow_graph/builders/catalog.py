"""Catalog reader and writer nodes."""

import json

import polars as pl

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.delta_utils import (
    is_delta_table,
    is_legacy_parquet,
)
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.storage_backend import CatalogStorageTarget, _is_cloud_uri, resolve_for_table
from flowfile_core.configs.settings import FLOWFILE_COMPUTE_PROTOCOL
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.catalog_cdc import (
    make_cdc_commit_callback,
    read_cursor,
    resolve_change_window,
    resolve_consumer_key,
)
from flowfile_core.flowfile.execution.context import current_context
from flowfile_core.flowfile.execution.protocol import ComputeBackend
from flowfile_core.flowfile.flow_data_engine.bounded import _deferred
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import (
    FlowDataEngine,
)
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import cast_str_to_polars_type
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
from flowfile_core.hub_client import tables as hub_tables
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.history_schema import (
    HistoryActionType,
)
from shared.delta_utils import (
    get_delta_head_version,
    scan_delta_changes,
)

PICK_HUB_TABLE = "No table on the hub is picked for this reader; pick one."


def unnamed_hub_table(settings: input_schema.NodeCatalogReader) -> str | None:
    """Why a reader on the hub cannot be read: it names no table (``PICK_HUB_TABLE``), or only a bare name, which a
    flow saved by an older Flowfile carries when its hub ids could not be converted at open; ``None`` when it names
    its table by its qualified name."""
    if settings.catalog_full_table_name:
        return None
    if settings.catalog_table_name:
        return (
            f"The table {settings.catalog_table_name} on the hub is named by an id from an older Flowfile; open this "
            "flow while connected to the hub to convert it, or pick the table again."
        )
    return PICK_HUB_TABLE


def _filtered_frame(lf: pl.LazyFrame, row_filter: pl.Expr | None, backend: ComputeBackend | None) -> FlowDataEngine:
    """``lf`` under ``row_filter``; with a ``backend``, its schema read there with the plan held deferred."""
    plan = lf if row_filter is None else lf.filter(row_filter)
    if backend is None:
        return FlowDataEngine(plan)
    with _deferred(backend, plan) as ref:
        return FlowDataEngine(plan, schema=ref.schema)


class CatalogBuildersMixin(GraphMixinBase):
    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_catalog_reader(self, node_catalog_reader: input_schema.NodeCatalogReader):
        """Adds a node that reads a table from the catalog.

        Resolves the catalog table by ID (or name + namespace) and reads
        the materialized Parquet file.  When ``sql_query`` is set, executes
        the SQL against all catalog Delta tables instead.
        """

        if node_catalog_reader.source == "hub":
            is_virtual_optimized = self._add_hub_table_reader(node_catalog_reader)
        elif node_catalog_reader.sql_query:
            is_virtual_optimized = self._add_catalog_sql_reader(node_catalog_reader)
        else:
            is_virtual_optimized = self._add_catalog_table_reader(node_catalog_reader)
        node_catalog_reader.is_virtual_optimized = is_virtual_optimized

    def _add_catalog_sql_reader(self, node_catalog_reader: input_schema.NodeCatalogReader) -> bool | None:
        """Execute a SQL query against all catalog tables (physical + virtual).

        With ``FLOWFILE_COMPUTE_PROTOCOL`` on, a virtual table whose producer flow runs is resolved on the reader's
        backend (placement's, in a run); with it off, on the flow's location.

        Returns:
            Whether all referenced virtual tables are optimized, or None if no virtual tables.
        """

        sql_code = node_catalog_reader.sql_query
        resolved = _resolve_catalog_sql_tables(node_catalog_reader.node_id, node_catalog_reader.user_id)
        table_paths = resolved.table_paths
        virtual_tables = resolved.virtual_tables
        table_namespaces = resolved.table_namespaces

        # Memoized per storage: one SQL query can join tables from catalogs with different storage.
        targets_by_name: dict[str, CatalogStorageTarget] = {}
        _targets: dict[tuple[bool, int | None], CatalogStorageTarget] = {}
        for _name, _path in table_paths.items():
            _ns = table_namespaces.get(_name)
            _key = (_is_cloud_uri(_path), _ns)
            if _key not in _targets:
                _targets[_key] = resolve_for_table(_path, _ns)
            targets_by_name[_name] = _targets[_key]

        def _func() -> FlowDataEngine:
            if not table_paths and not virtual_tables:
                raise ValueError("No catalog tables available to query")
            ctx = pl.SQLContext()
            for name, path in table_paths.items():
                scan_kwargs = targets_by_name[name].scan_kwargs(node_catalog_reader.user_id)
                ctx.register(name, pl.scan_delta(path, **scan_kwargs))
            run_location = current_context(self).backend_id if FLOWFILE_COMPUTE_PROTOCOL else self.execution_location
            for name, (is_opt, ser_lf, tid, stv) in virtual_tables.items():
                ctx.register(
                    name,
                    _resolve_virtual_table(
                        is_opt,
                        ser_lf,
                        tid,
                        node_logger=self.flow_logger.get_node_logger(node_catalog_reader.node_id),
                        run_location=run_location,
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

    def _add_hub_table_reader(self, node_catalog_reader: input_schema.NodeCatalogReader) -> None:
        """Read a table on the hub, named by its qualified name, through the credential the hub vends to the run.

        The table is looked up on the hub by name once here, and its columns are the node's predicted schema; a hub
        that does not answer leaves its message on the node, and the table is looked up again when the run reads it. A
        reader that names no table yet fails with ``unnamed_hub_table`` when it runs. The scan carries the run's
        credential encrypted, or a vend sealed to the servers the run uses (``RunVend.scan_delta``), so outside a run
        it cannot read.
        """
        user_id = node_catalog_reader.user_id
        qualified_name = node_catalog_reader.catalog_full_table_name
        node_logger = self.flow_logger.get_node_logger(node_catalog_reader.node_id)
        designed: dict | None = None
        unread: str | None = unnamed_hub_table(node_catalog_reader)
        if unread is None:
            try:
                designed = hub_tables.readable_named(user_id, qualified_name)
            except LookupError as e:
                unread = str(e)
        row_filter = _scd2_row_filter(None, node_catalog_reader.scd2_view, None, node_logger=node_logger)

        def _func() -> FlowDataEngine:
            unnamed = unnamed_hub_table(node_catalog_reader)
            if unnamed is not None:
                raise ValueError(unnamed)
            if self.hub_vend is None:
                raise ValueError("Run the flow to read a table on the hub")
            table = designed or hub_tables.readable_named(user_id, qualified_name)
            columns = {
                column["name"]: cast_str_to_polars_type(column["dtype"]) for column in table.get("schema_columns") or ()
            }
            lf = self.hub_vend.scan_delta(table["file_path"], user_id, columns)
            backend = current_context(self).backend if FLOWFILE_COMPUTE_PROTOCOL else None
            return _filtered_frame(lf, row_filter, backend)

        def _schema_callback() -> list[FlowfileColumn]:
            return [
                FlowfileColumn.from_input(column_name=column["name"], data_type=column["dtype"])
                for column in designed["schema_columns"]
            ]

        self.add_node_step(
            node_id=node_catalog_reader.node_id,
            function=_func,
            input_columns=[],
            node_type="catalog_reader",
            setting_input=node_catalog_reader,
            schema_callback=_schema_callback if designed is not None else None,
        )
        node = self.get_node(node_catalog_reader.node_id)
        self.add_node_to_starting_list(node)
        if unread is not None:
            node.results.errors = unread
        return None

    def _add_catalog_table_reader(self, node_catalog_reader: input_schema.NodeCatalogReader) -> bool | None:
        """Read a single table from the catalog (physical or virtual).

        With ``FLOWFILE_COMPUTE_PROTOCOL`` on, a table on object storage takes its schema from the reader's backend
        (its plan held there deferred while the schema is read), so that backend resolves the plan's connection.
        Every other read here resolves its schema in core. A virtual table whose producer flow runs is resolved on
        the reader's backend with the flag on, on the flow's location with it off.

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

        # Resolve storage once at wiring time; local tables don't touch the DB.
        _reader_target = resolve_for_table(resolved_path, info.namespace_id or node_catalog_reader.catalog_namespace_id)
        _reader_storage_options = _reader_target.storage_options or None

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

        def _apply_scd2_filter(lf: pl.LazyFrame, backend: ComputeBackend | None = None) -> FlowDataEngine:
            return _filtered_frame(lf, _scd2_filter, backend)

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
                **_reader_target.scan_kwargs(_user_id),
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
                        run_location=(
                            current_context(self).backend_id if FLOWFILE_COMPUTE_PROTOCOL else self.execution_location
                        ),
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
                scan_kwargs.update(_reader_target.scan_kwargs(_user_id))
                backend = current_context(self).backend if FLOWFILE_COMPUTE_PROTOCOL else None
                return _apply_scd2_filter(pl.scan_delta(resolved_path, **scan_kwargs), backend)
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
