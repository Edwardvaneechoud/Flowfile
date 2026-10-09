"""Local file sources and sinks, the generic external source, and their schema callbacks."""

from collections.abc import Callable
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
from fastapi.exceptions import HTTPException

from flowfile_core.configs import logger
from flowfile_core.configs.settings import is_electron_mode
from flowfile_core.fileExplorer.funcs import SecureFileExplorer
from flowfile_core.flowfile.flow_data_engine.create import funcs as create_funcs
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import (
    FlowDataEngine,
)
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_data_engine.read_excel_tables import (
    get_calamine_xlsx_data_types,
    get_open_xlsx_datatypes,
)
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
    ExternalOutputWriter,
)
from flowfile_core.flowfile.flow_graph._base import GraphMixinBase
from flowfile_core.flowfile.flow_graph.history import with_history_capture
from flowfile_core.flowfile.flow_node.flow_node import (
    FlowNode,
)
from flowfile_core.flowfile.schema_callbacks import (
    pl_schema_to_flowfile_columns,
)
from flowfile_core.flowfile.sources import external_sources
from flowfile_core.flowfile.sources.external_sources.factory import data_source_factory
from flowfile_core.flowfile.utils import snake_case_to_camel_case
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.history_schema import (
    HistoryActionType,
)
from shared.path_utils import assert_directory_scan_supported, expand_glob_pattern, is_utf8_encoding
from shared.storage_config import storage

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph.graph import FlowGraph


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


def get_directory_schema_callback(received_file: input_schema.ReceivedTable):
    """Directory-mode schema callback: probe only the first matched file.

    Under strict-native semantics the run errors on column-set divergence anyway, so the first
    file's schema is right whenever the run can succeed (known gap: csv dtype widening across
    files is not predicted). Zero matches yield an empty schema so settings saves stay tolerant.
    The probe stays in directory mode over that one file: single-file mode would let polars
    re-glob a matched path containing ``[``/``*``/``?`` (a bracketed directory name is enough).
    """

    def schema_callback():
        matches = expand_glob_pattern(received_file.abs_file_path)
        if not matches:
            return []
        probe = received_file.model_copy(deep=True)
        probe.name = None
        probe.path = matches[0]
        probe.abs_file_path = None
        probe.include_file_paths = None  # appended below instead, so the probe cannot emit it twice
        probe.set_absolute_filepath()
        schema = FlowDataEngine.create_from_path(probe).schema
        existing_names = {column.name for column in schema}
        # A collision fails the run with polars' own DuplicateError; never predict a duplicate.
        if received_file.include_file_paths and received_file.include_file_paths not in existing_names:
            schema = [*schema, FlowfileColumn.from_input(received_file.include_file_paths, "String")]
        return schema

    return schema_callback


LIST_FILES_SCHEMA: list[tuple[str, Any]] = [
    ("file_name", pl.String),
    ("file_path", pl.String),
    ("directory", pl.String),
    ("relative_path", pl.String),
    ("file_type", pl.String),
    ("size_bytes", pl.Int64),
    ("last_modified", pl.Datetime("us")),
    ("created_date", pl.Datetime("us")),
    ("is_directory", pl.Boolean),
]
"""Fixed output schema of the ``list_files`` node.

Fixed on purpose: schema prediction must never walk the filesystem, so the columns
cannot depend on the selected directory. ``file_path`` is the column a downstream
Read node consumes; ``relative_path`` keeps partition-style folder structure usable.
"""


def list_files_schema() -> list[FlowfileColumn]:
    """The ``list_files`` output schema as FlowfileColumns."""
    return [FlowfileColumn.create_from_polars_dtype(name, dtype) for name, dtype in LIST_FILES_SCHEMA]


def _list_files_sandbox_root() -> Path | None:
    """Run-time filesystem boundary for the ``list_files`` node.

    Mirrors ``GET /files/directory_contents/`` exactly: the desktop app browses the
    whole machine, every other mode is confined to the user-data directory. Enforcing
    it here too matters because a flow can be run by the scheduler or the API, which
    never pass through the browse route.
    """
    return None if is_electron_mode() else storage.user_data_directory


def scan_directory_to_frame(
    settings: input_schema.NodeListFiles, cancel_check: Callable[[], bool] | None = None
) -> pl.DataFrame:
    """Walk ``settings.path`` and return one row per entry, in ``LIST_FILES_SCHEMA`` shape.

    Raises ``HTTPException`` rather than a bare exception so the settings route reports a
    usable message instead of a generic 419. ``cancel_check`` is polled during the walk so
    a run over a big or slow tree stays cancellable (the walk happens here, in core).
    """
    if not settings.path:
        raise HTTPException(status_code=400, detail="No folder selected")

    sandbox_root = _list_files_sandbox_root()
    try:
        explorer = SecureFileExplorer(settings.path, sandbox_root)
    except PermissionError:
        raise HTTPException(
            status_code=403, detail=f"Access denied: '{settings.path}' is outside the allowed directory"
        ) from None
    except (OSError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Could not open folder '{settings.path}': {e}") from e

    root = explorer.current_path
    if not root.exists() or not root.is_dir():
        raise HTTPException(status_code=400, detail=f"Folder does not exist: {settings.path}")

    entries = explorer.list_contents(
        show_hidden=settings.include_hidden,
        file_types=settings.file_types or None,
        recursive=settings.recursive,
        max_depth=settings.max_depth,
        cancel_check=cancel_check,
    )
    if not settings.include_directories:
        entries = [e for e in entries if not e.is_directory]
    if not settings.include_files:
        entries = [e for e in entries if e.is_directory]
    if settings.max_files is not None:
        entries = entries[: settings.max_files]

    rows = []
    for entry in entries:
        entry_path = Path(entry.path)
        try:
            relative_path = str(entry_path.relative_to(root))
        except ValueError:
            relative_path = entry.name
        rows.append(
            {
                "file_name": entry.name,
                "file_path": str(entry_path),
                "directory": str(entry_path.parent),
                "relative_path": relative_path,
                "file_type": entry.file_type,
                "size_bytes": entry.size,
                "last_modified": entry.last_modified,
                "created_date": entry.created_date,
                "is_directory": entry.is_directory,
            }
        )
    return pl.DataFrame(rows, schema=dict(LIST_FILES_SCHEMA))


class FileIoBuildersMixin(GraphMixinBase):
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
                compression=output_fs.compression,
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

    def add_sql_source(self, external_source_input: input_schema.NodeExternalSource):
        """Adds a node that reads data from a SQL source.

        This is a convenience alias for `add_external_source`.

        Args:
            external_source_input: The settings for the external SQL source node.
        """
        logger.info("Adding sql source")
        self.add_external_source(external_source_input)

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
        received = input_file.received_file
        if received.scan_mode == "directory":
            assert_directory_scan_supported(
                received.file_type, getattr(received.table_settings, "encoding", None), received.path
            )

        received_file = input_file.received_file
        input_file.received_file.set_absolute_filepath()

        def _func():
            input_file.received_file.set_absolute_filepath()
            if self.execution_location == "local":
                input_data = FlowDataEngine.create_from_path(
                    input_file.received_file, node_logger=self.flow_logger.get_node_logger(input_file.node_id)
                )
            elif input_file.received_file.file_type in ("parquet", "ipc", "ndjson"):
                input_data = FlowDataEngine.create_from_path(input_file.received_file)
            elif input_file.received_file.file_type == "csv" and is_utf8_encoding(
                input_file.received_file.table_settings.encoding
            ):
                input_data = FlowDataEngine.create_from_path(input_file.received_file)
            else:
                input_data = FlowDataEngine.create_from_path_worker(
                    input_file.received_file, node_id=input_file.node_id, flow_id=self.flow_id
                )
            input_data.name = input_file.received_file.name
            return input_data

        def schema_from_fields():
            schema = [FlowfileColumn.from_input(f.name, f.data_type) for f in received_file.fields]
            # Saved fields may predate the source-path column; the engine adds it in every scan mode.
            existing_names = {f.name for f in received_file.fields}
            if received_file.include_file_paths and received_file.include_file_paths not in existing_names:
                schema.append(FlowfileColumn.from_input(received_file.include_file_paths, "String"))
            return schema

        directory_schema_callback = None
        if received.scan_mode == "directory":
            if len(received_file.fields) > 0:
                directory_schema_callback = schema_from_fields
            else:
                directory_schema_callback = get_directory_schema_callback(received_file)

        node = self.get_node(input_file.node_id)
        is_new = node is None
        schema_callback = None
        if node:
            start_hash = node.hash
            node.node_type = "read"
            node.name = "read"
            node.function = _func
            if directory_schema_callback is not None:
                # Before setting_input, so reset()'s eager prefetch runs this instead of a throwaway full _func build.
                node.user_provided_schema_callback = directory_schema_callback
            else:
                previous_file = getattr(node.setting_input, "received_file", None)
                if getattr(previous_file, "scan_mode", "single_file") == "directory":
                    # Leaving directory mode: the directory callback must not outlive it.
                    node.user_provided_schema_callback = None
            node.setting_input = input_file
            self.add_node_to_starting_list(node)

            if start_hash != node.hash:
                logger.info("Hash changed, updating schema")
                if directory_schema_callback is not None:
                    pass  # installed above; reinstalling would discard the started prefetch
                elif len(received_file.fields) > 0:
                    schema_callback = schema_from_fields
                elif input_file.received_file.file_type in ("csv", "json", "parquet", "ipc", "ndjson"):

                    def schema_callback():
                        input_data = FlowDataEngine.create_from_path(input_file.received_file)
                        return input_data.schema

                elif input_file.received_file.file_type in ("avro", "ipc_stream"):

                    def schema_callback():
                        return pl_schema_to_flowfile_columns(create_funcs.probe_eager_schema(input_file.received_file))

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
                # update_node installs this before setting_input, so the eager prefetch skips the full _func build.
                schema_callback=directory_schema_callback,
            )
            self._node_db[input_file.node_id] = node
            self.add_node_to_starting_list(node)
            self._node_ids.append(input_file.node_id)

        if schema_callback is not None:
            node.schema_callback = schema_callback
            node.user_provided_schema_callback = schema_callback
        self._notify_node_observers(node, is_new=is_new)
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
        is_new = node is None
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
        self._notify_node_observers(node, is_new=is_new)
        return self

    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_list_files(self, node_list_files: input_schema.NodeListFiles) -> None:
        """Adds a source node that lists a directory's contents as a table.

        The scan runs in core rather than on the worker: it is local filesystem
        metadata (one ``iterdir``/``stat`` pass), not network I/O and not a dataset,
        so there is nothing to offload and a CLI/scheduler run with no worker still
        works. The schema is fixed (``LIST_FILES_SCHEMA``), so ``schema_callback``
        never touches the disk and opening a flow costs no directory walk.
        """
        logger.info("Adding list files")
        node_type = "list_files"

        def schema_callback() -> list[FlowfileColumn]:
            return list_files_schema()

        def _func() -> FlowDataEngine:
            # The walk runs in core, so it polls for cancellation itself (no worker subprocess to kill).
            def is_cancelled() -> bool:
                if node._execution_state.is_canceled:
                    return True
                # The graph flag clears only when the next run starts: honour it mid-run and mirror it onto the node.
                if self.flow_settings.is_running and self.flow_settings.is_canceled:
                    node._execution_state.is_canceled = True
                    return True
                return False

            return FlowDataEngine(
                scan_directory_to_frame(node_list_files, cancel_check=is_cancelled),
                schema=schema_callback(),
                number_of_records=None,
            )

        node = self.get_node(node_list_files.node_id)
        is_new = node is None
        if node:
            node.schema_callback = schema_callback
            node.user_provided_schema_callback = schema_callback
            node.node_type = node_type
            node.name = node_type
            node.function = _func
            node.setting_input = node_list_files
            node.node_settings.cache_results = node_list_files.cache_results
            self.add_node_to_starting_list(node)
        else:
            node = FlowNode(
                node_list_files.node_id,
                function=_func,
                setting_input=node_list_files,
                name=node_type,
                node_type=node_type,
                parent_uuid=self.uuid,
                schema_callback=schema_callback,
            )
            node.user_provided_schema_callback = schema_callback
            self._node_db[node_list_files.node_id] = node
            self.add_node_to_starting_list(node)
            self._node_ids.append(node_list_files.node_id)
        self._notify_node_observers(node, is_new=is_new)

    def add_manual_input(self, input_file: input_schema.NodeManualInput):
        """Adds a node for manual data entry.

        This is a convenience alias for `add_datasource`.

        Args:
            input_file: The settings and data for the manual input node.
        """
        self.add_datasource(input_file)
