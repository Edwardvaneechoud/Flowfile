import io
import logging
import os
from collections.abc import Callable
from logging import Logger
from multiprocessing import Array, Queue, Value
from pathlib import Path

import polars as pl
from deltalake import DeltaTable
from pl_fuzzy_frame_match import FuzzyMapping, fuzzy_match_dfs

from flowfile_worker import models
from flowfile_worker.external_sources.s3_source.main import write_df_to_cloud
from flowfile_worker.external_sources.s3_source.models import CloudStorageWriteSettings
from flowfile_worker.external_sources.sql_source.main import write_df_to_database
from flowfile_worker.external_sources.sql_source.models import DatabaseWriteSettings
from flowfile_worker.flow_logger import get_worker_logger
from flowfile_worker.utils import collect_lazy_frame, collect_lazy_frame_and_get_streaming_info
from shared.delta_utils import format_delta_timestamp, get_delta_size_bytes, make_json_safe, validate_catalog_path
from shared.storage_config import storage


def _validate_catalog_path(table_name: str) -> Path:
    """Validate and resolve *table_name* under the catalog tables directory."""
    return validate_catalog_path(table_name, storage.catalog_tables_directory)


def _get_delta_size_bytes(delta_dir: Path) -> int:
    """Delegate to ``shared.delta_utils.get_delta_size_bytes``."""
    return get_delta_size_bytes(delta_dir)


# 'store', 'calculate_schema', 'calculate_number_of_records', 'write_output', 'fuzzy', 'store_sample']

logging.basicConfig(format="%(asctime)s: %(message)s")
logger = logging.getLogger("Spawner")
logger.setLevel(logging.INFO)


def fuzzy_join_task(
    left_serializable_object: bytes,
    right_serializable_object: bytes,
    fuzzy_maps: list[FuzzyMapping],
    error_message: Array,
    file_path: str,
    progress: Value,
    queue: Queue,
    flowfile_flow_id: int,
    flowfile_node_id: int | str,
):
    flowfile_logger = get_worker_logger(flowfile_flow_id, flowfile_node_id)
    try:
        flowfile_logger.info("Starting fuzzy join operation")
        left_df = pl.LazyFrame.deserialize(io.BytesIO(left_serializable_object))
        right_df = pl.LazyFrame.deserialize(io.BytesIO(right_serializable_object))
        fuzzy_match_result = fuzzy_match_dfs(
            left_df=left_df, right_df=right_df, fuzzy_maps=fuzzy_maps, logger=flowfile_logger
        )
        flowfile_logger.info("Fuzzy join operation completed successfully")
        fuzzy_match_result.write_ipc(file_path)
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        error_msg = str(e).encode()[:256]
        with error_message.get_lock():
            error_message[: len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1
        flowfile_logger.error(f"Error during fuzzy join operation: {str(e)}")
    lf = pl.scan_ipc(file_path)
    number_of_records = collect_lazy_frame(lf.select(pl.len()))[0, 0]
    flowfile_logger.info(f"Number of records after fuzzy match: {number_of_records}")
    # Put raw bytes in queue - encoding happens at the transport boundary
    queue.put(lf.serialize())


def process_and_cache(
    polars_serializable_object: io.BytesIO,
    progress: Value,
    error_message: Array,
    file_path: str,
    flowfile_logger: Logger,
) -> bytes:
    try:
        lf = pl.LazyFrame.deserialize(polars_serializable_object)
        collect_lazy_frame(lf).write_ipc(file_path)
        flowfile_logger.info("Process operation completed successfully")
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        error_msg = str(e).encode()[:1024]  # Limit error message length
        flowfile_logger.error(f"Error during process and cache operation: {str(e)}")
        with error_message.get_lock():
            error_message[: len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1  # Indicate error
        return error_msg


def store_sample(
    polars_serializable_object: bytes,
    progress: Value,
    error_message: Array,
    queue: Queue,
    file_path: str,
    sample_size: int,
    flowfile_flow_id: int,
    flowfile_node_id: int | str,
):
    flowfile_logger = get_worker_logger(flowfile_flow_id, flowfile_node_id)
    flowfile_logger.info("Starting store sample operation")
    try:
        lf = pl.LazyFrame.deserialize(io.BytesIO(polars_serializable_object))
        collect_lazy_frame(lf.limit(sample_size)).write_ipc(file_path)
        flowfile_logger.info("Store sample operation completed successfully")
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        flowfile_logger.error(f"Error during store sample operation: {str(e)}")
        error_msg = str(e).encode()[:1024]  # Limit error message length
        with error_message.get_lock():
            error_message[: len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1  # Indicate error
        return error_msg


def store(
    polars_serializable_object: bytes,
    progress: Value,
    error_message: Array,
    queue: Queue,
    file_path: str,
    flowfile_flow_id: int,
    flowfile_node_id: int | str,
):
    flowfile_logger = get_worker_logger(flowfile_flow_id, flowfile_node_id)
    flowfile_logger.info("Starting store operation")
    polars_serializable_object_io = io.BytesIO(polars_serializable_object)
    process_and_cache(polars_serializable_object_io, progress, error_message, file_path, flowfile_logger)
    lf = pl.scan_ipc(file_path)
    number_of_records = collect_lazy_frame(lf.select(pl.len()))[0, 0]
    flowfile_logger.info(f"Number of records processed: {number_of_records}")
    # Put raw bytes in queue - encoding happens at the transport boundary
    queue.put(lf.serialize())


def calculate_schema_logic(
    df: pl.LazyFrame, optimize_memory: bool = True, flowfile_logger: Logger = None
) -> list[dict]:
    if flowfile_logger is None:
        raise ValueError("flowfile_logger is required")
    schema = df.collect_schema()
    schema_stats = [dict(column_name=k, pl_datatype=str(v), col_index=i) for i, (k, v) in enumerate(schema.items())]
    flowfile_logger.info("Starting to calculate the number of records")
    collected_streaming_info = collect_lazy_frame_and_get_streaming_info(df.select(pl.len()))
    n_records = collected_streaming_info.df[0, 0]
    if n_records < 10_000:
        flowfile_logger.info("Collecting the whole dataset")
        df = collect_lazy_frame(df).lazy()
    if optimize_memory and n_records > 1_000_000:
        df = df.head(1_000_000)
    null_cols = [col for col, data_type in schema.items() if data_type is pl.Null]
    if not (n_records == 0 and df.width == 0):
        if len(null_cols) == 0:
            pl_stats = df.describe()
        else:
            df = df.drop(null_cols)
            pl_stats = df.describe()
        n_unique_per_cols = list(
            df.select(pl.all().approx_n_unique())
            .collect(engine="streaming" if collected_streaming_info.streaming_collect_available else "auto")
            .to_dicts()[0]
            .values()
        )
        stats_headers = pl_stats.drop_in_place("statistic").to_list()
        stats = {
            v["column_name"]: v
            for v in pl_stats.transpose(
                include_header=True, header_name="column_name", column_names=stats_headers
            ).to_dicts()
        }
        for _i, (col_stat, n_unique_values) in enumerate(zip(stats.values(), n_unique_per_cols, strict=False)):
            col_stat["n_unique"] = n_unique_values
            col_stat["examples"] = ", ".join({str(col_stat["min"]), str(col_stat["max"])})
            col_stat["null_count"] = int(float(col_stat["null_count"]))
            col_stat["count"] = int(float(col_stat["count"]))

        for schema_stat in schema_stats:
            deep_stat = stats.get(schema_stat["column_name"])
            if deep_stat:
                schema_stat.update(deep_stat)
        del df
    else:
        schema_stats = []
    return schema_stats


def calculate_schema(
    polars_serializable_object: bytes,
    progress: Value,
    error_message: Array,
    queue: Queue,
    flowfile_flow_id: int,
    flowfile_node_id: int | str,
    *args,
    **kwargs,
):
    polars_serializable_object_io = io.BytesIO(polars_serializable_object)
    flowfile_logger = get_worker_logger(flowfile_flow_id, flowfile_node_id)
    flowfile_logger.info("Starting schema calculation")
    try:
        lf = pl.LazyFrame.deserialize(polars_serializable_object_io)
        schema_stats = calculate_schema_logic(lf, flowfile_logger=flowfile_logger)
        flowfile_logger.info("schema_stats", schema_stats)
        queue.put(schema_stats)
        flowfile_logger.info("Schema calculation completed successfully")
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        error_msg = str(e).encode()[:256]  # Limit error message length
        flowfile_logger.error("error", e)
        with error_message.get_lock():
            error_message[: len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1  # Indicate error


def calculate_number_of_records(
    polars_serializable_object: bytes,
    progress: Value,
    error_message: Array,
    queue: Queue,
    flowfile_flow_id: int,
    *args,
    **kwargs,
):
    flowfile_logger = get_worker_logger(flowfile_flow_id, -1)
    flowfile_logger.info("Starting number of records calculation")
    polars_serializable_object_io = io.BytesIO(polars_serializable_object)
    try:
        lf = pl.LazyFrame.deserialize(polars_serializable_object_io)
        n_records = collect_lazy_frame(lf.select(pl.len()))[0, 0]
        queue.put(n_records)
        flowfile_logger.debug("Number of records calculation completed successfully")
        flowfile_logger.debug(f"n_records {n_records}")
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        flowfile_logger.error("error", e)
        error_msg = str(e).encode()[:256]  # Limit error message length
        with error_message.get_lock():
            error_message[: len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1  # Indicate error
        return b"error"


def execute_write_method(
    write_method: Callable,
    path: str,
    data_type: str = None,
    sheet_name: str = None,
    delimiter: str = None,
    write_mode: str = "create",
    flowfile_logger: Logger = None,
):
    flowfile_logger.info("executing write method")
    if data_type == "excel":
        logger.info("Writing as excel file")
        write_method(path, worksheet=sheet_name)
    elif data_type == "csv":
        logger.info("Writing as csv file")
        if write_mode == "append":
            with open(path, "ab") as f:
                write_method(f, separator=delimiter, quote_style="always")
        else:
            write_method(path, separator=delimiter, quote_style="always")
    elif data_type == "parquet":
        logger.info("Writing as parquet file")
        write_method(path)


def write_to_database(
    polars_serializable_object: bytes,
    progress: Value,
    error_message: Array,
    queue: Queue,
    file_path: str,
    database_write_settings: DatabaseWriteSettings,
    flowfile_flow_id: int = -1,
    flowfile_node_id: int | str = -1,
):
    """
    Writes a Polars DataFrame to a SQL database.
    """
    flowfile_logger = get_worker_logger(flowfile_flow_id, flowfile_node_id)
    flowfile_logger.info(f"Starting write operation to: {database_write_settings.table_name}")
    df = collect_lazy_frame(pl.LazyFrame.deserialize(io.BytesIO(polars_serializable_object)))
    flowfile_logger.info(f"Starting to write {len(df)} records")
    try:
        write_df_to_database(df, database_write_settings)
        flowfile_logger.info("Write operation completed successfully")
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        error_msg = str(e).encode()[:1024]
        flowfile_logger.error(f"Error during write operation: {str(e)}")
        with error_message.get_lock():
            error_message[: len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1


def write_to_cloud_storage(
    polars_serializable_object: bytes,
    progress: Value,
    error_message: Array,
    queue: Queue,
    file_path: str,
    cloud_write_settings: CloudStorageWriteSettings,
    flowfile_flow_id: int = -1,
    flowfile_node_id: int | str = -1,
) -> None:
    """
    Writes a Polars DataFrame to cloud storage using the provided settings.
    Args:
        polars_serializable_object ():  # Serialized Polars DataFrame object
        progress (): Multiprocessing Value to track progress
        error_message (): Array to store error messages
        queue (): Queue to send results back
        file_path (): Path to the file where the DataFrame will be written
        cloud_write_settings (): CloudStorageWriteSettings object containing write settings and connection details
        flowfile_flow_id (): Flowfile flow ID for logging
        flowfile_node_id (): Flowfile node ID for logging

    Returns:
        None
    """
    flowfile_logger = get_worker_logger(flowfile_flow_id, flowfile_node_id)
    flowfile_logger.info(f"Starting write operation to: {cloud_write_settings.write_settings.resource_path}")
    df = pl.LazyFrame.deserialize(io.BytesIO(polars_serializable_object))
    flowfile_logger.info(f"Starting to sync the data to cloud, execution plan: \n" f"{df.explain(format='plain')}")
    try:
        write_df_to_cloud(df, cloud_write_settings, flowfile_logger)
        flowfile_logger.info("Write operation completed successfully")
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        error_msg = str(e).encode()[:1024]
        flowfile_logger.error(f"Error during write operation: {str(e)}")
        with error_message.get_lock():
            error_message[: len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1


def write_output(
    polars_serializable_object: bytes,
    progress: Value,
    error_message: Array,
    queue: Queue,
    file_path: str,
    data_type: str,
    path: str,
    write_mode: str,
    sheet_name: str = None,
    delimiter: str = None,
    flowfile_flow_id: int = -1,
    flowfile_node_id: int | str = -1,
):
    flowfile_logger = get_worker_logger(flowfile_flow_id, flowfile_node_id)
    flowfile_logger.info(f"Starting write operation to: {path}")
    try:
        df = pl.LazyFrame.deserialize(io.BytesIO(polars_serializable_object))
        if isinstance(df, pl.LazyFrame):
            flowfile_logger.info(f'Execution plan explanation:\n{df.explain(format="plain")}')
        flowfile_logger.info("Successfully deserialized dataframe")
        sink_method_str = "sink_" + data_type
        write_method_str = "write_" + data_type
        has_sink_method = hasattr(df, sink_method_str)
        write_method = None
        if os.path.exists(path) and write_mode == "create":
            raise Exception("File already exists")
        if has_sink_method and write_method != "append":
            flowfile_logger.info(f"Using sink method: {sink_method_str}")
            write_method = getattr(df, "sink_" + data_type)
        elif not has_sink_method:
            if isinstance(df, pl.LazyFrame):
                df = collect_lazy_frame(df)
            write_method = getattr(df, write_method_str)
        if write_method is not None:
            execute_write_method(
                write_method,
                path=path,
                data_type=data_type,
                sheet_name=sheet_name,
                delimiter=delimiter,
                write_mode=write_mode,
                flowfile_logger=flowfile_logger,
            )
            number_of_records_written = (
                collect_lazy_frame(df.select(pl.len()))[0, 0] if isinstance(df, pl.LazyFrame) else df.height
            )
            flowfile_logger.info(f"Number of records written: {number_of_records_written}")
        else:
            raise Exception("Write method not found")
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        logger.info(f"Error during write operation: {str(e)}")
        error_message[: len(str(e))] = str(e).encode()


def write_parquet(
    polars_serializable_object: bytes,
    progress: Value,
    error_message: Array,
    queue: Queue,
    file_path: str,
    output_path: str,
    flowfile_flow_id: int = -1,
    flowfile_node_id: int | str = -1,
):
    """Collect a serialized LazyFrame and write it to a parquet file.

    This offloads the collect() from core to the worker process, producing
    a Polars-version-independent parquet file at *output_path*.
    """
    flowfile_logger = get_worker_logger(flowfile_flow_id, flowfile_node_id)
    flowfile_logger.info(f"Starting write_parquet operation to: {output_path}")
    try:
        lf = pl.LazyFrame.deserialize(io.BytesIO(polars_serializable_object))
        df = collect_lazy_frame(lf)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.write_parquet(output_path)
        # Flush to disk to prevent race conditions when another process reads
        with open(output_path, "rb") as f:
            os.fsync(f.fileno())
        flowfile_logger.info(f"write_parquet completed: {len(df)} records written to {output_path}")
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        error_msg = str(e).encode()[:1024]
        flowfile_logger.error(f"Error during write_parquet operation: {str(e)}")
        with error_message.get_lock():
            error_message[: len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1


def write_delta(
    polars_serializable_object: bytes,
    progress: Value,
    error_message: Array,
    queue: Queue,
    file_path: str,
    output_path: str,
    mode: str = "overwrite",
    flowfile_flow_id: int = -1,
    flowfile_node_id: int | str = -1,
):
    """Collect a serialized LazyFrame and write it to a Delta table directory.

    This offloads the collect() from core to the worker process, producing
    a Delta table at *output_path*.  Metadata (schema, row_count, size_bytes)
    is returned via the queue so the core never needs to read the table.
    """
    flowfile_logger = get_worker_logger(flowfile_flow_id, flowfile_node_id)
    flowfile_logger.info(f"Starting write_delta operation to: {output_path}")
    try:
        from shared.delta_utils import write_delta as _write_delta

        lf = pl.LazyFrame.deserialize(io.BytesIO(polars_serializable_object))
        df = collect_lazy_frame(lf)
        wrote = _write_delta(df, output_path, mode=mode)

        if not wrote:
            queue.put({"skipped": True})
            flowfile_logger.info(f"write_delta skipped (no-op) for {output_path}")
            with progress.get_lock():
                progress.value = 100
            return

        size_bytes = _get_delta_size_bytes(Path(output_path))
        schema = [{"name": col, "dtype": str(df[col].dtype)} for col in df.columns]

        queue.put(
            {
                "table_path": output_path,
                "storage_format": "delta",
                "schema": schema,
                "row_count": df.height,
                "column_count": len(df.columns),
                "size_bytes": size_bytes,
            }
        )
        flowfile_logger.info(f"write_delta completed: {df.height} records written to {output_path}")
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        error_msg = str(e).encode()[:1024]
        flowfile_logger.error(f"Error during write_delta operation: {str(e)}")
        with error_message.get_lock():
            error_message[: len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1


def merge_delta(
    polars_serializable_object: bytes,
    progress: Value,
    error_message: Array,
    queue: Queue,
    file_path: str,
    output_path: str,
    merge_mode: str = "upsert",
    merge_keys: list[str] | None = None,
    flowfile_flow_id: int = -1,
    flowfile_node_id: int | str = -1,
):
    """Collect a serialized LazyFrame and merge it into a Delta table.

    Supports three merge modes:
    - upsert: update matched rows + insert unmatched
    - update: update only matched rows (no inserts)
    - delete: remove matched rows from target
    """
    flowfile_logger = get_worker_logger(flowfile_flow_id, flowfile_node_id)
    flowfile_logger.info(f"Starting merge_delta ({merge_mode}) to: {output_path}")
    try:
        from shared.delta_utils import merge_into_delta

        lf = pl.LazyFrame.deserialize(io.BytesIO(polars_serializable_object))
        df = collect_lazy_frame(lf)
        wrote = merge_into_delta(df, output_path, merge_mode=merge_mode, merge_keys=merge_keys)

        if not wrote:
            queue.put({"skipped": True})
            flowfile_logger.info(f"merge_delta skipped (no-op) for {output_path}")
            with progress.get_lock():
                progress.value = 100
            return

        # Read back metadata from the resulting table
        result_df = pl.scan_delta(output_path)
        result_schema = result_df.collect_schema()
        schema = [{"name": n, "dtype": str(d)} for n, d in result_schema.items()]
        row_count = result_df.select(pl.len()).collect().item()
        size_bytes = _get_delta_size_bytes(Path(output_path))

        queue.put(
            {
                "table_path": output_path,
                "storage_format": "delta",
                "schema": schema,
                "row_count": row_count,
                "column_count": len(schema),
                "size_bytes": size_bytes,
            }
        )
        flowfile_logger.info(f"merge_delta ({merge_mode}) completed: {row_count} rows in {output_path}")
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        error_msg = str(e).encode()[:1024]
        flowfile_logger.error(f"Error during merge_delta operation: {str(e)}")
        with error_message.get_lock():
            error_message[: len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1


def materialize_catalog_table_task(
    source_file_path: str,
    dest_path: str,
    progress: Value,
    error_message: Array,
    queue: Queue,
):
    """Subprocess task: reads a source file and materializes it as a Delta table, returning metadata via queue."""
    try:
        ext = os.path.splitext(source_file_path)[1].lower()
        if ext in (".csv", ".txt", ".tsv"):
            df = pl.scan_csv(source_file_path, infer_schema_length=10000, encoding="utf8-lossy")
        elif ext == ".parquet":
            df = pl.read_parquet(source_file_path)
        elif ext in (".xlsx", ".xls"):
            df = pl.read_excel(source_file_path)
        else:
            raise ValueError(f"Unsupported file type: {ext}")

        if isinstance(df, pl.LazyFrame):
            df = df.collect()

        os.makedirs(dest_path, exist_ok=True)
        df.write_delta(dest_path, mode="overwrite")

        size_bytes = _get_delta_size_bytes(Path(dest_path))
        schema = [{"name": col, "dtype": str(df[col].dtype)} for col in df.columns]

        queue.put(
            {
                "table_path": dest_path,
                "storage_format": "delta",
                "schema": schema,
                "row_count": df.height,
                "column_count": len(df.columns),
                "size_bytes": size_bytes,
            }
        )
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        error_msg = str(e).encode()[:1024]
        with error_message.get_lock():
            error_message[: len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1


def execute_sql_query(
    query: str,
    tables: dict[str, str],
    max_rows: int = 10_000,
    virtual_tables_ipc: dict[str, str] | None = None,
) -> dict:
    """Execute a SQL query against catalog tables using pl.SQLContext.

    *tables* is a mapping of logical table name -> directory name.  The
    directory name is resolved under the catalog tables directory using
    ``_validate_catalog_path``.  Only tables actually referenced in the
    query plan are reported in *used_tables*.

    *virtual_tables_ipc* is an optional mapping of virtual table name ->
    base64-encoded IPC bytes for pre-resolved virtual tables.

    Returns a dict matching the SqlQueryResponse schema.
    """
    import base64
    import io
    import re
    import time

    start = time.perf_counter()

    ctx = pl.SQLContext()
    registered_names: list[str] = []
    for name, dir_name in tables.items():
        p = _validate_catalog_path(dir_name)
        if not p.is_dir() or not (p / "_delta_log").is_dir():
            raise ValueError(f"Table '{name}' is not a valid Delta table")
        ctx.register(name, pl.scan_delta(str(p)))
        registered_names.append(name)

    # Register virtual tables from pre-resolved IPC data
    if virtual_tables_ipc:
        for name, b64_data in virtual_tables_ipc.items():
            ipc_bytes = base64.b64decode(b64_data)
            lf = pl.read_ipc(io.BytesIO(ipc_bytes)).lazy()
            ctx.register(name, lf)
            registered_names.append(name)

    result_lf = ctx.execute(query)

    # Determine which registered tables were actually used via the query plan
    plan = result_lf.explain()
    used_tables = [name for name in registered_names if re.search(r"\b" + re.escape(name) + r"\b", plan)]

    # Collect with row limit
    schema = result_lf.collect_schema()
    columns = list(schema.keys())
    dtypes = [str(d) for d in schema.values()]

    total_df = result_lf.select(pl.len()).collect()
    total_rows = total_df.item()

    truncated = total_rows > max_rows
    df = result_lf.head(max_rows).collect()

    rows_data = df.to_dicts()
    rows = [[make_json_safe(row.get(c)) for c in columns] for row in rows_data]

    elapsed_ms = (time.perf_counter() - start) * 1000

    return {
        "columns": columns,
        "dtypes": dtypes,
        "rows": rows,
        "total_rows": total_rows,
        "truncated": truncated,
        "execution_time_ms": round(elapsed_ms, 1),
        "used_tables": used_tables,
    }


def read_table_metadata(table_name: str, storage_format: str = "delta") -> dict:
    """Read schema, row_count, column_count, size_bytes from a table on disk.

    *table_name* is the bare directory/file name inside the catalog tables
    directory (no path separators allowed).

    Called by the worker endpoint so the core process never touches data files.
    """
    p = _validate_catalog_path(table_name)
    if storage_format == "delta" or (p.is_dir() and (p / "_delta_log").is_dir()):
        lf = pl.scan_delta(str(p))
        schema = lf.collect_schema()
        schema_list = [{"name": n, "dtype": str(d)} for n, d in schema.items()]
        row_count = lf.select(pl.len()).collect().item()
        size_bytes = _get_delta_size_bytes(p)
    else:
        lf = pl.scan_parquet(p)
        schema = lf.collect_schema()
        schema_list = [{"name": n, "dtype": str(d)} for n, d in schema.items()]
        row_count = lf.select(pl.len()).collect().item()
        size_bytes = p.stat().st_size
    return {
        "schema": schema_list,
        "row_count": row_count,
        "column_count": len(schema_list),
        "size_bytes": size_bytes,
    }


def get_delta_history(table_name: str, limit: int | None = None) -> models.DeltaHistoryResponse:
    """Read version history from a Delta table using the deltalake library.

    *table_name* is the bare directory name inside the catalog tables directory.
    """
    validated = _validate_catalog_path(table_name)
    dt = DeltaTable(str(validated))
    history = dt.history(limit)
    current_version = dt.version()
    entries: list[models.DeltaVersionCommit] = []
    for h in history:
        entries.append(
            models.DeltaVersionCommit(
                version=h.get("version"),
                timestamp=format_delta_timestamp(h.get("timestamp")),
                operation=h.get("operation"),
                parameters=h.get("operationParameters"),
            )
        )
    return models.DeltaHistoryResponse(current_version=current_version, history=entries)


def read_delta_version_preview(table_name: str, version: int, n_rows: int = 100) -> models.DeltaVersionPreviewResponse:
    """Read a preview of a Delta table at a specific version using deltalake + PyArrow (no Polars).

    *table_name* is the bare directory name inside the catalog tables directory.
    """
    validated = _validate_catalog_path(table_name)
    dt = DeltaTable(str(validated), version=version)
    dataset = dt.to_pyarrow_dataset()
    pa_table = dataset.head(n_rows)
    columns = pa_table.column_names
    dtypes = [str(field.type) for field in pa_table.schema]
    rows = pa_table.to_pylist()

    row_list = [[make_json_safe(row.get(c)) for c in columns] for row in rows]
    # Estimate total rows from Delta metadata
    try:
        total_rows = sum(
            v for v in dt.get_add_actions(flatten=True).to_pydict().get("num_records", []) if v is not None
        )
    except Exception:
        total_rows = len(row_list)
    if total_rows == 0:
        total_rows = len(row_list)

    return models.DeltaVersionPreviewResponse(
        version=version,
        columns=columns,
        dtypes=dtypes,
        rows=row_list,
        total_rows=total_rows,
    )


def generic_task(
    func: Callable,
    progress: Value,
    error_message: Array,
    queue: Queue,
    file_path: str,
    flowfile_flow_id: int,
    flowfile_node_id: int | str,
    *args,
    **kwargs,
):
    flowfile_logger = get_worker_logger(flowfile_flow_id, flowfile_node_id)
    flowfile_logger.info("Starting generic task")
    try:
        result = func(*args, **kwargs)
        if result is None:
            # Function already wrote to file_path (e.g. Kafka spill_path)
            flowfile_logger.info("Function returned None — file already written")
        elif isinstance(result, pl.LazyFrame):
            collect_lazy_frame(result).write_ipc(file_path)
        elif isinstance(result, pl.DataFrame):
            result.write_ipc(file_path)
        else:
            raise Exception("Returned object is not a DataFrame, LazyFrame, or None")
        with progress.get_lock():
            progress.value = 100
        flowfile_logger.info("Task completed successfully")
    except Exception as e:
        flowfile_logger.error(f"Error during task execution: {str(e)}")
        error_msg = str(e).encode()[:1024]
        with error_message.get_lock():
            error_message[: len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1

    lf = pl.scan_ipc(file_path)
    number_of_records = collect_lazy_frame(lf.select(pl.len()))[0, 0]
    flowfile_logger.info(f"Number of records processed: {number_of_records}")
    # Put raw bytes in queue - encoding happens at the transport boundary
    queue.put(lf.serialize())


def _signal_error(error_message: Array, progress: Value, exc: Exception, logger_) -> None:
    logger_.error(f"Error during data science operation: {exc}")
    error_msg = str(exc).encode()[:1024]
    with error_message.get_lock():
        error_message[: len(error_msg)] = error_msg
    with progress.get_lock():
        progress.value = -1


def data_science_fit(
    polars_serializable_object: bytes,
    progress: Value,
    error_message: Array,
    queue: Queue,
    file_path: str,
    flowfile_flow_id: int,
    flowfile_node_id: int | str,
    kind: str,
    feature_cols: list[str],
    target_col: str | None,
    artefact_name: str,
    prediction_col: str,
    hyperparams: dict,
    source_registration_id: int,
    source_flow_id: int | None = None,
    source_node_id: int | None = None,
):
    """Fit an estimator on the incoming LazyFrame and publish it to the Catalog.

    Writes a small preview DataFrame (e.g. coefficients) to ``file_path`` so
    the calling node can surface it to the downstream graph.
    """
    flowfile_logger = get_worker_logger(flowfile_flow_id, flowfile_node_id)
    flowfile_logger.info(f"Starting data_science_fit ({kind}) -> publish '{artefact_name}'")
    try:
        from shared.data_science import artefact_io
        from shared.data_science.estimators import fit_estimator

        lf = pl.LazyFrame.deserialize(io.BytesIO(polars_serializable_object))
        df = collect_lazy_frame(lf)

        artefact, preview_df, output_schema = fit_estimator(
            kind=kind,
            df=df,
            feature_cols=feature_cols,
            target_col=target_col,
            hyperparams=hyperparams or {},
            prediction_col=prediction_col,
        )

        artefact_io.publish(
            name=artefact_name,
            artefact=artefact,
            source_registration_id=source_registration_id,
            source_flow_id=source_flow_id,
            source_node_id=source_node_id,
            output_schema=output_schema,
        )

        preview_df.write_ipc(file_path)
        flowfile_logger.info("data_science_fit completed successfully")
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        _signal_error(error_message, progress, e, flowfile_logger)
        return

    out = pl.scan_ipc(file_path)
    queue.put(out.serialize())


def data_science_predict(
    polars_serializable_object: bytes,
    progress: Value,
    error_message: Array,
    queue: Queue,
    file_path: str,
    flowfile_flow_id: int,
    flowfile_node_id: int | str,
    artefact_name: str,
    artefact_version: int | None,
    feature_cols: list[str],
    prediction_col: str,
):
    """Apply a Catalog estimator to the incoming LazyFrame.

    Writes the resulting DataFrame (input + prediction column) to ``file_path``.
    """
    flowfile_logger = get_worker_logger(flowfile_flow_id, flowfile_node_id)
    flowfile_logger.info(f"Starting data_science_predict using artefact '{artefact_name}'")
    try:
        from shared.data_science import artefact_io
        from shared.data_science.predict import apply_predict

        lf = pl.LazyFrame.deserialize(io.BytesIO(polars_serializable_object))

        blob = artefact_io.download_artifact(artefact_name, artefact_version)
        artefact = artefact_io.load_artefact(blob)

        # apply_predict keeps the plan lazy; collect only at the write boundary.
        result_lf = apply_predict(lf, artefact, feature_cols, prediction_col)
        collect_lazy_frame(result_lf).write_ipc(file_path)
        flowfile_logger.info("data_science_predict completed successfully")
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        _signal_error(error_message, progress, e, flowfile_logger)
        return

    out = pl.scan_ipc(file_path)
    queue.put(out.serialize())
