import os

import polars as pl
from polars._typing import CsvEncoding

from flowfile_core.configs import logger as _module_logger
from flowfile_core.flowfile.flow_data_engine.sample_data import create_fake_data
from flowfile_core.schemas import input_schema
from shared.excel_reader import read_excel_table
from shared.path_utils import NoFilesMatchedError, expand_glob_pattern, is_url, transcode_text_to_utf8

INFER_SCHEMA_RUNGS = (10_000, 100_000)


def _infer_schema_ladder(configured: int) -> list[int]:
    """Inference lengths to try in order: the configured value first, then any higher ladder rungs."""
    return [configured] + [rung for rung in INFER_SCHEMA_RUNGS if rung > configured]


def _low_memory_scan(received_table: input_schema.ReceivedTable, threshold_gb: float) -> bool:
    """Size-based low-memory heuristic; URLs and directory patterns cannot be stat'ed."""
    f = received_table.abs_file_path
    if is_url(f) or received_table.scan_mode == "directory":
        return False
    return os.path.getsize(f) / 1024 / 1000 / 1000 > threshold_gb


def _resolve_scan_source(received_table: input_schema.ReceivedTable) -> str | list[str]:
    """Return the source polars should scan: one path, or the expanded file list in directory mode.

    Expansion happens here so a zero-match pattern raises before any ``pl.scan_*`` is built —
    the csv inference ladder swallows per-rung exceptions and would otherwise degrade a
    zero-match into the lossy fallback.
    """
    if received_table.scan_mode != "directory":
        return received_table.abs_file_path
    matches = expand_glob_pattern(received_table.abs_file_path)
    if not matches:
        raise NoFilesMatchedError(
            f"No files matched '{received_table.path}' (expanded pattern: {received_table.abs_file_path})"
        )
    return matches


def _scan_extra_kwargs(received_table: input_schema.ReceivedTable) -> dict:
    """Optional polars scan kwargs shared by the csv/parquet/ipc readers.

    Directory mode passes ``glob=False``: the file list is already fully expanded, and polars'
    own globbing would reinterpret literal filenames containing ``[``/``*``/``?`` as patterns,
    silently dropping those files.
    """
    extra: dict = {}
    if received_table.scan_mode == "directory":
        extra["glob"] = False
    if received_table.include_file_paths:
        extra["include_file_paths"] = received_table.include_file_paths
    return extra


def _canonical_dtype(dtype: pl.DataType) -> pl.DataType:
    """Normalize away the differences polars unifies on its own when scanning a file list:
    struct fields align by name (order-insensitive) and datetimes coerce across time zones.
    The assertion below must reject only what polars itself would reject at collect time."""
    if isinstance(dtype, pl.Struct):
        return pl.Struct(
            {field.name: _canonical_dtype(field.dtype) for field in sorted(dtype.fields, key=lambda f: f.name)}
        )
    if isinstance(dtype, pl.List):
        return pl.List(_canonical_dtype(dtype.inner))
    if isinstance(dtype, pl.Array):
        return pl.Array(_canonical_dtype(dtype.inner), dtype.size)
    if isinstance(dtype, pl.Datetime):
        return pl.Datetime(dtype.time_unit)
    return dtype


def _assert_uniform_columns(matches: list[str], scan_single) -> None:
    """Fail a directory scan up front when its files disagree on schema.

    Polars only surfaces parquet/ipc schema divergence when the divergent file is physically
    read, which a lazy run may never do — the run would "succeed" and poison every later
    collect. Checking the (cheap, metadata-only) per-file schemas here turns that into an
    attributable build-time error. csv needs no equivalent: ``pl.scan_csv`` resolves the
    multi-file schema eagerly and raises on its own. Dtypes are compared per column after
    canonicalization, because parquet/ipc get no widening beyond struct-field order and
    datetime time zones — polars raises a SchemaError at collect time for anything else,
    Int32 vs Int64 included.
    """
    expected = scan_single(matches[0], glob=False).collect_schema()
    for path in matches[1:]:
        schema = scan_single(path, glob=False).collect_schema()
        if set(schema.names()) != set(expected.names()):
            raise ValueError(
                f"Directory scan column mismatch: '{path}' has columns {sorted(schema.names())}, "
                f"but '{matches[0]}' has {sorted(expected.names())}."
            )
        for name in schema.names():
            if _canonical_dtype(schema[name]) != _canonical_dtype(expected[name]):
                raise ValueError(
                    f"Directory scan dtype mismatch: column '{name}' is {schema[name]} in "
                    f"'{path}' but {expected[name]} in '{matches[0]}'."
                )


def create_from_json(received_table: input_schema.ReceivedTable):
    f = received_table.abs_file_path
    low_mem = _low_memory_scan(received_table, 10)

    if not isinstance(received_table.table_settings, input_schema.InputJsonTable):
        raise ValueError("Received table settings are not of type InputJsonTable")
    table_settings: input_schema.InputJsonTable = received_table.table_settings

    fallback_infer = {"infer_schema_length": 0} if not table_settings.infer_schema else {}

    if table_settings.encoding.upper() == "UTF8" or table_settings.encoding.upper() == "UTF-8":
        if not table_settings.infer_schema:
            return pl.scan_csv(
                f,
                low_memory=low_mem,
                separator=table_settings.delimiter,
                has_header=table_settings.has_headers,
                skip_rows=table_settings.starting_from_line,
                encoding="utf8",
                infer_schema_length=0,
            )
        for infer_len in _infer_schema_ladder(table_settings.infer_schema_length):
            try:
                data = pl.scan_csv(
                    f,
                    low_memory=low_mem,
                    try_parse_dates=True,
                    separator=table_settings.delimiter,
                    has_header=table_settings.has_headers,
                    skip_rows=table_settings.starting_from_line,
                    encoding="utf8",
                    infer_schema_length=infer_len,
                )
                data.head(1).collect()
                return data
            except Exception:
                continue
        try:
            data = pl.scan_csv(
                f,
                low_memory=low_mem,
                separator=table_settings.delimiter,
                has_header=table_settings.has_headers,
                skip_rows=table_settings.starting_from_line,
                encoding="utf8-lossy",
                ignore_errors=True,
                **fallback_infer,
            )
            return data
        except Exception:
            data = pl.scan_csv(
                f,
                low_memory=low_mem,
                separator=table_settings.delimiter,
                has_header=table_settings.has_headers,
                skip_rows=table_settings.starting_from_line,
                encoding="utf8",
                ignore_errors=True,
                **fallback_infer,
            )
            return data
    else:
        data = pl.read_csv(
            f,
            low_memory=low_mem,
            separator=table_settings.delimiter,
            has_header=table_settings.has_headers,
            skip_rows=table_settings.starting_from_line,
            encoding=table_settings.encoding,
            ignore_errors=True,
            **fallback_infer,
        )
        return data


def standardize_utf8_encoding(non_standardized_encoding: str) -> CsvEncoding:
    if non_standardized_encoding.upper() in ("UTF-8", "UTF8"):
        return "utf8"
    elif non_standardized_encoding.upper() in ("UTF-8-LOSSY", "UTF8-LOSSY"):
        return "utf8-lossy"
    else:
        raise ValueError(f"Encoding {non_standardized_encoding} is not supported.")


def create_from_path_csv(received_table: input_schema.ReceivedTable) -> pl.LazyFrame:
    if not isinstance(received_table.table_settings, input_schema.InputCsvTable):
        raise ValueError("Received table settings are not of type InputCsvTable")

    table_settings: input_schema.InputCsvTable = received_table.table_settings

    f = received_table.abs_file_path
    low_mem = _low_memory_scan(received_table, 10)

    fallback_infer = {"infer_schema_length": 0} if not table_settings.infer_schema else {}

    if table_settings.encoding.upper() in ("UTF-8", "UTF8", "UTF8-LOSSY", "UTF-8-LOSSY"):
        encoding: CsvEncoding = standardize_utf8_encoding(table_settings.encoding)
        source = _resolve_scan_source(received_table)
        extra = _scan_extra_kwargs(received_table)
        if not table_settings.infer_schema:
            # No type inference: every column stays text (Utf8).
            return pl.scan_csv(
                source,
                low_memory=low_mem,
                separator=table_settings.delimiter,
                has_header=table_settings.has_headers,
                skip_rows=table_settings.starting_from_line,
                encoding=encoding,
                infer_schema_length=0,
                **extra,
            )
        # The head(1) probe reads the first CSV batch, so a type conflict inside it fails here;
        # widen the inference window before resorting to the lossy ignore_errors fallback.
        for infer_len in _infer_schema_ladder(table_settings.infer_schema_length):
            try:
                data = pl.scan_csv(
                    source,
                    low_memory=low_mem,
                    try_parse_dates=True,
                    separator=table_settings.delimiter,
                    has_header=table_settings.has_headers,
                    skip_rows=table_settings.starting_from_line,
                    encoding=encoding,
                    infer_schema_length=infer_len,
                    **extra,
                )
                data.head(1).collect()
                return data
            except Exception:
                continue
        try:
            data = pl.scan_csv(
                source,
                low_memory=low_mem,
                separator=table_settings.delimiter,
                has_header=table_settings.has_headers,
                skip_rows=table_settings.starting_from_line,
                encoding="utf8-lossy",
                ignore_errors=True,
                **fallback_infer,
                **extra,
            )
            return data
        except Exception:
            data = pl.scan_csv(
                source,
                low_memory=False,
                separator=table_settings.delimiter,
                has_header=table_settings.has_headers,
                skip_rows=table_settings.starting_from_line,
                encoding=encoding,
                ignore_errors=True,
                **fallback_infer,
                **extra,
            )
            return data
    else:
        # Transcoded in memory: polars has no lazy path for non-utf8 encodings, gzipped or not.
        data = pl.read_csv(
            transcode_text_to_utf8(f, table_settings.encoding),
            low_memory=low_mem,
            separator=table_settings.delimiter,
            has_header=table_settings.has_headers,
            skip_rows=table_settings.starting_from_line,
            encoding="utf8",
            ignore_errors=True,
            **fallback_infer,
        )
        return data.lazy()


def create_random(number_of_records: int = 1000) -> pl.LazyFrame:
    return create_fake_data(number_of_records).lazy()


def parquet_row_count(received_table: input_schema.ReceivedTable) -> int | None:
    """Exact row count from parquet footers alone — metadata reads, never a data scan.

    Returns None whenever the count is not knowable for free (URLs, unreadable
    footers), so callers keep their unknown-count sentinel in that case.
    """
    from pyarrow.parquet import ParquetFile

    try:
        source = _resolve_scan_source(received_table)
        paths = source if isinstance(source, list) else [source]
        if any(is_url(str(p)) for p in paths):
            return None
        return sum(ParquetFile(p).metadata.num_rows for p in paths)
    except Exception:
        return None


def create_from_path_parquet(received_table: input_schema.ReceivedTable) -> pl.LazyFrame:
    if not isinstance(received_table.table_settings, input_schema.InputParquetTable):
        raise ValueError("Received table settings are not of type InputParquetTable")
    low_mem = _low_memory_scan(received_table, 2)
    source = _resolve_scan_source(received_table)
    if isinstance(source, list):
        _assert_uniform_columns(source, pl.scan_parquet)
    return pl.scan_parquet(source=source, low_memory=low_mem, **_scan_extra_kwargs(received_table))


def create_from_path_ipc(received_table: input_schema.ReceivedTable) -> pl.LazyFrame:
    if not isinstance(received_table.table_settings, input_schema.InputIpcTable):
        raise ValueError("Received table settings are not of type InputIpcTable")
    source = _resolve_scan_source(received_table)
    if isinstance(source, list):
        _assert_uniform_columns(source, pl.scan_ipc)
    return pl.scan_ipc(source, **_scan_extra_kwargs(received_table))


def create_from_path_ndjson(received_table: input_schema.ReceivedTable) -> pl.LazyFrame:
    if not isinstance(received_table.table_settings, input_schema.InputNdjsonTable):
        raise ValueError("Received table settings are not of type InputNdjsonTable")
    f = received_table.abs_file_path
    low_mem = _low_memory_scan(received_table, 10)
    return pl.scan_ndjson(f, low_memory=low_mem)


def create_from_path_avro(received_table: input_schema.ReceivedTable) -> pl.DataFrame:
    if not isinstance(received_table.table_settings, input_schema.InputAvroTable):
        raise ValueError("Received table settings are not of type InputAvroTable")
    return pl.read_avro(received_table.abs_file_path)


def create_from_path_ipc_stream(received_table: input_schema.ReceivedTable) -> pl.DataFrame:
    if not isinstance(received_table.table_settings, input_schema.InputIpcStreamTable):
        raise ValueError("Received table settings are not of type InputIpcStreamTable")
    return pl.read_ipc_stream(received_table.abs_file_path)


def probe_eager_schema(received_table: input_schema.ReceivedTable) -> pl.Schema:
    """Schema of a format polars cannot scan (avro, ipc_stream) from a zero-row eager read.

    Lets the read node predict its schema before it runs, like the excel header probe; the data
    itself still comes from the worker in remote mode.
    """
    readers = {"avro": pl.read_avro, "ipc_stream": pl.read_ipc_stream}
    return readers[received_table.file_type](received_table.abs_file_path, n_rows=0).schema


def create_from_path_excel(received_table: input_schema.ReceivedTable, logger=None):
    """Read an Excel sheet; see ``shared.excel_reader`` for engine selection and the openpyxl fallback.

    ``logger`` is the node logger during a run, so the engine used lands in the flow log.
    """
    if not isinstance(received_table.table_settings, input_schema.InputExcelTable):
        raise ValueError("Received table settings are not of type InputExcelTable")
    return read_excel_table(
        received_table.abs_file_path, received_table.table_settings, logger=logger or _module_logger
    )
