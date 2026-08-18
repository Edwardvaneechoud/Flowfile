import os

import polars as pl
from polars._typing import CsvEncoding

from flowfile_core.flowfile.flow_data_engine.read_excel_tables import df_from_calamine_xlsx, df_from_openpyxl
from flowfile_core.flowfile.flow_data_engine.sample_data import create_fake_data
from flowfile_core.schemas import input_schema
from shared.path_utils import NoFilesMatchedError, expand_glob_pattern, is_url

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
        data = pl.read_csv_batched(
            f,
            low_memory=low_mem,
            separator=table_settings.delimiter,
            has_header=table_settings.has_headers,
            skip_rows=table_settings.starting_from_line,
            encoding=table_settings.encoding,
            ignore_errors=True,
            batch_size=2,
            **fallback_infer,
        ).next_batches(1)
        return data[0].lazy()


def create_random(number_of_records: int = 1000) -> pl.LazyFrame:
    return create_fake_data(number_of_records).lazy()


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


def create_from_path_excel(received_table: input_schema.ReceivedTable):
    if not isinstance(received_table.table_settings, input_schema.InputExcelTable):
        raise ValueError("Received table settings are not of type InputExcelTable")
    table_settings: input_schema.InputExcelTable = received_table.table_settings
    if table_settings.type_inference:
        engine = "openpyxl"
    elif table_settings.start_row > 0 and table_settings.start_column == 0:
        engine = "calamine" if table_settings.has_headers else "xlsx2csv"
    elif table_settings.start_column > 0 or table_settings.start_row > 0:
        engine = "openpyxl"
    else:
        engine = "calamine"

    sheet_name = table_settings.sheet_name

    if engine == "calamine":
        df = df_from_calamine_xlsx(
            file_path=received_table.abs_file_path,
            sheet_name=sheet_name,
            start_row=table_settings.start_row,
            end_row=table_settings.end_row,
        )
        if table_settings.end_column > 0:
            end_col_index = table_settings.end_column
            cols_to_select = [df.columns[i] for i in range(table_settings.start_column, end_col_index)]
            df = df.select(cols_to_select)

    elif engine == "xlsx2csv":
        csv_options = {"skip_rows": table_settings.start_row}
        df = pl.read_excel(
            source=received_table.abs_file_path,
            read_options=csv_options,
            engine="xlsx2csv",
            sheet_name=table_settings.sheet_name,
            has_header=table_settings.has_headers,
        )
        end_col_index = table_settings.end_column if table_settings.end_column > 0 else len(df.columns)
        cols_to_select = [df.columns[i] for i in range(table_settings.start_column, end_col_index)]
        df = df.select(cols_to_select)
        if 0 < table_settings.end_row < len(df):
            df = df.head(table_settings.end_row)

    else:
        max_col = table_settings.end_column if table_settings.end_column > 0 else None
        max_row = table_settings.end_row + 1 if table_settings.end_row > 0 else None
        df = df_from_openpyxl(
            file_path=received_table.abs_file_path,
            sheet_name=table_settings.sheet_name,
            min_row=table_settings.start_row + 1,
            min_col=table_settings.start_column + 1,
            max_row=max_row,
            max_col=max_col,
            has_headers=table_settings.has_headers,
        )
    return df
