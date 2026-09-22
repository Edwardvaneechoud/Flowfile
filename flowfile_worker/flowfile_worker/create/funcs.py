import os

import polars as pl

from flowfile_worker.configs import logger as _module_logger
from flowfile_worker.create.models import (
    InputAvroTable,
    InputCsvTable,
    InputExcelTable,
    InputIpcStreamTable,
    InputIpcTable,
    InputJsonTable,
    InputNdjsonTable,
    InputParquetTable,
    ReceivedTable,
)
from flowfile_worker.create.utils import create_fake_data
from shared.excel_reader import read_excel_table
from shared.path_utils import is_url

INFER_SCHEMA_RUNGS = (10_000, 100_000)


def _infer_schema_ladder(configured: int) -> list[int]:
    """Inference lengths to try in order: the configured value first, then any higher ladder rungs."""
    return [configured] + [rung for rung in INFER_SCHEMA_RUNGS if rung > configured]


def create_from_path_json(received_table: ReceivedTable):
    if not isinstance(received_table.table_settings, InputJsonTable):
        raise ValueError("Received table settings are not of type InputJsonTable")
    input_table_settings: InputJsonTable = received_table.table_settings
    f = received_table.abs_file_path
    low_mem = False if is_url(f) else os.path.getsize(f) / 1024 / 1000 / 1000 > 10
    fallback_infer = {"infer_schema_length": 0} if not input_table_settings.infer_schema else {}
    if input_table_settings.encoding.upper() == "UTF8" or input_table_settings.encoding.upper() == "UTF-8":
        if not input_table_settings.infer_schema:
            return pl.scan_csv(
                f,
                low_memory=low_mem,
                separator=input_table_settings.delimiter,
                has_header=input_table_settings.has_headers,
                skip_rows=input_table_settings.starting_from_line,
                encoding="utf8",
                infer_schema_length=0,
            )
        for infer_len in _infer_schema_ladder(input_table_settings.infer_schema_length):
            try:
                df = pl.scan_csv(
                    f,
                    low_memory=low_mem,
                    try_parse_dates=True,
                    separator=input_table_settings.delimiter,
                    has_header=input_table_settings.has_headers,
                    skip_rows=input_table_settings.starting_from_line,
                    encoding="utf8",
                    infer_schema_length=infer_len,
                )
                df.head(1).collect()
                return df
            except Exception:
                continue
        try:
            df = pl.scan_csv(
                f,
                low_memory=low_mem,
                separator=input_table_settings.delimiter,
                has_header=input_table_settings.has_headers,
                skip_rows=input_table_settings.starting_from_line,
                encoding="utf8-lossy",
                ignore_errors=True,
                **fallback_infer,
            )
            return df
        except Exception:
            df = pl.scan_csv(
                f,
                low_memory=low_mem,
                separator=input_table_settings.delimiter,
                has_header=input_table_settings.has_headers,
                skip_rows=input_table_settings.starting_from_line,
                encoding="utf8",
                ignore_errors=True,
                **fallback_infer,
            )
            return df
    else:
        df = pl.read_csv(
            f,
            low_memory=low_mem,
            separator=input_table_settings.delimiter,
            has_header=input_table_settings.has_headers,
            skip_rows=input_table_settings.starting_from_line,
            encoding=input_table_settings.encoding,
            ignore_errors=True,
            **fallback_infer,
        )
        return df


def create_from_path_csv(received_table: ReceivedTable) -> pl.DataFrame:
    f = received_table.abs_file_path
    if not isinstance(received_table.table_settings, InputCsvTable):
        raise ValueError("Received table settings are not of type InputCsvTable")
    input_table_settings: InputCsvTable = received_table.table_settings
    low_mem = False if is_url(f) else os.path.getsize(f) / 1024 / 1000 / 1000 > 10
    fallback_infer = {"infer_schema_length": 0} if not input_table_settings.infer_schema else {}
    if input_table_settings.encoding.upper() == "UTF8" or input_table_settings.encoding.upper() == "UTF-8":
        if not input_table_settings.infer_schema:
            return pl.scan_csv(
                f,
                low_memory=low_mem,
                separator=input_table_settings.delimiter,
                has_header=input_table_settings.has_headers,
                skip_rows=input_table_settings.starting_from_line,
                encoding="utf8",
                infer_schema_length=0,
            )
        for infer_len in _infer_schema_ladder(input_table_settings.infer_schema_length):
            try:
                df = pl.scan_csv(
                    f,
                    low_memory=low_mem,
                    try_parse_dates=True,
                    separator=input_table_settings.delimiter,
                    has_header=input_table_settings.has_headers,
                    skip_rows=input_table_settings.starting_from_line,
                    encoding="utf8",
                    infer_schema_length=infer_len,
                )
                df.head(1).collect()
                return df
            except Exception:
                continue
        try:
            df = pl.scan_csv(
                f,
                low_memory=low_mem,
                separator=input_table_settings.delimiter,
                has_header=input_table_settings.has_headers,
                skip_rows=input_table_settings.starting_from_line,
                encoding="utf8-lossy",
                ignore_errors=True,
                **fallback_infer,
            )
            return df
        except Exception:
            df = pl.scan_csv(
                f,
                low_memory=low_mem,
                separator=input_table_settings.delimiter,
                has_header=input_table_settings.has_headers,
                skip_rows=input_table_settings.starting_from_line,
                encoding="utf8",
                ignore_errors=True,
                **fallback_infer,
            )
            return df
    else:
        df = pl.read_csv(
            f,
            low_memory=low_mem,
            separator=input_table_settings.delimiter,
            has_header=input_table_settings.has_headers,
            skip_rows=input_table_settings.starting_from_line,
            encoding=input_table_settings.encoding,
            ignore_errors=True,
            **fallback_infer,
        )
        return df


def create_random(number_of_records: int = 1000) -> pl.LazyFrame:
    return create_fake_data(number_of_records).lazy()


def create_from_path_parquet(received_table: ReceivedTable):
    if not isinstance(received_table.table_settings, InputParquetTable):
        raise ValueError("Received table settings are not of type InputParquetTable")
    f = received_table.abs_file_path
    low_mem = False if is_url(f) else os.path.getsize(f) / 1024 / 1000 / 1000 > 2
    return pl.scan_parquet(source=f, low_memory=low_mem)


def create_from_path_ipc(received_table: ReceivedTable):
    if not isinstance(received_table.table_settings, InputIpcTable):
        raise ValueError("Received table settings are not of type InputIpcTable")
    return pl.scan_ipc(received_table.abs_file_path)


def create_from_path_ndjson(received_table: ReceivedTable):
    if not isinstance(received_table.table_settings, InputNdjsonTable):
        raise ValueError("Received table settings are not of type InputNdjsonTable")
    f = received_table.abs_file_path
    low_mem = False if is_url(f) else os.path.getsize(f) / 1024 / 1000 / 1000 > 10
    return pl.scan_ndjson(f, low_memory=low_mem)


def create_from_path_avro(received_table: ReceivedTable):
    if not isinstance(received_table.table_settings, InputAvroTable):
        raise ValueError("Received table settings are not of type InputAvroTable")
    return pl.read_avro(received_table.abs_file_path)


def create_from_path_ipc_stream(received_table: ReceivedTable):
    if not isinstance(received_table.table_settings, InputIpcStreamTable):
        raise ValueError("Received table settings are not of type InputIpcStreamTable")
    return pl.read_ipc_stream(received_table.abs_file_path)


def create_from_path_excel(received_table: ReceivedTable, logger=None):
    """Read an Excel sheet; see ``shared.excel_reader`` for engine selection and the openpyxl fallback.

    ``generic_task`` injects the node logger, so the engine used lands in the flow log.
    """
    if not isinstance(received_table.table_settings, InputExcelTable):
        raise ValueError("Received table settings are not of type InputExcelTable")
    return read_excel_table(
        received_table.abs_file_path, received_table.table_settings, logger=logger or _module_logger
    )
