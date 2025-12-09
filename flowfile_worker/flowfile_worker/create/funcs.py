import polars as pl
import os
import logging
from typing import Literal

from flowfile_worker.create.models import ReceivedCsvTable, ReceivedParquetTable, ReceivedExcelTable
from flowfile_worker.create.utils import create_fake_data
from flowfile_worker.create.read_excel_tables import df_from_openpyxl, df_from_calamine_xlsx

logger = logging.getLogger(__name__)

CsvEncoding = Literal['utf8', 'utf8-lossy']


def _try_scan_csv_with_fallbacks(
    file_path: str,
    low_memory: bool,
    separator: str,
    has_header: bool,
    skip_rows: int,
    encoding: CsvEncoding,
    infer_schema_length: int | None = None,
    try_parse_dates: bool = True
) -> pl.LazyFrame:
    """Try to scan a CSV file with progressive fallback strategies.

    First attempts strict parsing, then falls back to lossy encoding,
    and finally to error-ignoring mode.

    Args:
        file_path: Path to the CSV file
        low_memory: Whether to use low memory mode
        separator: CSV delimiter character
        has_header: Whether the CSV has headers
        skip_rows: Number of rows to skip
        encoding: Character encoding to use
        infer_schema_length: Number of rows to use for schema inference
        try_parse_dates: Whether to attempt date parsing

    Returns:
        A LazyFrame representing the CSV data
    """
    # Strategy 1: Strict parsing with full schema inference
    try:
        data = pl.scan_csv(
            file_path,
            low_memory=low_memory,
            try_parse_dates=try_parse_dates,
            separator=separator,
            has_header=has_header,
            skip_rows=skip_rows,
            encoding=encoding,
            infer_schema_length=infer_schema_length
        )
        # Validate by reading first row
        data.head(1).collect()
        return data
    except (pl.exceptions.ComputeError, pl.exceptions.SchemaError, UnicodeDecodeError) as e:
        logger.debug(f"Strict CSV parsing failed for {file_path}: {e}")

    # Strategy 2: Try with lossy UTF-8 encoding
    try:
        data = pl.scan_csv(
            file_path,
            low_memory=low_memory,
            separator=separator,
            has_header=has_header,
            skip_rows=skip_rows,
            encoding='utf8-lossy',
            ignore_errors=True
        )
        return data
    except (pl.exceptions.ComputeError, pl.exceptions.SchemaError) as e:
        logger.debug(f"Lossy CSV parsing failed for {file_path}: {e}")

    # Strategy 3: Final fallback with original encoding and error ignoring
    data = pl.scan_csv(
        file_path,
        low_memory=low_memory,
        separator=separator,
        has_header=has_header,
        skip_rows=skip_rows,
        encoding=encoding,
        ignore_errors=True
    )
    return data


def create_from_path_json(received_table: ReceivedCsvTable):
    """Create a LazyFrame from a CSV file (legacy JSON-named function).

    Args:
        received_table: Configuration for reading the CSV file

    Returns:
        A LazyFrame or DataFrame representing the CSV data
    """
    f = received_table.abs_file_path
    gbs_to_load = os.path.getsize(f) / 1024 / 1000 / 1000
    low_mem = gbs_to_load > 10

    if received_table.encoding.upper() in ('UTF8', 'UTF-8'):
        return _try_scan_csv_with_fallbacks(
            file_path=f,
            low_memory=low_mem,
            separator=received_table.delimiter,
            has_header=received_table.has_headers,
            skip_rows=received_table.starting_from_line,
            encoding='utf8',
            infer_schema_length=received_table.infer_schema_length
        )
    else:
        df = pl.read_csv(
            f,
            low_memory=low_mem,
            separator=received_table.delimiter,
            has_header=received_table.has_headers,
            skip_rows=received_table.starting_from_line,
            encoding=received_table.encoding,
            ignore_errors=True
        )
        return df


def create_from_path_csv(received_table: ReceivedCsvTable) -> pl.DataFrame:
    """Create a LazyFrame from a CSV file path.

    Args:
        received_table: Configuration for reading the CSV file

    Returns:
        A LazyFrame representing the CSV data
    """
    f = received_table.abs_file_path
    gbs_to_load = os.path.getsize(f) / 1024 / 1000 / 1000
    low_mem = gbs_to_load > 10

    if received_table.encoding.upper() in ('UTF8', 'UTF-8'):
        return _try_scan_csv_with_fallbacks(
            file_path=f,
            low_memory=low_mem,
            separator=received_table.delimiter,
            has_header=received_table.has_headers,
            skip_rows=received_table.starting_from_line,
            encoding='utf8',
            infer_schema_length=received_table.infer_schema_length
        )
    else:
        df = pl.read_csv(
            f,
            low_memory=low_mem,
            separator=received_table.delimiter,
            has_header=received_table.has_headers,
            skip_rows=received_table.starting_from_line,
            encoding=received_table.encoding,
            ignore_errors=True
        )
        return df


def create_random(number_of_records: int = 1000) -> pl.LazyFrame:
    return create_fake_data(number_of_records).lazy()


def create_from_path_parquet(received_table: ReceivedParquetTable):
    low_mem = (os.path.getsize(received_table.abs_file_path) / 1024 / 1000 / 1000) > 2
    return pl.scan_parquet(source=received_table.abs_file_path, low_memory=low_mem)


def create_from_path_excel(received_table: ReceivedExcelTable):
    if received_table.type_inference:
        engine = 'openpyxl'
    elif received_table.start_row > 0 and received_table.start_column == 0:
        engine = 'calamine' if received_table.has_headers else 'xlsx2csv'
    elif received_table.start_column > 0 or received_table.start_row > 0:
        engine = 'openpyxl'
    else:
        engine = 'calamine'

    sheet_name = received_table.sheet_name

    if engine == 'calamine':
        df = df_from_calamine_xlsx(file_path=received_table.abs_file_path, sheet_name=sheet_name,
                                   start_row=received_table.start_row, end_row=received_table.end_row)
        if received_table.end_column > 0:
            end_col_index = received_table.end_column
            cols_to_select = [df.columns[i] for i in range(received_table.start_column, end_col_index)]
            df = df.select(cols_to_select)

    elif engine == 'xlsx2csv':
        csv_options = {'has_header': received_table.has_headers, 'skip_rows': received_table.start_row}
        df = pl.read_excel(source=received_table.abs_file_path,
                           read_options=csv_options,
                           engine='xlsx2csv',
                           sheet_name=received_table.sheet_name)
        end_col_index = received_table.end_column if received_table.end_column > 0 else len(df.columns)
        cols_to_select = [df.columns[i] for i in range(received_table.start_column, end_col_index)]
        df = df.select(cols_to_select)
        if 0 < received_table.end_row < len(df):
            df = df.head(received_table.end_row)

    else:
        max_col = received_table.end_column if received_table.end_column > 0 else None
        max_row = received_table.end_row + 1 if received_table.end_row > 0 else None
        df = df_from_openpyxl(file_path=received_table.abs_file_path,
                              sheet_name=received_table.sheet_name,
                              min_row=received_table.start_row + 1,
                              min_col=received_table.start_column + 1,
                              max_row=max_row,
                              max_col=max_col, has_headers=received_table.has_headers)
    return df
