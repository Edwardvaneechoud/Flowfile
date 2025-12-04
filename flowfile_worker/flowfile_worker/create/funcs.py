import polars as pl
import os

from flowfile_worker.create.models import ReceivedTable, InputCsvTable, InputJsonTable, InputExcelTable, InputParquetTable
from flowfile_worker.create.utils import create_fake_data
from flowfile_worker.create.read_excel_tables import df_from_openpyxl, df_from_calamine_xlsx


def create_from_path_json(received_table: ReceivedTable):
    if not isinstance(received_table.table_settings, InputJsonTable):
        raise ValueError("Received table settings are not of type InputJsonTable")
    input_table_settings: InputJsonTable = received_table.table_settings
    f = received_table.abs_file_path
    gbs_to_load = os.path.getsize(f) / 1024 / 1000 / 1000
    low_mem = gbs_to_load > 10
    if input_table_settings.encoding.upper() == 'UTF8' or input_table_settings.encoding.upper() == 'UTF-8':
        try:
            df = pl.scan_csv(f,
                               low_memory=low_mem,
                               try_parse_dates=True,
                               separator=input_table_settings.delimiter,
                               has_header=input_table_settings.has_headers,
                               skip_rows=input_table_settings.starting_from_line,
                               encoding='utf8',
                               infer_schema_length=input_table_settings.infer_schema_length)
            df.head(1).collect()
            return df
        except:
            try:
                df = pl.scan_csv(f, low_memory=low_mem,
                                   separator=input_table_settings.delimiter,
                                   has_header=input_table_settings.has_headers,
                                   skip_rows=input_table_settings.starting_from_line,
                                   encoding='utf8-lossy',
                                   ignore_errors=True)
                return df
            except:
                df = pl.scan_csv(f, low_memory=low_mem,
                                   separator=input_table_settings.delimiter,
                                   has_header=input_table_settings.has_headers,
                                   skip_rows=input_table_settings.starting_from_line,
                                   encoding='utf8',
                                   ignore_errors=True)
                return df
    else:
        df = pl.read_csv(f, low_memory=low_mem,
                           separator=input_table_settings.delimiter,
                           has_header=input_table_settings.has_headers,
                           skip_rows=input_table_settings.starting_from_line,
                           encoding=input_table_settings.encoding,
                           ignore_errors=True)
        return df


def create_from_path_csv(received_table: ReceivedTable) -> pl.DataFrame:
    f = received_table.abs_file_path
    if not isinstance(received_table.table_settings, InputCsvTable):
        raise ValueError("Received table settings are not of type InputCsvTable")
    input_table_settings: InputCsvTable = received_table.table_settings
    gbs_to_load = os.path.getsize(f) / 1024 / 1000 / 1000
    low_mem = gbs_to_load > 10
    if input_table_settings.encoding.upper() == 'UTF8' or input_table_settings.encoding.upper() == 'UTF-8':
        try:
            df = pl.scan_csv(f,
                               low_memory=low_mem,
                               try_parse_dates=True,
                               separator=input_table_settings.delimiter,
                               has_header=input_table_settings.has_headers,
                               skip_rows=input_table_settings.starting_from_line,
                               encoding='utf8',
                               infer_schema_length=input_table_settings.infer_schema_length)
            df.head(1).collect()
            return df
        except:
            try:
                df = pl.scan_csv(f, low_memory=low_mem,
                                   separator=input_table_settings.delimiter,
                                   has_header=input_table_settings.has_headers,
                                   skip_rows=input_table_settings.starting_from_line,
                                   encoding='utf8-lossy',
                                   ignore_errors=True)
                return df
            except:
                df = pl.scan_csv(f, low_memory=low_mem,
                                   separator=input_table_settings.delimiter,
                                   has_header=input_table_settings.has_headers,
                                   skip_rows=input_table_settings.starting_from_line,
                                   encoding='utf8',
                                   ignore_errors=True)
                return df
    else:
        df = pl.read_csv(f,
                           low_memory=low_mem,
                           separator=input_table_settings.delimiter,
                           has_header=input_table_settings.has_headers,
                           skip_rows=input_table_settings.starting_from_line,
                           encoding=input_table_settings.encoding,
                           ignore_errors=True)
        return df


def create_random(number_of_records: int = 1000) -> pl.LazyFrame:
    return create_fake_data(number_of_records).lazy()


def create_from_path_parquet(received_table: ReceivedTable):
    if not isinstance(received_table.table_settings, InputParquetTable):
        raise ValueError("Received table settings are not of type InputParquetTable")
    low_mem = (os.path.getsize(received_table.abs_file_path) / 1024 / 1000 / 1000) > 2
    return pl.scan_parquet(source=received_table.abs_file_path, low_memory=low_mem)


def create_from_path_excel(received_table: ReceivedTable):
    if not isinstance(received_table.table_settings, InputExcelTable):
        raise ValueError("Received table settings are not of type InputExcelTable")
    input_table_settings: InputExcelTable = received_table.table_settings

    if input_table_settings.type_inference:
        engine = 'openpyxl'
    elif input_table_settings.start_row > 0 and input_table_settings.start_column == 0:
        engine = 'calamine' if input_table_settings.has_headers else 'xlsx2csv'
    elif input_table_settings.start_column > 0 or input_table_settings.start_row > 0:
        engine = 'openpyxl'
    else:
        engine = 'calamine'

    sheet_name = input_table_settings.sheet_name

    if engine == 'calamine':
        df = df_from_calamine_xlsx(file_path=received_table.abs_file_path, sheet_name=sheet_name,
                                   start_row=input_table_settings.start_row, end_row=input_table_settings.end_row)
        if input_table_settings.end_column > 0:
            end_col_index = input_table_settings.end_column
            cols_to_select = [df.columns[i] for i in range(input_table_settings.start_column, end_col_index)]
            df = df.select(cols_to_select)

    elif engine == 'xlsx2csv':
        csv_options = {'has_header': input_table_settings.has_headers, 'skip_rows': input_table_settings.start_row}
        df = pl.read_excel(source=received_table.abs_file_path,
                           read_options=csv_options,
                           engine='xlsx2csv',
                           sheet_name=input_table_settings.sheet_name)
        end_col_index = input_table_settings.end_column if input_table_settings.end_column > 0 else len(df.columns)
        cols_to_select = [df.columns[i] for i in range(input_table_settings.start_column, end_col_index)]
        df = df.select(cols_to_select)
        if 0 < input_table_settings.end_row < len(df):
            df = df.head(input_table_settings.end_row)

    else:
        max_col = input_table_settings.end_column if input_table_settings.end_column > 0 else None
        max_row = input_table_settings.end_row + 1 if input_table_settings.end_row > 0 else None
        df = df_from_openpyxl(file_path=received_table.abs_file_path,
                              sheet_name=input_table_settings.sheet_name,
                              min_row=input_table_settings.start_row + 1,
                              min_col=input_table_settings.start_column + 1,
                              max_row=max_row,
                              max_col=max_col, has_headers=input_table_settings.has_headers)
    return df
