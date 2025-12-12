import polars as pl
import os
from flowfile_core.schemas import input_schema
from flowfile_core.flowfile.flow_data_engine.sample_data import create_fake_data
from flowfile_core.flowfile.flow_data_engine.read_excel_tables import df_from_openpyxl, df_from_calamine_xlsx
from polars._typing import CsvEncoding


def create_from_json(received_table: input_schema.ReceivedTable):
    f = received_table.abs_file_path
    gbs_to_load = os.path.getsize(f) / 1024 / 1000 / 1000
    low_mem = gbs_to_load > 10

    if not isinstance(received_table.table_settings, input_schema.InputJsonTable):
        raise ValueError("Received table settings are not of type InputJsonTable")
    table_settings: input_schema.InputJsonTable = received_table.table_settings

    if table_settings.encoding.upper() == 'UTF8' or table_settings.encoding.upper() == 'UTF-8':
        try:
            data = pl.scan_csv(f,
                               low_memory=low_mem,
                               try_parse_dates=True,
                               separator=table_settings.delimiter,
                               has_header=table_settings.has_headers,
                               skip_rows=table_settings.starting_from_line,
                               encoding='utf8',
                               infer_schema_length=table_settings.infer_schema_length)
            data.head(1).collect()
            return data
        except:
            try:
                data = pl.scan_csv(f, low_memory=low_mem,
                                   separator=table_settings.delimiter,
                                   has_header=table_settings.has_headers,
                                   skip_rows=table_settings.starting_from_line,
                                   encoding='utf8-lossy',
                                   ignore_errors=True)
                return data
            except:
                data = pl.scan_csv(f, low_memory=low_mem,
                                   separator=table_settings.delimiter,
                                   has_header=table_settings.has_headers,
                                   skip_rows=table_settings.starting_from_line,
                                   encoding='utf8',
                                   ignore_errors=True)
                return data
    else:
        data = pl.read_csv(f, low_memory=low_mem,
                           separator=table_settings.delimiter,
                           has_header=table_settings.has_headers,
                           skip_rows=table_settings.starting_from_line,
                           encoding=table_settings.encoding,
                           ignore_errors=True)
        return data


def standardize_utf8_encoding(non_standardized_encoding: str) -> CsvEncoding:
    if non_standardized_encoding.upper() in ('UTF-8', 'UTF8'):
        return 'utf8'
    elif non_standardized_encoding.upper() in ('UTF-8-LOSSY', 'UTF8-LOSSY'):
        return 'utf8-lossy'
    else:
        raise ValueError(f"Encoding {non_standardized_encoding} is not supported.")


def create_from_path_csv(received_table: input_schema.ReceivedTable) -> pl.LazyFrame:
    if not isinstance(received_table.table_settings, input_schema.InputCsvTable):
        raise ValueError("Received table settings are not of type InputCsvTable")

    table_settings: input_schema.InputCsvTable = received_table.table_settings

    f = received_table.abs_file_path
    gbs_to_load = os.path.getsize(f) / 1024 / 1000 / 1000
    low_mem = gbs_to_load > 10

    if table_settings.encoding.upper() in ("UTF-8", "UTF8", 'UTF8-LOSSY', 'UTF-8-LOSSY'):
        encoding: CsvEncoding = standardize_utf8_encoding(table_settings.encoding)
        try:
            data = pl.scan_csv(f,
                               low_memory=low_mem,
                               try_parse_dates=True,
                               separator=table_settings.delimiter,
                               has_header=table_settings.has_headers,
                               skip_rows=table_settings.starting_from_line,
                               encoding=encoding,
                               infer_schema_length=table_settings.infer_schema_length)
            data.head(1).collect()
            return data
        except:

            try:
                data = pl.scan_csv(f, low_memory=low_mem,
                                   separator=table_settings.delimiter,
                                   has_header=table_settings.has_headers,
                                   skip_rows=table_settings.starting_from_line,
                                   encoding='utf8-lossy',
                                   ignore_errors=True)
                return data
            except:
                data = pl.scan_csv(f, low_memory=False,
                                   separator=table_settings.delimiter,
                                   has_header=table_settings.has_headers,
                                   skip_rows=table_settings.starting_from_line,
                                   encoding=encoding,
                                   ignore_errors=True)
                return data
    else:
        data = pl.read_csv_batched(f,
                                   low_memory=low_mem,
                                   separator=table_settings.delimiter,
                                   has_header=table_settings.has_headers,
                                   skip_rows=table_settings.starting_from_line,
                                   encoding=table_settings.encoding,
                                   ignore_errors=True, batch_size=2).next_batches(1)
        return data[0].lazy()


def create_random(number_of_records: int = 1000) -> pl.LazyFrame:
    return create_fake_data(number_of_records).lazy()


def create_from_path_parquet(received_table: input_schema.ReceivedTable) -> pl.LazyFrame:
    if not isinstance(received_table.table_settings, input_schema.InputParquetTable):
        raise ValueError("Received table settings are not of type InputParquetTable")
    low_mem = (os.path.getsize(received_table.abs_file_path) / 1024 / 1000 / 1000) > 2
    return pl.scan_parquet(source=received_table.abs_file_path, low_memory=low_mem)


def create_from_path_excel(received_table: input_schema.ReceivedTable):
    if not isinstance(received_table.table_settings, input_schema.InputExcelTable):
        raise ValueError("Received table settings are not of type InputExcelTable")

    table_settings: input_schema.InputExcelTable = received_table.table_settings
    if table_settings.type_inference:
        engine = 'openpyxl'
    elif table_settings.start_row > 0 and table_settings.start_column == 0:
        engine = 'calamine' if table_settings.has_headers else 'xlsx2csv'
    elif table_settings.start_column > 0 or table_settings.start_row > 0:
        engine = 'openpyxl'
    else:
        engine = 'calamine'

    sheet_name = table_settings.sheet_name

    if engine == 'calamine':
        df = df_from_calamine_xlsx(file_path=received_table.abs_file_path, sheet_name=sheet_name,
                                   start_row=table_settings.start_row, end_row=table_settings.end_row)
        if table_settings.end_column > 0:
            end_col_index = table_settings.end_column
            cols_to_select = [df.columns[i] for i in range(table_settings.start_column, end_col_index)]
            df = df.select(cols_to_select)

    elif engine == 'xlsx2csv':
        csv_options = {'has_header': table_settings.has_headers, 'skip_rows': table_settings.start_row}
        df = pl.read_excel(source=received_table.abs_file_path,
                           read_options=csv_options,
                           engine='xlsx2csv',
                           sheet_name=table_settings.sheet_name)
        end_col_index = table_settings.end_column if table_settings.end_column > 0 else len(df.columns)
        cols_to_select = [df.columns[i] for i in range(table_settings.start_column, end_col_index)]
        df = df.select(cols_to_select)
        if 0 < table_settings.end_row < len(df):
            df = df.head(table_settings.end_row)

    else:
        max_col = table_settings.end_column if table_settings.end_column > 0 else None
        max_row = table_settings.end_row + 1 if table_settings.end_row > 0 else None
        df = df_from_openpyxl(file_path=received_table.abs_file_path,
                              sheet_name=table_settings.sheet_name,
                              min_row=table_settings.start_row + 1,
                              min_col=table_settings.start_column + 1,
                              max_row=max_row,
                              max_col=max_col, has_headers=table_settings.has_headers)
    return df
