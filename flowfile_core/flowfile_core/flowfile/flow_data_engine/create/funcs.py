import polars as pl
import os

from flowfile_core.schemas import input_schema
from flowfile_core.flowfile.flow_data_engine.sample_data import create_fake_data
from flowfile_core.flowfile.flow_data_engine.read_excel_tables import df_from_openpyxl, df_from_calamine_xlsx


def create_from_json(received_table: input_schema.ReceivedCsvTable):
    f = received_table.abs_file_path
    gbs_to_load = os.path.getsize(f) / 1024 / 1000 / 1000
    low_mem = gbs_to_load > 10
    if received_table.encoding.upper() == 'UTF8' or received_table.encoding.upper() == 'UTF-8':
        try:
            data = pl.scan_csv(f,
                               low_memory=low_mem,
                               try_parse_dates=True,
                               separator=received_table.delimiter,
                               has_header=received_table.has_headers,
                               skip_rows=received_table.starting_from_line,
                               encoding='utf8',
                               infer_schema_length=received_table.infer_schema_length)
            data.head(1).collect()
            return data
        except:
            try:
                data = pl.scan_csv(f, low_memory=low_mem,
                                   separator=received_table.delimiter,
                                   has_header=received_table.has_headers,
                                   skip_rows=received_table.starting_from_line,
                                   encoding='utf8-lossy',
                                   ignore_errors=True)
                return data
            except:
                data = pl.scan_csv(f, low_memory=low_mem,
                                   separator=received_table.delimiter,
                                   has_header=received_table.has_headers,
                                   skip_rows=received_table.starting_from_line,
                                   encoding='utf8',
                                   ignore_errors=True)
                return data
    else:
        data = pl.read_csv(f, low_memory=low_mem,
                           separator=received_table.delimiter,
                           has_header=received_table.has_headers,
                           skip_rows=received_table.starting_from_line,
                           encoding=received_table.encoding,
                           ignore_errors=True)
        return data


def create_from_path_csv(received_table: input_schema.ReceivedCsvTable) -> pl.DataFrame:
    f = received_table.abs_file_path
    gbs_to_load = os.path.getsize(f) / 1024 / 1000 / 1000
    low_mem = gbs_to_load > 10
    if received_table.encoding.upper() == 'UTF8' or received_table.encoding.upper() == 'UTF-8':
        try:
            data = pl.scan_csv(f,
                               low_memory=low_mem,
                               try_parse_dates=True,
                               separator=received_table.delimiter,
                               has_header=received_table.has_headers,
                               skip_rows=received_table.starting_from_line,
                               encoding='utf8',
                               infer_schema_length=received_table.infer_schema_length)
            data.head(1).collect()
            return data
        except:
            try:
                data = pl.scan_csv(f, low_memory=low_mem,
                                   separator=received_table.delimiter,
                                   has_header=received_table.has_headers,
                                   skip_rows=received_table.starting_from_line,
                                   encoding='utf8-lossy',
                                   ignore_errors=True)
                return data
            except:
                data = pl.scan_csv(f, low_memory=low_mem,
                                   separator=received_table.delimiter,
                                   has_header=received_table.has_headers,
                                   skip_rows=received_table.starting_from_line,
                                   encoding='utf8',
                                   ignore_errors=True)
                return data
    else:
        data = pl.read_csv_batched(f,
                                   low_memory=low_mem,
                                   separator=received_table.delimiter,
                                   has_header=received_table.has_headers,
                                   skip_rows=received_table.starting_from_line,
                                   encoding=received_table.encoding,
                                   ignore_errors=True, batch_size=2).next_batches(1)
        return data[0]


def create_random(number_of_records: int = 1000) -> pl.LazyFrame:
    return create_fake_data(number_of_records).lazy()


def create_from_path_parquet(received_table: input_schema.ReceivedParquetTable):
    low_mem = (os.path.getsize(received_table.abs_file_path) / 1024 / 1000 / 1000) > 2
    return pl.scan_parquet(source=received_table.abs_file_path, low_memory=low_mem)


def create_from_path_excel(received_table: input_schema.ReceivedExcelTable):
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
