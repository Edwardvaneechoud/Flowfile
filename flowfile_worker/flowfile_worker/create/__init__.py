from flowfile_worker.create.models import (ReceivedCsvTable, ReceivedParquetTable, ReceivedExcelTable,
                                           ReceivedJsonTable)
from flowfile_worker.create.funcs import (create_from_path_csv, create_from_path_parquet, create_from_path_excel,
                                          create_from_path_json)
from typing import Dict, Literal

ReceivedTableCollection = ReceivedCsvTable | ReceivedParquetTable | ReceivedJsonTable | ReceivedExcelTable
FileType = Literal['csv', 'parquet', 'json', 'excel']


def received_table_parser(received_table_raw: Dict, file_type: FileType) -> ReceivedTableCollection:
    match file_type:
        case 'csv':
            received_table = ReceivedCsvTable.model_validate(received_table_raw)
        case 'parquet':
            received_table = ReceivedParquetTable.model_validate(received_table_raw)
        case 'excel':
            received_table = ReceivedExcelTable.model_validate(received_table_raw)
        case 'json':
            return ReceivedJsonTable.model_validate(received_table_raw)
        case _:
            raise ValueError(f'Unsupported file type: {file_type}')
    return received_table


def table_creator_factory_method(file_type: Literal['csv', 'parquet', 'json', 'excel']) -> callable:
    match file_type:
        case 'csv':
            return create_from_path_csv
        case 'parquet':
            return create_from_path_parquet
        case 'excel':
            return create_from_path_excel
        case 'json':
            return create_from_path_json
        case _:
            raise ValueError(f'Unsupported file type: {file_type}')
