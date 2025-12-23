
from flowfile_worker.create.funcs import (create_from_path_csv, create_from_path_parquet, create_from_path_excel,
                                          create_from_path_json)
from typing import Literal

FileType = Literal['csv', 'parquet', 'json', 'excel']

def table_creator_factory_method(file_type: FileType) -> callable:
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
