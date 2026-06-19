from typing import Literal

from flowfile_worker.create.funcs import (
    create_from_path_avro,
    create_from_path_csv,
    create_from_path_excel,
    create_from_path_ipc,
    create_from_path_json,
    create_from_path_ndjson,
    create_from_path_parquet,
)

FileType = Literal["csv", "parquet", "json", "excel", "ipc", "ndjson", "avro"]


def table_creator_factory_method(file_type: FileType) -> callable:
    match file_type:
        case "csv":
            return create_from_path_csv
        case "parquet":
            return create_from_path_parquet
        case "excel":
            return create_from_path_excel
        case "json":
            return create_from_path_json
        case "ipc":
            return create_from_path_ipc
        case "ndjson":
            return create_from_path_ndjson
        case "avro":
            return create_from_path_avro
        case _:
            raise ValueError(f"Unsupported file type: {file_type}")
