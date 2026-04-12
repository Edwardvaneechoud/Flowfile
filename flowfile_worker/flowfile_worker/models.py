from base64 import b64decode, b64encode
from typing import Annotated, Any, Literal

from pl_fuzzy_frame_match import FuzzyMapping
from pydantic import BaseModel, BeforeValidator, Field, PlainSerializer

from flowfile_worker.external_sources.s3_source.models import CloudStorageWriteSettings
from flowfile_worker.external_sources.sql_source.models import DatabaseWriteSettings
from shared.delta_models import DeltaVersionCommit as DeltaVersionCommit  # noqa: F401


# Custom type for bytes that serializes to/from base64 string in JSON
def _decode_bytes(v: Any) -> bytes:
    if isinstance(v, bytes):
        return v
    if isinstance(v, str):
        return b64decode(v)
    raise ValueError(f"Expected bytes or base64 string, got {type(v)}")


Base64Bytes = Annotated[
    bytes,
    BeforeValidator(_decode_bytes),
    PlainSerializer(lambda x: b64encode(x).decode("ascii"), return_type=str),
]

OperationType = Literal[
    "store",
    "calculate_schema",
    "calculate_number_of_records",
    "write_output",
    "fuzzy",
    "store_sample",
    "write_to_database",
    "write_to_cloud_storage",
    "write_parquet",
    "write_delta",
    "merge_delta",
]
ResultType = Literal["polars", "other"]


class PolarsOperation(BaseModel):
    operation: Base64Bytes  # Automatically encodes/decodes base64 for JSON
    flowfile_flow_id: int | None = 1
    flowfile_node_id: int | str | None = -1

    def polars_serializable_object(self):
        # Operation is raw bytes (auto-decoded from base64 if received as JSON)
        return self.operation


class PolarsScript(PolarsOperation):
    task_id: str | None = None
    cache_dir: str | None = None
    operation_type: OperationType


class PolarsScriptSample(PolarsScript):
    sample_size: int | None = 100


class PolarsScriptWrite(BaseModel):
    operation: Base64Bytes  # Automatically encodes/decodes base64 for JSON
    data_type: str
    path: str
    write_mode: str
    sheet_name: str | None = None
    delimiter: str | None = None
    flowfile_flow_id: int | None = -1
    flowfile_node_id: int | str | None = -1

    def polars_serializable_object(self):
        # Operation is raw bytes (auto-decoded from base64 if received as JSON)
        return self.operation


class DatabaseScriptWrite(DatabaseWriteSettings):
    operation: Base64Bytes  # Automatically encodes/decodes base64 for JSON

    def polars_serializable_object(self):
        # Operation is raw bytes (auto-decoded from base64 if received as JSON)
        return self.operation

    def get_database_write_settings(self) -> DatabaseWriteSettings:
        """
        Converts the current instance to a DatabaseWriteSettings object.
        Returns:
            DatabaseWriteSettings: The corresponding DatabaseWriteSettings object.
        """
        return DatabaseWriteSettings(
            connection=self.connection,
            table_name=self.table_name,
            if_exists=self.if_exists,
            flowfile_flow_id=self.flowfile_flow_id,
            flowfile_node_id=self.flowfile_node_id,
        )


class CloudStorageScriptWrite(CloudStorageWriteSettings):
    operation: Base64Bytes  # Automatically encodes/decodes base64 for JSON

    def polars_serializable_object(self):
        # Operation is raw bytes (auto-decoded from base64 if received as JSON)
        return self.operation

    def get_cloud_storage_write_settings(self) -> CloudStorageWriteSettings:
        """
        Converts the current instance to a DatabaseWriteSettings object.
        Returns:
            DatabaseWriteSettings: The corresponding DatabaseWriteSettings object.
        """
        return CloudStorageWriteSettings(
            write_settings=self.write_settings,
            connection=self.connection,
            flowfile_flow_id=self.flowfile_flow_id,
            flowfile_node_id=self.flowfile_node_id,
        )


class FuzzyJoinInput(BaseModel):
    task_id: str | None = None
    cache_dir: str | None = None
    left_df_operation: PolarsOperation
    right_df_operation: PolarsOperation
    fuzzy_maps: list[FuzzyMapping]
    flowfile_flow_id: int | None = 1
    flowfile_node_id: int | str | None = -1


class Status(BaseModel):
    background_task_id: str
    status: Literal["Processing", "Completed", "Error", "Unknown Error", "Starting"]  # Type alias for status
    file_ref: str
    progress: int | None = 0
    error_message: str | None = None  # Add error_message field
    results: Any | None = None
    result_type: ResultType | None = "polars"

    def __hash__(self):
        return hash(self.file_ref)


class RawLogInput(BaseModel):
    flowfile_flow_id: int
    log_message: str
    log_type: Literal["INFO", "WARNING", "ERROR"]
    node_id: int | None = None
    extra: dict | None = None


class ColumnSchema(BaseModel):
    name: str
    dtype: str


class CatalogMaterializeRequest(BaseModel):
    source_file_path: str
    table_name: str | None = None


class CatalogMaterializeResponse(BaseModel):
    parquet_path: str | None = None  # Legacy field for backward compat
    table_path: str
    storage_format: str = "delta"  # "delta" or "parquet"
    schema: list[ColumnSchema]
    row_count: int
    column_count: int
    size_bytes: int


class TableMetadataRequest(BaseModel):
    table_path: str  # Bare table directory/file name (no path separators)
    storage_format: str = "delta"  # "delta" or "parquet"


class TableMetadataResponse(BaseModel):
    schema: list[ColumnSchema]
    row_count: int
    column_count: int
    size_bytes: int


class DeltaHistoryRequest(BaseModel):
    table_path: str  # Bare table directory name (no path separators)
    limit: int | None = None


class DeltaHistoryResponse(BaseModel):
    current_version: int
    history: list[DeltaVersionCommit]


class DeltaVersionPreviewRequest(BaseModel):
    table_path: str  # Bare table directory name (no path separators)
    version: int
    n_rows: int = 100


class DeltaVersionPreviewResponse(BaseModel):
    version: int
    columns: list[str]
    dtypes: list[str]
    rows: list[list]
    total_rows: int


class SqlQueryRequest(BaseModel):
    query: str
    tables: dict[str, str]  # mapping of logical table name -> directory name
    max_rows: int = 10_000


class SqlQueryResponse(BaseModel):
    columns: list[str] = Field(default_factory=list)
    dtypes: list[str] = Field(default_factory=list)
    rows: list[list] = Field(default_factory=list)
    total_rows: int = 0
    truncated: bool = False
    execution_time_ms: float = 0.0
    used_tables: list[str] = Field(default_factory=list)
    error: str | None = None
