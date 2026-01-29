from base64 import b64decode, b64encode
from typing import Annotated, Any, Literal

from pl_fuzzy_frame_match import FuzzyMapping
from pydantic import BaseModel, BeforeValidator, PlainSerializer

from flowfile_worker.external_sources.s3_source.models import CloudStorageWriteSettings
from flowfile_worker.external_sources.sql_source.models import DatabaseWriteSettings


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
    PlainSerializer(lambda x: b64encode(x).decode('ascii'), return_type=str),
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
    log_type: Literal["INFO", "ERROR"]
    extra: dict | None = None
