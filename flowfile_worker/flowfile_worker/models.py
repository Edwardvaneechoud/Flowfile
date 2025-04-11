from pydantic import BaseModel
from typing import Optional, Literal, Any
from base64 import decodebytes
from flowfile_worker.polars_fuzzy_match.models import FuzzyMapping
from flowfile_worker.external_sources.sql_source.models import DatabaseWriteSettings


OperationType = Literal[
    'store', 'calculate_schema', 'calculate_number_of_records', 'write_output', 'fuzzy', 'store_sample',
    'write_to_database']
ResultType = Literal['polars', 'other']


class PolarsOperation(BaseModel):
    operation: bytes
    flowfile_flow_id: Optional[int] = 1
    flowfile_node_id: Optional[int | str] = -1
    def polars_serializable_object(self):
        return decodebytes(self.operation)


class PolarsScript(PolarsOperation):
    task_id: Optional[str] = None
    cache_dir: Optional[str] = None
    operation_type: OperationType


class PolarsScriptSample(PolarsScript):
    sample_size: Optional[int] = 100


class PolarsScriptWrite(BaseModel):
    operation: bytes
    data_type: str
    path: str
    write_mode: str
    sheet_name: Optional[str] = None
    delimiter: Optional[str] = None
    flowfile_flow_id: Optional[int] = -1
    flowfile_node_id: Optional[int | str] = -1

    def polars_serializable_object(self):
        return decodebytes(self.operation)


class DatabaseScriptWrite(DatabaseWriteSettings):
    operation: bytes

    def polars_serializable_object(self):
        return decodebytes(self.operation)

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
            flowfile_node_id=self.flowfile_node_id
        )


class FuzzyJoinInput(BaseModel):
    task_id: Optional[str] = None
    cache_dir: Optional[str] = None
    left_df_operation: PolarsOperation
    right_df_operation: PolarsOperation
    fuzzy_maps: list[FuzzyMapping]
    flowfile_flow_id: Optional[int] = 1
    flowfile_node_id: Optional[int | str] = -1


class Status(BaseModel):
    background_task_id: str
    status: Literal['Processing', 'Completed', 'Error', 'Unknown Error', 'Starting']  # Type alias for status
    file_ref: str
    progress: Optional[int] = 0
    error_message: Optional[str] = None  # Add error_message field
    results: Optional[Any] = None
    result_type: Optional[ResultType] = 'polars'

    def __hash__(self):
        return hash(self.file_ref)


class RawLogInput(BaseModel):
    flowfile_flow_id: int
    log_message: str
    log_type: Literal["INFO", "ERROR"]
    extra: Optional[dict] = None
