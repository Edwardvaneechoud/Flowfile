from pydantic import BaseModel
from typing import Optional, Literal, Any
from base64 import decodebytes
from flowfile_worker.polars_fuzzy_match.models import FuzzyMapping


OperationType = Literal[
    'store', 'calculate_schema', 'calculate_number_of_records', 'write_output', 'fuzzy', 'store_sample']
ResultType = Literal['polars', 'other']


class PolarsOperation(BaseModel):
    operation: bytes

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

    def polars_serializable_object(self):
        return decodebytes(self.operation)


class FuzzyJoinInput(BaseModel):
    task_id: Optional[str] = None
    cache_dir: Optional[str] = None
    left_df_operation: PolarsOperation
    right_df_operation: PolarsOperation
    fuzzy_maps: list[FuzzyMapping]


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

