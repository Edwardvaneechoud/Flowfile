from typing import Any, Optional, Literal
from pydantic import BaseModel
from flowfile_core.schemas.transform_schema import FuzzyMap

OperationType = Literal['store', 'calculate_schema', 'calculate_number_of_records', 'write_output', 'store_sample']


class PolarsOperation(BaseModel):
    operation: bytes


class PolarsScript(PolarsOperation):
    task_id: Optional[str] = None
    cache_dir: Optional[str] = None
    operation_type: OperationType


class FuzzyJoinInput(BaseModel):
    task_id: Optional[str] = None
    cache_dir: Optional[str] = None
    left_df_operation: PolarsOperation
    right_df_operation: PolarsOperation
    fuzzy_maps: list[FuzzyMap]
    flowfile_node_id: int|str
    flowfile_flow_id: int


class Status(BaseModel):
    background_task_id: str
    status: Literal['Processing', 'Completed', 'Error', 'Unknown Error', 'Starting', 'Cancelled']  # Type alias for status
    file_ref: str
    progress: int = 0
    error_message: Optional[str] = None  # Add error_message field
    results: Any
    result_type: Literal['polars', 'other'] = 'polars'

