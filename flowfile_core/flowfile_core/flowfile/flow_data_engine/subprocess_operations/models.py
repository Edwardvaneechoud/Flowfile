from base64 import b64decode, b64encode
from typing import Annotated, Any, Literal

from pl_fuzzy_frame_match.models import FuzzyMapping
from pydantic import BaseModel, BeforeValidator, PlainSerializer

OperationType = Literal["store", "calculate_schema", "calculate_number_of_records", "write_output", "store_sample"]


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


class PolarsOperation(BaseModel):
    operation: Base64Bytes  # Automatically encodes/decodes base64 for JSON


class PolarsScript(PolarsOperation):
    task_id: str | None = None
    cache_dir: str | None = None
    operation_type: OperationType


class FuzzyJoinInput(BaseModel):
    task_id: str | None = None
    cache_dir: str | None = None
    left_df_operation: PolarsOperation
    right_df_operation: PolarsOperation
    fuzzy_maps: list[FuzzyMapping]
    flowfile_node_id: int | str
    flowfile_flow_id: int


class Status(BaseModel):
    background_task_id: str
    status: Literal[
        "Processing", "Completed", "Error", "Unknown Error", "Starting", "Cancelled"
    ]  # Type alias for status
    file_ref: str
    progress: int = 0
    error_message: str | None = None  # Add error_message field
    results: Any
    result_type: Literal["polars", "other"] = "polars"
