from base64 import b64decode, b64encode
from typing import Annotated, Any, Literal

from pl_fuzzy_frame_match.models import FuzzyMapping
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, PlainSerializer

OperationType = Literal[
    "store",
    "calculate_schema",
    "calculate_number_of_records",
    "write_output",
    "store_sample",
    "write_parquet",
    "write_delta",
    "merge_delta",
]


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


class CustomNodeExecuteInput(BaseModel):
    """Outgoing payload for ``POST /execute_custom_node`` on the worker.

    Mirrors flowfile_worker.models.CustomNodeExecuteInput — keep in sync.
    """

    task_id: str | None = None
    cache_dir: str | None = None
    node_source: str
    class_name: str | None = None
    settings_values: dict[str, dict[str, Any]] = Field(default_factory=dict)
    secrets: dict[str, str] = Field(default_factory=dict)
    inputs: list[Base64Bytes] = Field(default_factory=list)
    output_names: list[str] = Field(default_factory=lambda: ["main"])
    dry_run: bool = False
    row_limit: int | None = None
    user_id: int | None = None
    flowfile_flow_id: int | None = 1
    flowfile_node_id: int | str | None = -1


def custom_node_task_id(node_hash: str) -> str:
    """Distinct from the bare node-hash id used by the later result-store task,
    so neither the worker status entry nor the ``<hash>.arrow`` file is overwritten."""
    return f"{node_hash}-custom-node"


class TrainModelInput(BaseModel):
    """Outgoing payload for ``POST /train_ml_model`` on the worker."""

    model_config = ConfigDict(protected_namespaces=())

    task_id: str | None = None
    cache_dir: str | None = None
    df_operation: PolarsOperation
    model_type: str
    target_column: str
    feature_columns: list[str]
    params: dict[str, Any] = Field(default_factory=dict)
    staging_path: str
    flowfile_node_id: int | str
    flowfile_flow_id: int


class ApplyModelInput(BaseModel):
    """Outgoing payload for ``POST /apply_ml_model`` on the worker."""

    model_config = ConfigDict(protected_namespaces=())

    task_id: str | None = None
    cache_dir: str | None = None
    df_operation: PolarsOperation
    model_path: str
    output_column: str = "prediction"
    flowfile_node_id: int | str
    flowfile_flow_id: int


class Status(BaseModel):
    background_task_id: str
    status: Literal[
        "Processing", "Completed", "Error", "Unknown Error", "Starting", "Cancelled"
    ]
    file_ref: str
    progress: int = 0
    error_message: str | None = None
    results: Any
    result_type: Literal["polars", "other"] = "polars"
    number_of_records: int | None = None
