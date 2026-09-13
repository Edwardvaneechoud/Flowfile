import time
from datetime import date, datetime, time as time_of_day, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any, Literal
from uuid import UUID

from pydantic import AliasChoices, BaseModel, Field, field_validator

from flowfile_core.flowfile.flow_data_engine.flow_file_column.interface import ReadableDataTypeGroup


class NodeResult(BaseModel):
    """Represents the execution result of a single node in a FlowGraph run."""

    node_id: int
    node_name: str | None = None
    description: str = ""
    start_timestamp: float = Field(default_factory=time.time)
    end_timestamp: float = 0
    success: bool | None = None
    # True when the node was deliberately skipped (e.g. a closed gate upstream).
    # Deliberate skips carry success=True so a gated run stays green; nodes
    # skipped because of an upstream *failure* still produce no result at all.
    skipped: bool = False
    error: str = ""
    run_time_ms: int = Field(
        default=-1,
        description="Run time in milliseconds",
        validation_alias=AliasChoices("run_time_ms", "run_time"),
    )
    is_running: bool = True


class RunInformation(BaseModel):
    """Contains summary information about a complete FlowGraph execution."""

    flow_id: int
    start_time: datetime | None = Field(default_factory=datetime.now)
    end_time: datetime | None = None
    success: bool | None = None
    is_running: bool = False
    execution_mode: str | None = None
    nodes_completed: int = 0
    number_of_nodes: int = 0
    node_step_result: list[NodeResult]
    run_type: Literal["fetch_one", "full_run", "init"]


class BaseItem(BaseModel):
    """A base model for any item in a file system, like a file or directory."""

    name: str
    path: str
    size: int | None = None
    creation_date: datetime | None = None
    access_date: datetime | None = None
    modification_date: datetime | None = None
    source_path: str | None = None
    number_of_items: int = -1


class FileColumn(BaseModel):
    """Represents detailed schema and statistics for a single column (field).

    The statistics fields are None until they are actually computed — either
    never (plain schema previews) or exactly, on demand, via the column-stats
    endpoint writing into the node's ``FlowfileColumn``.
    """

    name: str
    data_type: str
    data_type_group: ReadableDataTypeGroup = "Other"
    is_unique: bool = False
    max_value: str | None = None
    min_value: str | None = None
    average_value: str | None = None
    number_of_empty_values: int | None = None
    number_of_filled_values: int | None = None
    number_of_unique_values: int | None = None
    size: int | None = None


# Preview cells arrive straight from polars `.to_dicts()` / arrow `.to_pylist()`,
# so a Binary or Object column hands pydantic raw Python bytes. Those are not
# JSON-serializable: non-UTF-8 bytes (any real WKB geometry) raise and take the
# whole preview response down with them, while UTF-8-decodable bytes are worse
# still — they silently render as mojibake text. Coerce by value, not by dtype:
# pl.Object reports data_type_group "Other", so a dtype-keyed guard would miss it.
_JSON_NATIVE = (bool, int, float, str)
_BINARY_PREVIEW_BYTES = 16


def _format_binary_cell(raw: bytes) -> str:
    """Render bytes as GIS-conventional uppercase hex, truncated with a byte count."""
    head = raw[:_BINARY_PREVIEW_BYTES].hex().upper()
    if len(raw) <= _BINARY_PREVIEW_BYTES:
        return f"0x{head}"
    return f"0x{head}\u2026 ({len(raw)} bytes)"


def make_preview_cell_json_safe(value: Any) -> Any:
    """Coerce one preview cell to a JSON-serializable value, recursing into containers.

    Lists and structs are preserved as lists/dicts so the frontend keeps rendering
    them as JSON; only genuinely unserializable leaves are replaced.
    """
    if value is None or isinstance(value, _JSON_NATIVE):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        return _format_binary_cell(bytes(value))
    if isinstance(value, dict):
        return {k: make_preview_cell_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [make_preview_cell_json_safe(v) for v in value]
    if isinstance(value, (datetime, date, time_of_day, timedelta, Decimal, UUID, Enum)):
        return value
    return str(value)


class TableExample(BaseModel):
    """Represents a preview of a table, including schema and sample data.

    ``number_of_records`` is None when the total is unknown (e.g. a lazy result
    whose count was never computed); 0 always means a genuinely empty result.
    """

    node_id: int
    number_of_records: int | None = None
    number_of_columns: int
    name: str
    table_schema: list[FileColumn]
    columns: list[str]
    data: list[dict] | None = None
    has_example_data: bool = False
    has_run_with_current_setup: bool = False

    @field_validator("data")
    @classmethod
    def _sanitize_preview_rows(cls, rows: list[dict] | None) -> list[dict] | None:
        if not rows:
            return rows
        return [{k: make_preview_cell_json_safe(v) for k, v in row.items()} for row in rows]


class NodeInputNameInfo(BaseModel):
    """Describes a named input available for a kernel node."""

    name: str
    source_node_id: int
    source_node_type: str


class NodeData(BaseModel):
    """A comprehensive model holding the complete state and data for a single node.

    This includes its input/output data previews, settings, and run status.
    """

    flow_id: int
    node_id: int
    flow_type: str
    left_input: TableExample | None = None
    right_input: TableExample | None = None
    main_input: TableExample | None = None
    main_output: TableExample | None = None
    left_output: TableExample | None = None
    right_output: TableExample | None = None
    has_run: bool = False
    is_cached: bool = False
    setting_input: Any = None
    # Set when column prediction for this node (or one of its inputs) would
    # require executing an un-run kernel node — the user-facing warning text.
    prediction_warning: str | None = None


class OutputFile(BaseItem):
    """Represents a single file in an output directory, extending BaseItem."""

    ext: str | None = None
    mimetype: str | None = None


class OutputFiles(BaseItem):
    """Represents a collection of files, typically within a directory."""

    files: list[OutputFile] = Field(default_factory=list)


class OutputTree(OutputFiles):
    """Represents a directory tree, including subdirectories."""

    directories: list[OutputFiles] = Field(default_factory=list)


class ItemInfo(OutputFile):
    """Provides detailed information about a single item in an output directory."""

    id: int = -1
    type: str
    analysis_file_available: bool = False
    analysis_file_location: str = None
    analysis_file_error: str = None


class OutputDir(BaseItem):
    """Represents the contents of a single output directory."""

    all_items: list[str]
    items: list[ItemInfo]


class ExpressionRef(BaseModel):
    """A reference to a single Polars expression, including its name and docstring."""

    name: str
    doc: str | None


class ExpressionsOverview(BaseModel):
    """Represents a categorized list of available Polars expressions."""

    expression_type: str
    expressions: list[ExpressionRef]


class InstantFuncResult(BaseModel):
    """Represents the result of a function that is expected to execute instantly."""

    success: bool | None = None
    result: str


class NodeDescriptionResponse(BaseModel):
    """Response model for the node description endpoint."""

    description: str = ""
    is_auto_generated: bool = False


class ProjectExportFile(BaseModel):
    """A single file in a project export (path relative to the project root)."""

    path: str
    content: str


class ProjectExportManifest(BaseModel):
    """The full file manifest of a flow exported as a Python project."""

    project_name: str
    files: list[ProjectExportFile]
    warnings: list[str] = Field(default_factory=list)


class ProjectSaveRequest(BaseModel):
    """Request to write a project export to a directory on the server."""

    flow_id: int
    target_directory: str
    overwrite: bool = False


class ProjectSaveResponse(BaseModel):
    """Result of writing a project export to disk."""

    saved_to: str
    file_count: int


class ShareLinkNodeReport(BaseModel):
    """How one node fares in a browser share link."""

    node_id: int
    node_type: str
    status: Literal["supported", "placeholder"]
    reason: str | None = None


class ShareLinkResponse(BaseModel):
    """A flow encoded as a browser (WASM) share link, plus what it cost.

    ``url`` is null only on a hard refusal (the payload is too large to share as
    a link). A degraded share — one with placeholder nodes — still returns a
    usable url with ``compatible=False``.
    """

    url: str | None = None
    hash_chars: int = 0
    compatible: bool
    nodes_report: list[ShareLinkNodeReport] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    placeholder_count: int = 0
    local_file_nodes: list[int] = Field(default_factory=list)
