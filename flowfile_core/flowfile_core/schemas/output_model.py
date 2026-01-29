import time
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class NodeResult(BaseModel):
    """Represents the execution result of a single node in a FlowGraph run."""

    node_id: int
    node_name: str | None = None
    start_timestamp: float = Field(default_factory=time.time)
    end_timestamp: float = 0
    success: bool | None = None
    error: str = ""
    run_time: int = -1
    is_running: bool = True


class RunInformation(BaseModel):
    """Contains summary information about a complete FlowGraph execution."""

    flow_id: int
    start_time: datetime | None = Field(default_factory=datetime.now)
    end_time: datetime | None = None
    success: bool | None = None
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
    """Represents detailed schema and statistics for a single column (field)."""

    name: str
    data_type: str
    is_unique: bool
    max_value: str
    min_value: str
    number_of_empty_values: int
    number_of_filled_values: int
    number_of_unique_values: int
    size: int


class TableExample(BaseModel):
    """Represents a preview of a table, including schema and sample data."""

    node_id: int
    number_of_records: int
    number_of_columns: int
    name: str
    table_schema: list[FileColumn]
    columns: list[str]
    data: list[dict] | None = {}
    has_example_data: bool = False
    has_run_with_current_setup: bool = False


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


class OutputFile(BaseItem):
    """Represents a single file in an output directory, extending BaseItem."""

    ext: str | None = None
    mimetype: str | None = None


class OutputFiles(BaseItem):
    """Represents a collection of files, typically within a directory."""

    files: list[OutputFile] = Field(default_factory=list)


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
