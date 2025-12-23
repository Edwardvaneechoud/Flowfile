from typing import List, Dict, Optional, Any, Literal
from pydantic import BaseModel, Field
from datetime import datetime
import time


class NodeResult(BaseModel):
    """Represents the execution result of a single node in a FlowGraph run."""
    node_id: int
    node_name: Optional[str] = None
    start_timestamp: float = Field(default_factory=time.time)
    end_timestamp: float = 0
    success: Optional[bool] = None
    error: str = ''
    run_time: int = -1
    is_running: bool = True


class RunInformation(BaseModel):
    """Contains summary information about a complete FlowGraph execution."""
    flow_id: int
    start_time: Optional[datetime] = Field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    success: Optional[bool] = None
    nodes_completed: int = 0
    number_of_nodes: int = 0
    node_step_result: List[NodeResult]
    run_type: Literal["fetch_one", "full_run", "init"]


class BaseItem(BaseModel):
    """A base model for any item in a file system, like a file or directory."""
    name: str
    path: str
    size: Optional[int] = None
    creation_date: Optional[datetime] = None
    access_date: Optional[datetime] = None
    modification_date: Optional[datetime] = None
    source_path: Optional[str] = None
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
    table_schema: List[FileColumn]
    columns: List[str]
    data: Optional[List[Dict]] = {}
    has_example_data: bool = False
    has_run_with_current_setup: bool = False


class NodeData(BaseModel):
    """A comprehensive model holding the complete state and data for a single node.

    This includes its input/output data previews, settings, and run status.
    """
    flow_id: int
    node_id: int
    flow_type: str
    left_input: Optional[TableExample] = None
    right_input: Optional[TableExample] = None
    main_input: Optional[TableExample] = None
    main_output: Optional[TableExample] = None
    left_output: Optional[TableExample] = None
    right_output: Optional[TableExample] = None
    has_run: bool = False
    is_cached: bool = False
    setting_input: Any = None


class OutputFile(BaseItem):
    """Represents a single file in an output directory, extending BaseItem."""
    ext: Optional[str] = None
    mimetype: Optional[str] = None


class OutputFiles(BaseItem):
    """Represents a collection of files, typically within a directory."""
    files: List[OutputFile] = Field(default_factory=list)


class OutputTree(OutputFiles):
    """Represents a directory tree, including subdirectories."""
    directories: List[OutputFiles] = Field(default_factory=list)


class ItemInfo(OutputFile):
    """Provides detailed information about a single item in an output directory."""
    id: int = -1
    type: str
    analysis_file_available: bool = False
    analysis_file_location: str = None
    analysis_file_error: str = None


class OutputDir(BaseItem):
    """Represents the contents of a single output directory."""
    all_items: List[str]
    items: List[ItemInfo]


class ExpressionRef(BaseModel):
    """A reference to a single Polars expression, including its name and docstring."""
    name: str
    doc: Optional[str]


class ExpressionsOverview(BaseModel):
    """Represents a categorized list of available Polars expressions."""
    expression_type: str
    expressions: List[ExpressionRef]


class InstantFuncResult(BaseModel):
    """Represents the result of a function that is expected to execute instantly."""
    success: Optional[bool] = None
    result: str