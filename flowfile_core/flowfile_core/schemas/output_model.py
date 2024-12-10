from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field
from datetime import datetime
import time


class NodeResult(BaseModel):
    node_id: int
    node_name: str = None
    start_timestamp: float = Field(default_factory=time.time)
    end_timestamp: float = 0
    success: Optional[bool] = None
    error: str = ''
    run_time: int = -1
    is_running: bool = True


class RunInformation(BaseModel):
    flow_id: int
    start_time: Optional[datetime] = Field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    success: bool
    nodes_completed: int = 0
    number_of_nodes: int = 0
    node_step_result: List[NodeResult]


class BaseItem(BaseModel):
    name: str
    path: str
    size: Optional[int] = None
    creation_date: Optional[datetime] = None
    access_date: Optional[datetime] = None
    modification_date: Optional[datetime] = None
    source_path: Optional[str] = None
    number_of_items: int = -1


class FileColumn(BaseModel):
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
    node_id: int
    number_of_records: int
    number_of_columns: int
    name: str
    table_schema: List[FileColumn]
    columns: List[str]
    data: Optional[List[Dict]] = {}


class NodeData(BaseModel):
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
    ext: Optional[str] = None
    mimetype: Optional[str] = None


class OutputFiles(BaseItem):
    files: List[OutputFile] = Field(default_factory=list)


class OutputTree(OutputFiles):
    directories: List[OutputFiles] = Field(default_factory=list)


class ItemInfo(OutputFile):
    id: int = -1
    type: str
    analysis_file_available: bool = False
    analysis_file_location: str = None
    analysis_file_error: str = None


class OutputDir(BaseItem):
    all_items: List[str]
    items: List[ItemInfo]


class ExpressionRef(BaseModel):
    name: str
    doc: Optional[str]


class ExpressionsOverview(BaseModel):
    expression_type: str
    expressions: List[ExpressionRef]


class InstantFuncResult(BaseModel):
    success: Optional[bool] = None
    result: str

