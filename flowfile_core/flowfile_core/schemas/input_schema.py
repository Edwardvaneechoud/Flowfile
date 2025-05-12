from typing import List, Optional, Literal
from flowfile_core.schemas import transform_schema
from pathlib import Path
import os
from flowfile_core.schemas.analysis_schemas import graphic_walker_schemas as gs_schemas
from flowfile_core.schemas.external_sources.airbyte_schemas import AirbyteConfig
from pydantic import BaseModel, Field, model_validator, SecretStr, ConfigDict


OutputConnectionClass = Literal['output-0', 'output-1', 'output-2', 'output-3', 'output-4',
                                'output-5', 'output-6', 'output-7', 'output-8', 'output-9']

InputConnectionClass = Literal['input-0', 'input-1', 'input-2', 'input-3', 'input-4',
                               'input-5', 'input-6', 'input-7', 'input-8', 'input-9']

InputType = Literal["main", "left", "right"]

class NewDirectory(BaseModel):
    source_path: str
    dir_name: str


class RemoveItem(BaseModel):
    path: str
    id: int = -1


class RemoveItemsInput(BaseModel):
    paths: List[RemoveItem]
    source_path: str


class MinimalFieldInfo(BaseModel):
    name: str
    data_type: str


class ReceivedTableBase(BaseModel):
    id: Optional[int] = None
    name: Optional[str]
    path: str  # This can be an absolute or relative path
    directory: Optional[str] = None
    analysis_file_available: bool = False
    status: Optional[str] = None
    file_type: Optional[str] = None
    fields: List[MinimalFieldInfo] = Field(default_factory=list)
    abs_file_path: Optional[str] = None

    @classmethod
    def create_from_path(cls, path: str):
        filename = Path(path).name
        # Create an instance of ReceivedTableBase with the extracted filename and path
        return cls(name=filename, path=path)

    @property
    def file_path(self) -> str:
        if not self.name in self.path:
            return os.path.join(self.path, self.name)
        else:
            return self.path

    def set_absolute_filepath(self):
        base_path = Path(self.path)
        # Check if the path is relative, resolve it with the current working directory
        if not base_path.is_absolute():
            base_path = Path.cwd() / base_path

        if self.name and self.name not in base_path.name:
            base_path = base_path / self.name

        self.abs_file_path = str(base_path.resolve())

    @model_validator(mode='after')
    def populate_abs_file_path(self):
        if not self.abs_file_path:
            self.set_absolute_filepath()
        return self


class ReceivedCsvTable(ReceivedTableBase):
    file_type: str = 'csv'
    reference: str = ''
    starting_from_line: int = 0
    delimiter: str = ','
    has_headers: bool = True
    encoding: Optional[str] = 'utf-8'
    parquet_ref: Optional[str] = None
    row_delimiter: str = '\n'
    quote_char: str = '"'
    infer_schema_length: int = 10_000
    truncate_ragged_lines: bool = False
    ignore_errors: bool = False


class ReceivedJsonTable(ReceivedCsvTable):
    pass


class ReceivedParquetTable(BaseModel):
    file_type: str = 'parquet'


class ReceivedExcelTable(ReceivedTableBase):
    sheet_name: Optional[str] = None
    start_row: int = 0  # optional
    start_column: int = 0  # optional
    end_row: int = 0  # optional
    end_column: int = 0  # optional
    has_headers: bool = True  # optional
    type_inference: bool = False  # optional

    def validate_range_values(self):
        # Validate that start and end rows/columns are non-negative integers
        for attribute in [self.start_row, self.start_column, self.end_row, self.end_column]:
            if not isinstance(attribute, int) or attribute < 0:
                raise ValueError("Row and column indices must be non-negative integers")

        # Validate that start is before end if end is specified (non-zero)
        if (self.end_row > 0 and self.start_row > self.end_row) or \
                (self.end_column > 0 and self.start_column > self.end_column):
            raise ValueError("Start row/column must not be greater than end row/column if specified")


class ReceivedTable(ReceivedExcelTable, ReceivedCsvTable, ReceivedParquetTable):
    ...


class OutputCsvTable(BaseModel):
    file_type: str = 'csv'
    delimiter: str = ','
    encoding: str = 'utf-8'


class OutputParquetTable(BaseModel):
    file_type: str = 'parquet'


class OutputExcelTable(BaseModel):
    file_type: str = 'excel'
    sheet_name: str = 'Sheet1'


class OutputSettings(BaseModel):
    name: str
    directory: str
    file_type: str
    fields: Optional[List[str]] = Field(default_factory=list)
    write_mode: str = 'overwrite'
    output_csv_table: OutputCsvTable
    output_parquet_table: OutputParquetTable
    output_excel_table: OutputExcelTable
    abs_file_path: Optional[str] = None

    def set_absolute_filepath(self):
        base_path = Path(self.directory)
        # Check if the path is relative, resolve it with the current working directory
        if not base_path.is_absolute():
            base_path = Path.cwd() / base_path

        if self.name and self.name not in base_path.name:
            base_path = base_path / self.name

        self.abs_file_path = str(base_path.resolve())

    @model_validator(mode='after')
    def populate_abs_file_path(self):
        self.set_absolute_filepath()
        return self


class NodeBase(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    flow_id: int
    node_id: int
    cache_results: Optional[bool] = False
    pos_x: Optional[float] = 0
    pos_y: Optional[float] = 0
    is_setup: Optional[bool] = True
    description: Optional[str] = ''
    user_id: Optional[int] = None

    @classmethod
    def overridden_hash(cls):
        if getattr(cls, '__hash__'):
            return BaseModel.__hash__ is not getattr(cls, '__hash__')
        return False


class NodeSingleInput(NodeBase):
    depending_on_id: Optional[int] = -1


class NodeMultiInput(NodeBase):
    depending_on_ids: Optional[List[int]] = [-1]


class NodeSelect(NodeSingleInput):
    keep_missing: bool = True
    select_input: List[transform_schema.SelectInput] = Field(default_factory=list)
    sorted_by: Optional[Literal['none', 'asc', 'desc']] = 'none'


class NodeFilter(NodeSingleInput):
    filter_input: transform_schema.FilterInput


class NodeSort(NodeSingleInput):
    sort_input: List[transform_schema.SortByInput] = Field(default_factory=list)


class NodeTextToRows(NodeSingleInput):
    text_to_rows_input: transform_schema.TextToRowsInput


class NodeSample(NodeSingleInput):
    sample_size: int = 1000


class NodeRecordId(NodeSingleInput):
    record_id_input: transform_schema.RecordIdInput


class NodeJoin(NodeMultiInput):
    auto_generate_selection: bool = True
    verify_integrity: bool = True
    join_input: transform_schema.JoinInput
    auto_keep_all: bool = True
    auto_keep_right: bool = True
    auto_keep_left: bool = True


class NodeCrossJoin(NodeMultiInput):
    auto_generate_selection: bool = True
    verify_integrity: bool = True
    cross_join_input: transform_schema.CrossJoinInput
    auto_keep_all: bool = True
    auto_keep_right: bool = True
    auto_keep_left: bool = True


class NodeFuzzyMatch(NodeJoin):
    join_input: transform_schema.FuzzyMatchInput


class NodeDatasource(NodeBase):
    file_ref: str = None


class NodeManualInput(NodeBase):
    raw_data: List = None


class NodeRead(NodeBase):
    received_file: ReceivedTable


class DatabaseConnection(BaseModel):
    database_type: str = "postgresql"  # Database type (postgresql, mysql, etc.)
    username: Optional[str] = None
    password_ref: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    url: Optional[str] = None


class FullDatabaseConnection(BaseModel):
    connection_name: str
    database_type: str = "postgresql"  # Database type (postgresql, mysql, etc.)
    username: str
    password: SecretStr
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    ssl_enabled: Optional[bool] = False
    url: Optional[str] = None


class FullDatabaseConnectionInterface(BaseModel):
    connection_name: str
    database_type: str = "postgresql"  # Database type (postgresql, mysql, etc.)
    username: str
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    ssl_enabled: Optional[bool] = False
    url: Optional[str] = None


class DatabaseSettings(BaseModel):
    connection_mode: Optional[Literal['inline', 'reference']] = 'inline'
    database_connection: Optional[DatabaseConnection] = None
    database_connection_name: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    query: Optional[str] = None
    query_mode: Literal['query', 'table', 'reference'] = 'table'

    @model_validator(mode='after')
    def validate_table_or_query(self):
        # Validate that either table_name or query is provided
        if (not self.table_name and not self.query) and self.query_mode == 'inline':
            raise ValueError("Either 'table_name' or 'query' must be provided")

        # Validate correct connection information based on connection_mode
        if self.connection_mode == 'inline' and self.database_connection is None:
            raise ValueError("When 'connection_mode' is 'inline', 'database_connection' must be provided")

        if self.connection_mode == 'reference' and not self.database_connection_name:
            raise ValueError("When 'connection_mode' is 'reference', 'database_connection_name' must be provided")

        return self


class DatabaseWriteSettings(BaseModel):
    connection_mode: Optional[Literal['inline', 'reference']] = 'inline'
    database_connection: Optional[DatabaseConnection] = None
    database_connection_name: Optional[str] = None
    table_name: str
    schema_name: Optional[str] = None
    if_exists: Optional[Literal['append', 'replace', 'fail']] = 'append'


class NodeDatabaseReader(NodeBase):
    database_settings: DatabaseSettings
    fields: Optional[List[MinimalFieldInfo]] = None


class NodeDatabaseWriter(NodeSingleInput):
    database_write_settings: DatabaseWriteSettings


class ExternalSource(BaseModel):
    orientation: str = 'row'
    fields: Optional[List[MinimalFieldInfo]] = None


class SampleUsers(ExternalSource):
    SAMPLE_USERS: bool
    class_name: str = "sample_users"
    size: int = 100


class GoogleSheet(ExternalSource):
    GOOGLE_SHEET: bool
    class_name: str = "google_sheet"
    access_token: SecretStr = None
    sheet_id: str
    worksheet_name: str
    sheet_name: str


class AirbyteReader(AirbyteConfig):
    class_name: Optional[str] = "airbyte_reader"
    fields: Optional[List[MinimalFieldInfo]] = None


class AccessToken(BaseModel):
    user_id: str
    access_token: SecretStr = None


class NodeExternalSource(NodeBase):
    identifier: str
    source_settings: SampleUsers | GoogleSheet


class NodeAirbyteReader(NodeExternalSource):
    identifier: str = 'airbyte'
    source_settings: AirbyteReader


class NodeFormula(NodeSingleInput):
    function: transform_schema.FunctionInput = None


class NodeGroupBy(NodeSingleInput):
    groupby_input: transform_schema.GroupByInput = None


class NodePromise(NodeBase):
    is_setup: bool = False
    node_type: str


class NodeInputConnection(BaseModel):
    node_id: int
    connection_class: InputConnectionClass

    def get_node_input_connection_type(self) -> Literal['main', 'right', 'left']:
        match self.connection_class:
            case 'input-0':
                return 'main'
            case 'input-1':
                return 'right'
            case 'input-2':
                return 'left'
            case _:
                raise ValueError(f"Unexpected connection_class: {self.connection_class}")


class NodePivot(NodeSingleInput):
    pivot_input: transform_schema.PivotInput = None
    output_fields: Optional[List[MinimalFieldInfo]] = None


class NodeUnpivot(NodeSingleInput):
    unpivot_input: transform_schema.UnpivotInput = None


class NodeUnion(NodeMultiInput):
    union_input: transform_schema.UnionInput = Field(default_factory=transform_schema.UnionInput)


class NodeOutput(NodeSingleInput):
    output_settings: OutputSettings


class NodeOutputConnection(BaseModel):
    node_id: int
    connection_class: OutputConnectionClass


class NodeConnection(BaseModel):
    input_connection: NodeInputConnection
    output_connection: NodeOutputConnection

    @classmethod
    def create_from_simple_input(cls, from_id: int, to_id: int, input_type: InputType = "input-0"):

        match input_type:
            case "main":
                connection_class: InputConnectionClass = "input-0"
            case "right":
                connection_class: InputConnectionClass = "input-1"
            case "left":
                connection_class: InputConnectionClass = "input-2"
            case _:
                connection_class: InputConnectionClass = "input-0"
        node_input = NodeInputConnection(node_id=to_id, connection_class=connection_class)
        node_output = NodeOutputConnection(node_id=from_id, connection_class='output-0')
        return cls(input_connection=node_input, output_connection=node_output)


class NodeDescription(BaseModel):
    description: str = ''


class NodeExploreData(NodeBase):
    graphic_walker_input: Optional[gs_schemas.GraphicWalkerInput] = None
    _hash_overrule: int = 0

    def __hash__(self):
        return 0


class NodeGraphSolver(NodeSingleInput):
    graph_solver_input: transform_schema.GraphSolverInput


class NodeUnique(NodeSingleInput):
    unique_input: transform_schema.UniqueInput


class NodeRecordCount(NodeSingleInput):
    pass


class NodePolarsCode(NodeMultiInput):
    polars_code_input: transform_schema.PolarsCodeInput
