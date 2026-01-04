"""
Legacy schema definitions for loading old flowfile pickles.

These definitions mirror the OLD schema structure BEFORE the migration to the new
discriminated union table_settings format.

OLD structure:
- ReceivedTable: All fields flat (delimiter, encoding, sheet_name all at top level)
- OutputSettings: Separate output_csv_table, output_parquet_table, output_excel_table fields

NEW structure:
- ReceivedTable: Has nested table_settings with discriminated union
- OutputSettings: Has single table_settings field with discriminated union

DO NOT USE THESE IN PRODUCTION CODE - use the actual schemas from flowfile_core.
"""

from dataclasses import dataclass, field
from typing import Any, Literal

from pydantic import BaseModel, Field

# =============================================================================
# OLD INPUT/OUTPUT SCHEMAS (before table_settings migration)
# These mirror the structure that exists in old pickle files
# =============================================================================


class MinimalFieldInfo(BaseModel):
    """Represents the most basic information about a data field (column)."""

    name: str
    data_type: str = "String"


class OutputCsvTable(BaseModel):
    """OLD: Settings for writing a CSV file."""

    file_type: str = "csv"
    delimiter: str = ","
    encoding: str = "utf-8"


class OutputParquetTable(BaseModel):
    """OLD: Settings for writing a Parquet file."""

    file_type: str = "parquet"


class OutputExcelTable(BaseModel):
    """OLD: Settings for writing an Excel file."""

    file_type: str = "excel"
    sheet_name: str = "Sheet1"


class OutputSettings(BaseModel):
    """OLD OutputSettings structure with SEPARATE table fields.

    This is the OLD format where CSV, Parquet, and Excel settings
    were stored in separate fields rather than a unified table_settings.
    """

    name: str
    directory: str
    file_type: str
    fields: list[str] | None = Field(default_factory=list)
    write_mode: str = "overwrite"
    # OLD: Separate fields for each output type
    output_csv_table: OutputCsvTable | None = Field(default_factory=OutputCsvTable)
    output_parquet_table: OutputParquetTable = Field(default_factory=OutputParquetTable)
    output_excel_table: OutputExcelTable = Field(default_factory=OutputExcelTable)
    abs_file_path: str | None = None


class ReceivedTable(BaseModel):
    """OLD ReceivedTable structure with FLAT fields.

    This is the OLD format where all settings (CSV, Excel, Parquet)
    were stored as flat fields on the model rather than nested in table_settings.
    """

    # Metadata fields
    id: int | None = None
    name: str | None = None
    path: str = ""
    directory: str | None = None
    analysis_file_available: bool = False
    status: str | None = None
    file_type: str | None = None
    fields: list[MinimalFieldInfo] = Field(default_factory=list)
    abs_file_path: str | None = None

    # OLD: CSV/JSON fields at top level (not nested)
    reference: str = ""
    starting_from_line: int = 0
    delimiter: str = ","
    has_headers: bool = True
    encoding: str | None = "utf-8"
    parquet_ref: str | None = None
    row_delimiter: str = "\n"
    quote_char: str = '"'
    infer_schema_length: int = 10_000
    truncate_ragged_lines: bool = False
    ignore_errors: bool = False

    # OLD: Excel fields at top level (not nested)
    sheet_name: str | None = None
    start_row: int = 0
    start_column: int = 0
    end_row: int = 0
    end_column: int = 0
    type_inference: bool = False


# =============================================================================
# FLOW AND NODE SCHEMAS (Pydantic - structure unchanged, just re-exported)
# =============================================================================


class FlowGraphConfig(BaseModel):
    """Configuration model for a flow graph's basic properties."""

    flow_id: int = 1
    description: str | None = None
    save_location: str | None = None
    name: str = ""
    path: str = ""
    execution_mode: str = "Performance"
    execution_location: str = "local"


class FlowSettings(FlowGraphConfig):
    """Extends FlowGraphConfig with additional operational settings."""

    auto_save: bool = False
    modified_on: float | None = None
    show_detailed_progress: bool = True
    is_running: bool = False
    is_canceled: bool = False


class NodeBase(BaseModel):
    """Base model for all nodes in a FlowGraph."""

    flow_id: int
    node_id: int
    cache_results: bool | None = False
    pos_x: float | None = 0
    pos_y: float | None = 0
    is_setup: bool | None = True
    description: str | None = ""
    user_id: int | None = None
    is_flow_output: bool | None = False
    is_user_defined: bool | None = False


class NodeSingleInput(NodeBase):
    """A base model for any node that takes a single data input."""

    depending_on_id: int | None = -1


class NodeMultiInput(NodeBase):
    """A base model for any node that takes multiple data inputs."""

    depending_on_ids: list[int] | None = Field(default_factory=lambda: [-1])


class NodeRead(NodeBase):
    """Settings for a node that reads data from a file."""

    received_file: ReceivedTable


class NodeSelect(NodeSingleInput):
    """Settings for a node that selects, renames, and reorders columns."""

    keep_missing: bool = True
    select_input: list[Any] = Field(default_factory=list)
    sorted_by: str | None = "none"


class NodeFilter(NodeSingleInput):
    """Settings for a node that filters rows based on a condition."""

    filter_input: Any = None


class NodeFormula(NodeSingleInput):
    """Settings for a node that applies a formula to create/modify a column."""

    function: Any = None


class NodeJoin(NodeMultiInput):
    """Settings for a node that performs a standard SQL-style join."""

    auto_generate_selection: bool = True
    verify_integrity: bool = True
    join_input: Any = None
    auto_keep_all: bool = True
    auto_keep_right: bool = True
    auto_keep_left: bool = True


class NodeCrossJoin(NodeMultiInput):
    """Settings for a node that performs a cross join."""

    auto_generate_selection: bool = True
    verify_integrity: bool = True
    cross_join_input: Any = None
    auto_keep_all: bool = True
    auto_keep_right: bool = True
    auto_keep_left: bool = True


class NodeFuzzyMatch(NodeMultiInput):
    """Settings for a node that performs a fuzzy join."""

    auto_generate_selection: bool = True
    verify_integrity: bool = True
    join_input: Any = None  # FuzzyMatchInput
    auto_keep_all: bool = True
    auto_keep_right: bool = True
    auto_keep_left: bool = True


class NodePolarsCode(NodeMultiInput):
    """Settings for a node that executes arbitrary Polars code."""

    polars_code_input: Any = None


class NodeOutput(NodeSingleInput):
    """Settings for a node that writes its input to a file."""

    output_settings: OutputSettings


class NodeGroupBy(NodeSingleInput):
    """Settings for a node that performs a group-by and aggregation."""

    groupby_input: Any = None


class NodeSort(NodeSingleInput):
    """Settings for a node that sorts the data."""

    sort_input: list[Any] = Field(default_factory=list)


class NodeUnion(NodeMultiInput):
    """Settings for a node that concatenates multiple inputs."""

    union_input: Any = None


class NodeUnique(NodeSingleInput):
    """Settings for a node that returns unique rows."""

    unique_input: Any = None


class NodePivot(NodeSingleInput):
    """Settings for a node that pivots data."""

    pivot_input: Any = None
    output_fields: list[MinimalFieldInfo] | None = None


class NodeUnpivot(NodeSingleInput):
    """Settings for a node that unpivots data."""

    unpivot_input: Any = None


class NodeRecordId(NodeSingleInput):
    """Settings for adding a record ID column."""

    record_id_input: Any = None


class NodeTextToRows(NodeSingleInput):
    """Settings for splitting text into rows."""

    text_to_rows_input: Any = None


class NodeGraphSolver(NodeSingleInput):
    """Settings for graph-solving operations."""

    graph_solver_input: Any = None


class NodeSample(NodeSingleInput):
    """Settings for sampling data."""

    sample_size: int = 1000


class NodePromise(NodeBase):
    """A placeholder node not yet configured."""

    is_setup: bool = False
    node_type: str = ""


class DatabaseConnection(BaseModel):
    """Defines database connection parameters."""

    database_type: str = "postgresql"
    username: str | None = None
    password_ref: str | None = None
    host: str | None = None
    port: int | None = None
    database: str | None = None
    url: str | None = None


class DatabaseSettings(BaseModel):
    """Defines settings for reading from a database."""

    connection_mode: str | None = "inline"
    database_connection: DatabaseConnection | None = None
    database_connection_name: str | None = None
    schema_name: str | None = None
    table_name: str | None = None
    query: str | None = None
    query_mode: str = "table"


class NodeDatabaseReader(NodeBase):
    """Settings for reading from a database."""

    database_settings: DatabaseSettings
    fields: list[MinimalFieldInfo] | None = None


class NodeInformation(BaseModel):
    """Stores the state and configuration of a node instance."""

    id: int | None = None
    type: str | None = None
    is_setup: bool | None = None
    description: str | None = ""
    x_position: int | None = 0
    y_position: int | None = 0
    left_input_id: int | None = None
    right_input_id: int | None = None
    input_ids: list[int] | None = Field(default_factory=lambda: [-1])
    outputs: list[int] | None = Field(default_factory=lambda: [-1])
    setting_input: Any | None = None


class FlowInformation(BaseModel):
    """Represents the complete state of a flow."""

    flow_id: int
    flow_name: str | None = ""
    flow_settings: FlowSettings | None = None
    data: dict[int, NodeInformation] = Field(default_factory=dict)
    node_starts: list[int] = Field(default_factory=list)
    node_connections: list[tuple[int, int]] = Field(default_factory=list)


# =============================================================================
# TRANSFORM SCHEMAS (dataclasses - these changed from @dataclass to BaseModel)
# =============================================================================


@dataclass
class SelectInput:
    """Defines how a single column should be selected, renamed, or type-cast."""

    old_name: str
    original_position: int | None = None
    new_name: str | None = None
    data_type: str | None = None
    data_type_change: bool | None = False
    join_key: bool | None = False
    is_altered: bool | None = False
    position: int | None = None
    is_available: bool | None = True
    keep: bool | None = True

    def __post_init__(self):
        if self.new_name is None:
            self.new_name = self.old_name


@dataclass
class FieldInput:
    """Represents a single field with its name and data type."""

    name: str
    data_type: str | None = None


@dataclass
class FunctionInput:
    """Defines a formula to be applied."""

    field: FieldInput = None
    function: str = ""


@dataclass
class BasicFilter:
    """Defines a simple, single-condition filter."""

    field: str = ""
    filter_type: str = ""
    filter_value: str = ""


@dataclass
class FilterInput:
    """Defines the settings for a filter operation."""

    advanced_filter: str = ""
    basic_filter: BasicFilter = None
    filter_type: str = "basic"


@dataclass
class SelectInputs:
    """A container for a list of SelectInput objects."""

    renames: list[SelectInput] = field(default_factory=list)

    @property
    def old_cols(self) -> set:
        return set(v.old_name for v in self.renames if v.keep)

    @property
    def new_cols(self) -> set:
        return set(v.new_name for v in self.renames if v.keep)


@dataclass
class JoinInputs:
    """Extends SelectInputs with functionality specific to join operations."""

    renames: list[SelectInput] = field(default_factory=list)


@dataclass
class JoinMap:
    """Defines a single mapping between a left and right column for a join key."""

    left_col: str = None
    right_col: str = None


@dataclass
class CrossJoinInput:
    """Defines the settings for a cross join operation."""

    left_select: Any = None
    right_select: Any = None


@dataclass
class JoinInput:
    """Defines the settings for a standard SQL-style join."""

    join_mapping: list[JoinMap] = field(default_factory=list)
    left_select: Any = None
    right_select: Any = None
    how: str = "inner"


@dataclass
class FuzzyMapping:
    """Defines a fuzzy match column mapping with threshold."""

    left_col: str = None
    right_col: str = None
    threshold_score: int = 80
    fuzzy_type: str = "levenshtein"


@dataclass
class FuzzyMatchInput:
    """Extends JoinInput with settings specific to fuzzy matching."""

    join_mapping: list[FuzzyMapping] = field(default_factory=list)
    left_select: Any = None
    right_select: Any = None
    how: str = "inner"
    aggregate_output: bool = False


@dataclass
class AggColl:
    """Represents a single aggregation operation."""

    old_name: str = None
    agg: str = None
    new_name: str | None = None
    output_type: str | None = None


@dataclass
class GroupByInput:
    """Represents the input for a group by operation."""

    agg_cols: list[AggColl] = field(default_factory=list)


@dataclass
class PivotInput:
    """Defines the settings for a pivot operation."""

    index_columns: list[str] = field(default_factory=list)
    pivot_column: str = None
    value_col: str = None
    aggregations: list[str] = field(default_factory=list)


@dataclass
class SortByInput:
    """Defines a single sort condition on a column."""

    column: str = None
    how: str = "asc"


@dataclass
class RecordIdInput:
    """Defines settings for adding a record ID column."""

    output_column_name: str = "record_id"
    offset: int = 1
    group_by: bool | None = False
    group_by_columns: list[str] | None = field(default_factory=list)


@dataclass
class TextToRowsInput:
    """Defines settings for splitting a text column into multiple rows."""

    column_to_split: str = None
    output_column_name: str | None = None
    split_by_fixed_value: bool | None = True
    split_fixed_value: str | None = ","
    split_by_column: str | None = None


@dataclass
class UnpivotInput:
    """Defines settings for an unpivot operation."""

    index_columns: list[str] | None = field(default_factory=list)
    value_columns: list[str] | None = field(default_factory=list)
    data_type_selector: Literal["float", "all", "date", "numeric", "string"] | None = None
    data_type_selector_mode: Literal["data_type", "column"] | None = "column"

    def __post_init__(self):
        if self.index_columns is None:
            self.index_columns = []
        if self.value_columns is None:
            self.value_columns = []


@dataclass
class UnionInput:
    """Defines settings for a union operation."""

    mode: Literal["selective", "relaxed"] = "relaxed"


@dataclass
class UniqueInput:
    """Defines settings for a uniqueness operation."""

    columns: list[str] | None = None
    strategy: str = "any"


@dataclass
class GraphSolverInput:
    """Defines settings for a graph-solving operation."""

    col_from: str = None
    col_to: str = None
    output_column_name: str | None = "graph_group"


@dataclass
class PolarsCodeInput:
    """A simple container for user-provided Polars code."""

    polars_code: str = ""


@dataclass
class SampleInput:
    """Defines settings for sampling rows."""

    n: int | None = None
    fraction: float | None = None
    with_replacement: bool = False
    shuffle: bool = False
    seed: int | None = None


# =============================================================================
# CLASS NAME MAPPING for pickle.Unpickler.find_class
# Maps class names to their legacy implementations for unpickling
# =============================================================================

LEGACY_CLASS_MAP = {
    # Transform schema dataclasses
    "SelectInput": SelectInput,
    "FieldInput": FieldInput,
    "FunctionInput": FunctionInput,
    "BasicFilter": BasicFilter,
    "FilterInput": FilterInput,
    "SelectInputs": SelectInputs,
    "JoinInputs": JoinInputs,
    "JoinMap": JoinMap,
    "CrossJoinInput": CrossJoinInput,
    "JoinInput": JoinInput,
    "FuzzyMapping": FuzzyMapping,
    "FuzzyMatchInput": FuzzyMatchInput,
    "AggColl": AggColl,
    "GroupByInput": GroupByInput,
    "PivotInput": PivotInput,
    "SortByInput": SortByInput,
    "RecordIdInput": RecordIdInput,
    "TextToRowsInput": TextToRowsInput,
    "UnpivotInput": UnpivotInput,
    "UnionInput": UnionInput,
    "UniqueInput": UniqueInput,
    "GraphSolverInput": GraphSolverInput,
    "PolarsCodeInput": PolarsCodeInput,
    "SampleInput": SampleInput,
    # OLD Input/Output schemas (before table_settings)
    "ReceivedTable": ReceivedTable,
    "OutputSettings": OutputSettings,
    "OutputCsvTable": OutputCsvTable,
    "OutputParquetTable": OutputParquetTable,
    "OutputExcelTable": OutputExcelTable,
    "MinimalFieldInfo": MinimalFieldInfo,
    # Flow and Node schemas
    "FlowSettings": FlowSettings,
    "FlowGraphConfig": FlowGraphConfig,
    "FlowInformation": FlowInformation,
    "NodeInformation": NodeInformation,
    "NodeBase": NodeBase,
    "NodeSingleInput": NodeSingleInput,
    "NodeMultiInput": NodeMultiInput,
    "NodeRead": NodeRead,
    "NodeSelect": NodeSelect,
    "NodeFilter": NodeFilter,
    "NodeFormula": NodeFormula,
    "NodeJoin": NodeJoin,
    "NodeCrossJoin": NodeCrossJoin,
    "NodeFuzzyMatch": NodeFuzzyMatch,
    "NodePolarsCode": NodePolarsCode,
    "NodeOutput": NodeOutput,
    "NodeGroupBy": NodeGroupBy,
    "NodeSort": NodeSort,
    "NodeUnion": NodeUnion,
    "NodeUnique": NodeUnique,
    "NodePivot": NodePivot,
    "NodeUnpivot": NodeUnpivot,
    "NodeRecordId": NodeRecordId,
    "NodeTextToRows": NodeTextToRows,
    "NodeGraphSolver": NodeGraphSolver,
    "NodeSample": NodeSample,
    "NodePromise": NodePromise,
    "DatabaseConnection": DatabaseConnection,
    "DatabaseSettings": DatabaseSettings,
    "NodeDatabaseReader": NodeDatabaseReader,
}


# Export all classes
__all__ = list(LEGACY_CLASS_MAP.keys()) + ["LEGACY_CLASS_MAP"]
