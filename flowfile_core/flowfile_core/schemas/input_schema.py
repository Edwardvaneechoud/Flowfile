import os
from pathlib import Path
from typing import Annotated, Any, Literal

import polars as pl
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    StringConstraints,
    ValidationInfo,
    field_validator,
    model_validator,
)

from flowfile_core.schemas import transform_schema
from flowfile_core.schemas.analysis_schemas import graphic_walker_schemas as gs_schemas
from flowfile_core.schemas.cloud_storage_schemas import CloudStorageReadSettings, CloudStorageWriteSettings
from flowfile_core.schemas.yaml_types import (
    NodeCrossJoinYaml,
    NodeFuzzyMatchYaml,
    NodeJoinYaml,
    NodeOutputYaml,
    NodeSelectYaml,
    OutputSettingsYaml,
)
from flowfile_core.types import DataTypeStr
from flowfile_core.utils.utils import ensure_similarity_dicts, standardize_col_dtype

SecretRef = Annotated[
    str, StringConstraints(min_length=1, max_length=100), Field(description="An ID referencing an encrypted secret.")
]


OutputConnectionClass = Literal[
    "output-0",
    "output-1",
    "output-2",
    "output-3",
    "output-4",
    "output-5",
    "output-6",
    "output-7",
    "output-8",
    "output-9",
]

InputConnectionClass = Literal[
    "input-0", "input-1", "input-2", "input-3", "input-4", "input-5", "input-6", "input-7", "input-8", "input-9"
]

InputType = Literal["main", "left", "right"]


class NewDirectory(BaseModel):
    """Defines the information required to create a new directory."""

    source_path: str
    dir_name: str


class RemoveItem(BaseModel):
    """Represents a single item to be removed from a directory or list."""

    path: str
    id: int = -1


class RemoveItemsInput(BaseModel):
    """Defines a list of items to be removed."""

    paths: list[RemoveItem]
    source_path: str


class MinimalFieldInfo(BaseModel):
    """Represents the most basic information about a data field (column)."""

    name: str
    data_type: str = "String"


class OutputFieldInfo(BaseModel):
    """Field information with optional default value for output field configuration."""

    name: str
    data_type: DataTypeStr = "String"
    default_value: str | None = None  # Can be a literal value or expression


class OutputFieldConfig(BaseModel):
    """Configuration for output field validation and transformation behavior."""

    enabled: bool = False
    validation_mode_behavior: Literal[
        "add_missing",  # Add missing fields with defaults, remove extra columns
        "add_missing_keep_extra",  # Add missing fields with defaults, keep all incoming columns
        "raise_on_missing",  # Raise error if any fields are missing
        "select_only"  # Select only specified fields, skip missing silently
    ] = "select_only"
    fields: list[OutputFieldInfo] = Field(default_factory=list)
    validate_data_types: bool = False  # Enable data type validation without casting


class InputTableBase(BaseModel):
    """Base settings for input file operations."""

    file_type: str  # Will be overridden with Literal in subclasses


class InputCsvTable(InputTableBase):
    """Defines settings for reading a CSV file."""

    file_type: Literal["csv"] = "csv"
    reference: str = ""
    starting_from_line: int = 0
    delimiter: str = ","
    has_headers: bool = True
    encoding: str = "utf-8"
    parquet_ref: str | None = None
    row_delimiter: str = "\n"
    quote_char: str = '"'
    infer_schema_length: int = 10_000
    truncate_ragged_lines: bool = False
    ignore_errors: bool = False


class InputJsonTable(InputCsvTable):
    """Defines settings for reading a JSON file."""

    file_type: Literal["json"] = "json"


class InputParquetTable(InputTableBase):
    """Defines settings for reading a Parquet file."""

    file_type: Literal["parquet"] = "parquet"


class InputExcelTable(InputTableBase):
    """Defines settings for reading an Excel file."""

    file_type: Literal["excel"] = "excel"
    sheet_name: str | None = None
    start_row: int = 0
    start_column: int = 0
    end_row: int = 0
    end_column: int = 0
    has_headers: bool = True
    type_inference: bool = False

    @model_validator(mode="after")
    def validate_range_values(self):
        """Validates that the Excel cell range is logical."""
        for attribute in [self.start_row, self.start_column, self.end_row, self.end_column]:
            if not isinstance(attribute, int) or attribute < 0:
                raise ValueError("Row and column indices must be non-negative integers")
        if (self.end_row > 0 and self.start_row > self.end_row) or (
            self.end_column > 0 and self.start_column > self.end_column
        ):
            raise ValueError("Start row/column must not be greater than end row/column")
        return self


# Create the discriminated union (similar to OutputTableSettings)
InputTableSettings = Annotated[
    InputCsvTable | InputJsonTable | InputParquetTable | InputExcelTable, Field(discriminator="file_type")
]


# Now create the main ReceivedTable model
class ReceivedTable(BaseModel):
    """Model for defining a table received from an external source."""

    # Metadata fields
    id: int | None = None
    name: str | None = None
    path: str  # This can be an absolute or relative path
    directory: str | None = None
    analysis_file_available: bool = False
    status: str | None = None
    fields: list[MinimalFieldInfo] = Field(default_factory=list)
    abs_file_path: str | None = None

    file_type: Literal["csv", "json", "parquet", "excel"]

    table_settings: InputTableSettings

    @classmethod
    def create_from_path(cls, path: str, file_type: Literal["csv", "json", "parquet", "excel"] = "csv"):
        """Creates an instance from a file path string."""
        filename = Path(path).name

        # Create appropriate table_settings based on file_type
        settings_map = {
            "csv": InputCsvTable(),
            "json": InputJsonTable(),
            "parquet": InputParquetTable(),
            "excel": InputExcelTable(),
        }

        return cls(
            name=filename, path=path, file_type=file_type, table_settings=settings_map.get(file_type, InputCsvTable())
        )

    @property
    def file_path(self) -> str:
        """Constructs the full file path from the directory and name."""
        if self.name and self.name not in self.path:
            return os.path.join(self.path, self.name)
        else:
            return self.path

    def set_absolute_filepath(self):
        """Resolves the path to an absolute file path."""
        base_path = Path(self.path).expanduser()
        if not base_path.is_absolute():
            base_path = Path.cwd() / base_path
        if self.name and self.name not in base_path.name:
            base_path = base_path / self.name
        self.abs_file_path = str(base_path.resolve())

    @model_validator(mode="before")
    @classmethod
    def set_default_table_settings(cls, data):
        """Create default table_settings based on file_type if not provided."""
        if isinstance(data, dict):
            if "table_settings" not in data or data["table_settings"] is None:
                data["table_settings"] = {}

            if isinstance(data["table_settings"], dict) and "file_type" not in data["table_settings"]:
                data["table_settings"]["file_type"] = data.get("file_type", "csv")
        return data

    @model_validator(mode="after")
    def populate_abs_file_path(self):
        """Ensures the absolute file path is populated after validation."""
        if not self.abs_file_path:
            self.set_absolute_filepath()
        return self


class OutputCsvTable(BaseModel):
    """Defines settings for writing a CSV file."""

    file_type: Literal["csv"] = "csv"
    delimiter: str = ","
    encoding: str = "utf-8"


class OutputParquetTable(BaseModel):
    """Defines settings for writing a Parquet file."""

    file_type: Literal["parquet"] = "parquet"


class OutputExcelTable(BaseModel):
    """Defines settings for writing an Excel file."""

    file_type: Literal["excel"] = "excel"
    sheet_name: str = "Sheet1"


# Create a discriminated union
OutputTableSettings = Annotated[
    OutputCsvTable | OutputParquetTable | OutputExcelTable, Field(discriminator="file_type")
]


class OutputSettings(BaseModel):
    """Defines the complete settings for an output node."""

    name: str
    directory: str
    file_type: str  # This drives which table_settings to use
    fields: list[str] | None = Field(default_factory=list)
    write_mode: str = "overwrite"
    table_settings: OutputTableSettings
    abs_file_path: str | None = None

    def to_yaml_dict(self) -> OutputSettingsYaml:
        """Converts the output settings to a dictionary suitable for YAML serialization."""
        result: OutputSettingsYaml = {
            "name": self.name,
            "directory": self.directory,
            "file_type": self.file_type,
            "write_mode": self.write_mode,
        }
        if self.abs_file_path:
            result["abs_file_path"] = self.abs_file_path
        if self.fields:
            result["fields"] = self.fields
        # Only include table_settings if it has non-default values beyond file_type
        ts_dict = self.table_settings.model_dump(exclude={"file_type"})
        if any(v for v in ts_dict.values()):  # Has meaningful settings
            result["table_settings"] = ts_dict
        return result

    @property
    def sheet_name(self) -> str | None:
        if self.file_type == "excel":
            return self.table_settings.sheet_name

    @property
    def delimiter(self) -> str | None:
        if self.file_type == "csv":
            return self.table_settings.delimiter

    @field_validator("table_settings", mode="before")
    @classmethod
    def validate_table_settings(cls, v, info: ValidationInfo):
        """Ensures table_settings matches the file_type."""
        if v is None:
            file_type = info.data.get("file_type", "csv")
            # Create default based on file_type
            match file_type:
                case "csv":
                    return OutputCsvTable()
                case "parquet":
                    return OutputParquetTable()
                case "excel":
                    return OutputExcelTable()
                case _:
                    return OutputCsvTable()

        # If it's a dict, add file_type if missing
        if isinstance(v, dict) and "file_type" not in v:
            v["file_type"] = info.data.get("file_type", "csv")

        return v

    def set_absolute_filepath(self):
        """Resolves the output directory and name into an absolute path."""
        base_path = Path(self.directory)
        if not base_path.is_absolute():
            base_path = Path.cwd() / base_path
        if self.name and self.name not in base_path.name:
            base_path = base_path / self.name
        self.abs_file_path = str(base_path.resolve())

    @model_validator(mode="after")
    def populate_abs_file_path(self):
        """Ensures the absolute file path is populated after validation."""
        self.set_absolute_filepath()
        return self


class NodeBase(BaseModel):
    """Base model for all nodes in a FlowGraph. Contains common metadata."""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    flow_id: int
    node_id: int
    cache_results: bool | None = False
    pos_x: float | None = 0
    pos_y: float | None = 0
    is_setup: bool | None = True
    description: str | None = ""
    node_reference: str | None = None  # Unique reference identifier for code generation (lowercase, no spaces)
    user_id: int | None = None
    is_flow_output: bool | None = False
    is_user_defined: bool | None = False  # Indicator if the node is a user defined node
    output_field_config: OutputFieldConfig | None = None

    @field_validator("node_reference", mode="before")
    @classmethod
    def validate_node_reference(cls, v):
        """Validates that node_reference is lowercase and contains no spaces."""
        if v is None or v == "":
            return None
        if not isinstance(v, str):
            raise ValueError("node_reference must be a string")
        if " " in v:
            raise ValueError("node_reference cannot contain spaces")
        if v != v.lower():
            raise ValueError("node_reference must be lowercase")
        return v


class NodeSingleInput(NodeBase):
    """A base model for any node that takes a single data input."""

    depending_on_id: int | None = -1


class NodeMultiInput(NodeBase):
    """A base model for any node that takes multiple data inputs."""

    depending_on_ids: list[int] | None = Field(default_factory=list)


class NodeSelect(NodeSingleInput):
    """Settings for a node that selects, renames, and reorders columns."""

    keep_missing: bool = True
    select_input: list[transform_schema.SelectInput] = Field(default_factory=list)
    sorted_by: Literal["none", "asc", "desc"] | None = "none"

    def to_yaml_dict(self) -> NodeSelectYaml:
        """Converts the select node settings to a dictionary for YAML serialization."""
        result: NodeSelectYaml = {
            "cache_results": bool(self.cache_results),
            "keep_missing": self.keep_missing,
            "select_input": [s.to_yaml_dict() for s in self.select_input],
            "sorted_by": self.sorted_by,
        }
        if self.output_field_config:
            result["output_field_config"] = {
                "enabled": self.output_field_config.enabled,
                "validation_mode_behavior": self.output_field_config.validation_mode_behavior,
                "validate_data_types": self.output_field_config.validate_data_types,
                "fields": [
                    {
                        "name": f.name,
                        "data_type": f.data_type,
                        "default_value": f.default_value,
                    }
                    for f in self.output_field_config.fields
                ],
            }
        return result


class NodeFilter(NodeSingleInput):
    """Settings for a node that filters rows based on a condition."""

    filter_input: transform_schema.FilterInput


class NodeSort(NodeSingleInput):
    """Settings for a node that sorts the data by one or more columns."""

    sort_input: list[transform_schema.SortByInput] = Field(default_factory=list)


class NodeTextToRows(NodeSingleInput):
    """Settings for a node that splits a text column into multiple rows."""

    text_to_rows_input: transform_schema.TextToRowsInput


class NodeSample(NodeSingleInput):
    """Settings for a node that samples a subset of the data."""

    sample_size: int = 1000


class NodeRecordId(NodeSingleInput):
    """Settings for a node that adds a unique record ID column."""

    record_id_input: transform_schema.RecordIdInput


class NodeJoin(NodeMultiInput):
    """Settings for a node that performs a standard SQL-style join."""

    auto_generate_selection: bool = True
    verify_integrity: bool = True
    join_input: transform_schema.JoinInput
    auto_keep_all: bool = True
    auto_keep_right: bool = True
    auto_keep_left: bool = True

    def to_yaml_dict(self) -> NodeJoinYaml:
        """Converts the join node settings to a dictionary for YAML serialization."""
        result: NodeJoinYaml = {
            "cache_results": self.cache_results,
            "auto_generate_selection": self.auto_generate_selection,
            "verify_integrity": self.verify_integrity,
            "join_input": self.join_input.to_yaml_dict(),
            "auto_keep_all": self.auto_keep_all,
            "auto_keep_right": self.auto_keep_right,
            "auto_keep_left": self.auto_keep_left,
        }
        if self.output_field_config:
            result["output_field_config"] = {
                "enabled": self.output_field_config.enabled,
                "validation_mode_behavior": self.output_field_config.validation_mode_behavior,
                "validate_data_types": self.output_field_config.validate_data_types,
                "fields": [
                    {
                        "name": f.name,
                        "data_type": f.data_type,
                        "default_value": f.default_value,
                    }
                    for f in self.output_field_config.fields
                ],
            }
        return result


class NodeCrossJoin(NodeMultiInput):
    """Settings for a node that performs a cross join."""

    auto_generate_selection: bool = True
    verify_integrity: bool = True
    cross_join_input: transform_schema.CrossJoinInput
    auto_keep_all: bool = True
    auto_keep_right: bool = True
    auto_keep_left: bool = True

    def to_yaml_dict(self) -> NodeCrossJoinYaml:
        """Converts the cross join node settings to a dictionary for YAML serialization."""
        result: NodeCrossJoinYaml = {
            "cache_results": self.cache_results,
            "auto_generate_selection": self.auto_generate_selection,
            "verify_integrity": self.verify_integrity,
            "cross_join_input": self.cross_join_input.to_yaml_dict(),
            "auto_keep_all": self.auto_keep_all,
            "auto_keep_right": self.auto_keep_right,
            "auto_keep_left": self.auto_keep_left,
        }
        if self.output_field_config:
            result["output_field_config"] = {
                "enabled": self.output_field_config.enabled,
                "validation_mode_behavior": self.output_field_config.validation_mode_behavior,
                "validate_data_types": self.output_field_config.validate_data_types,
                "fields": [
                    {
                        "name": f.name,
                        "data_type": f.data_type,
                        "default_value": f.default_value,
                    }
                    for f in self.output_field_config.fields
                ],
            }
        return result


class NodeFuzzyMatch(NodeJoin):
    """Settings for a node that performs a fuzzy join based on string similarity."""

    join_input: transform_schema.FuzzyMatchInput

    def to_yaml_dict(self) -> NodeFuzzyMatchYaml:
        """Converts the fuzzy match node settings to a dictionary for YAML serialization."""
        result: NodeFuzzyMatchYaml = {
            "cache_results": self.cache_results,
            "auto_generate_selection": self.auto_generate_selection,
            "verify_integrity": self.verify_integrity,
            "join_input": self.join_input.to_yaml_dict(),
            "auto_keep_all": self.auto_keep_all,
            "auto_keep_right": self.auto_keep_right,
            "auto_keep_left": self.auto_keep_left,
        }
        if self.output_field_config:
            result["output_field_config"] = {
                "enabled": self.output_field_config.enabled,
                "validation_mode_behavior": self.output_field_config.validation_mode_behavior,
                "validate_data_types": self.output_field_config.validate_data_types,
                "fields": [
                    {
                        "name": f.name,
                        "data_type": f.data_type,
                        "default_value": f.default_value,
                    }
                    for f in self.output_field_config.fields
                ],
            }
        return result


class NodeDatasource(NodeBase):
    """Base settings for a node that acts as a data source."""

    file_ref: str = None


class RawData(BaseModel):
    """Represents data in a raw, columnar format for manual input."""

    columns: list[MinimalFieldInfo] = None
    data: list[list]

    @classmethod
    def from_pylist(cls, pylist: list[dict]):
        """Creates a RawData object from a list of Python dictionaries."""
        if len(pylist) == 0:
            return cls(columns=[], data=[])
        pylist = ensure_similarity_dicts(pylist)
        values = [standardize_col_dtype([vv for vv in c]) for c in zip(*(r.values() for r in pylist), strict=False)]
        data_types = (pl.DataType.from_python(type(next((v for v in column_values), None))) for column_values in values)
        columns = [MinimalFieldInfo(name=c, data_type=str(next(data_types))) for c in pylist[0].keys()]
        return cls(columns=columns, data=values)

    @classmethod
    def from_pydict(cls, pydict: dict[str, list]):
        """Creates a RawData object from a dictionary of lists."""
        if len(pydict) == 0:
            return cls(columns=[], data=[])
        values = [standardize_col_dtype(column_values) for column_values in pydict.values()]
        data_types = (pl.DataType.from_python(type(next((v for v in column_values), None))) for column_values in values)
        columns = [MinimalFieldInfo(name=c, data_type=str(next(data_types))) for c in pydict.keys()]
        return cls(columns=columns, data=values)

    def to_pylist(self) -> list[dict]:
        """Converts the RawData object back into a list of Python dictionaries."""
        return [{c.name: self.data[ci][ri] for ci, c in enumerate(self.columns)} for ri in range(len(self.data[0]))]


class NodeManualInput(NodeBase):
    """Settings for a node that allows direct data entry in the UI."""

    raw_data_format: RawData | None = None


class NodeRead(NodeBase):
    """Settings for a node that reads data from a file."""

    received_file: ReceivedTable


class DatabaseConnection(BaseModel):
    """Defines the connection parameters for a database."""

    database_type: str = "postgresql"
    username: str | None = None
    password_ref: SecretRef | None = None
    host: str | None = None
    port: int | None = None
    database: str | None = None
    url: str | None = None


class FullDatabaseConnection(BaseModel):
    """A complete database connection model including the secret password."""

    connection_name: str
    database_type: str = "postgresql"
    username: str
    password: SecretStr
    host: str | None = None
    port: int | None = None
    database: str | None = None
    ssl_enabled: bool | None = False
    url: str | None = None


class FullDatabaseConnectionInterface(BaseModel):
    """A database connection model intended for UI display, omitting the password."""

    connection_name: str
    database_type: str = "postgresql"
    username: str
    host: str | None = None
    port: int | None = None
    database: str | None = None
    ssl_enabled: bool | None = False
    url: str | None = None


class DatabaseSettings(BaseModel):
    """Defines settings for reading from a database, either via table or query."""

    connection_mode: Literal["inline", "reference"] | None = "inline"
    database_connection: DatabaseConnection | None = None
    database_connection_name: str | None = None
    schema_name: str | None = None
    table_name: str | None = None
    query: str | None = None
    query_mode: Literal["query", "table", "reference"] = "table"

    @model_validator(mode="after")
    def validate_table_or_query(self):
        # Validate that either table_name or query is provided
        if (not self.table_name and not self.query) and self.query_mode == "inline":
            raise ValueError("Either 'table_name' or 'query' must be provided")

        # Validate correct connection information based on connection_mode
        if self.connection_mode == "inline" and self.database_connection is None:
            raise ValueError("When 'connection_mode' is 'inline', 'database_connection' must be provided")

        if self.connection_mode == "reference" and not self.database_connection_name:
            raise ValueError("When 'connection_mode' is 'reference', 'database_connection_name' must be provided")

        return self


class DatabaseWriteSettings(BaseModel):
    """Defines settings for writing data to a database table."""

    connection_mode: Literal["inline", "reference"] | None = "inline"
    database_connection: DatabaseConnection | None = None
    database_connection_name: str | None = None
    table_name: str
    schema_name: str | None = None
    if_exists: Literal["append", "replace", "fail"] | None = "append"


class NodeDatabaseReader(NodeBase):
    """Settings for a node that reads from a database."""

    database_settings: DatabaseSettings
    fields: list[MinimalFieldInfo] | None = None


class NodeDatabaseWriter(NodeSingleInput):
    """Settings for a node that writes data to a database."""

    database_write_settings: DatabaseWriteSettings


class NodeCloudStorageReader(NodeBase):
    """Settings for a node that reads from a cloud storage service (S3, GCS, etc.)."""

    cloud_storage_settings: CloudStorageReadSettings
    fields: list[MinimalFieldInfo] | None = None


class NodeCloudStorageWriter(NodeSingleInput):
    """Settings for a node that writes to a cloud storage service."""

    cloud_storage_settings: CloudStorageWriteSettings


class ExternalSource(BaseModel):
    """Base model for data coming from a predefined external source."""

    orientation: str = "row"
    fields: list[MinimalFieldInfo] | None = None


class SampleUsers(ExternalSource):
    """Settings for generating a sample dataset of users."""

    SAMPLE_USERS: bool
    class_name: str = "sample_users"
    size: int = 100


class NodeExternalSource(NodeBase):
    """Settings for a node that connects to a registered external data source."""

    identifier: str
    source_settings: SampleUsers


class NodeFormula(NodeSingleInput):
    """Settings for a node that applies a formula to create/modify a column."""

    function: transform_schema.FunctionInput = None


class NodeGroupBy(NodeSingleInput):
    """Settings for a node that performs a group-by and aggregation operation."""

    groupby_input: transform_schema.GroupByInput = None


class NodePromise(NodeBase):
    """A placeholder node for an operation that has not yet been configured."""

    is_setup: bool = False
    node_type: str


class NodeInputConnection(BaseModel):
    """Represents the input side of a connection between two nodes."""

    node_id: int
    connection_class: InputConnectionClass

    def get_node_input_connection_type(self) -> Literal["main", "right", "left"]:
        """Determines the semantic type of the input (e.g., for a join)."""
        match self.connection_class:
            case "input-0":
                return "main"
            case "input-1":
                return "right"
            case "input-2":
                return "left"
            case _:
                raise ValueError(f"Unexpected connection_class: {self.connection_class}")


class NodePivot(NodeSingleInput):
    """Settings for a node that pivots data from a long to a wide format."""

    pivot_input: transform_schema.PivotInput = None
    output_fields: list[MinimalFieldInfo] | None = None


class NodeUnpivot(NodeSingleInput):
    """Settings for a node that unpivots data from a wide to a long format."""

    unpivot_input: transform_schema.UnpivotInput = None


class NodeUnion(NodeMultiInput):
    """Settings for a node that concatenates multiple data inputs."""

    union_input: transform_schema.UnionInput = Field(default_factory=transform_schema.UnionInput)


class NodeOutput(NodeSingleInput):
    """Settings for a node that writes its input to a file."""

    output_settings: OutputSettings

    def to_yaml_dict(self) -> NodeOutputYaml:
        """Converts the output node settings to a dictionary for YAML serialization."""
        result: NodeOutputYaml = {
            "cache_results": self.cache_results,
            "output_settings": self.output_settings.to_yaml_dict(),
        }
        if self.output_field_config:
            result["output_field_config"] = {
                "enabled": self.output_field_config.enabled,
                "validation_mode_behavior": self.output_field_config.validation_mode_behavior,
                "validate_data_types": self.output_field_config.validate_data_types,
                "fields": [
                    {
                        "name": f.name,
                        "data_type": f.data_type,
                        "default_value": f.default_value,
                    }
                    for f in self.output_field_config.fields
                ],
            }
        return result


class NodeOutputConnection(BaseModel):
    """Represents the output side of a connection between two nodes."""

    node_id: int
    connection_class: OutputConnectionClass


class NodeConnection(BaseModel):
    """Represents a connection (edge) between two nodes in the graph."""

    input_connection: NodeInputConnection
    output_connection: NodeOutputConnection

    @classmethod
    def create_from_simple_input(cls, from_id: int, to_id: int, input_type: InputType = "input-0"):
        """Creates a standard connection between two nodes."""
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
        node_output = NodeOutputConnection(node_id=from_id, connection_class="output-0")
        return cls(input_connection=node_input, output_connection=node_output)


class NodeDescription(BaseModel):
    """A simple model for updating a node's description text."""

    description: str = ""


class NodeExploreData(NodeBase):
    """Settings for a node that provides an interactive data exploration interface."""

    graphic_walker_input: gs_schemas.GraphicWalkerInput | None = None


class NodeGraphSolver(NodeSingleInput):
    """Settings for a node that solves graph-based problems (e.g., connected components)."""

    graph_solver_input: transform_schema.GraphSolverInput


class NodeUnique(NodeSingleInput):
    """Settings for a node that returns the unique rows from the data."""

    unique_input: transform_schema.UniqueInput


class NodeRecordCount(NodeSingleInput):
    """Settings for a node that counts the number of records."""

    pass


class NodePolarsCode(NodeMultiInput):
    """Settings for a node that executes arbitrary user-provided Polars code."""

    polars_code_input: transform_schema.PolarsCodeInput


class NotebookCell(BaseModel):
    """A single cell in the notebook editor."""

    id: str
    code: str = ""


class PythonScriptInput(BaseModel):
    """Settings for Python code execution on a kernel."""

    code: str = ""
    kernel_id: str | None = None
    cells: list[NotebookCell] | None = None


class NodePythonScript(NodeMultiInput):
    """Node that executes Python code on a kernel container."""

    python_script_input: PythonScriptInput = PythonScriptInput()


class UserDefinedNode(NodeMultiInput):
    """Settings for a node that contains the user defined node information"""

    settings: Any
