import os
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from shared.path_utils import is_url


class MinimalFieldInfo(BaseModel):
    name: str
    data_type: str


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


class InputIpcTable(InputTableBase):
    """Defines settings for reading an Arrow IPC/Feather file."""

    file_type: Literal["ipc"] = "ipc"


class InputNdjsonTable(InputTableBase):
    """Defines settings for reading a newline-delimited JSON file."""

    file_type: Literal["ndjson"] = "ndjson"


class InputAvroTable(InputTableBase):
    """Defines settings for reading an Avro file."""

    file_type: Literal["avro"] = "avro"


InputTableSettings = Annotated[
    InputCsvTable
    | InputJsonTable
    | InputParquetTable
    | InputExcelTable
    | InputIpcTable
    | InputNdjsonTable
    | InputAvroTable,
    Field(discriminator="file_type"),
]


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

    file_type: Literal["csv", "json", "parquet", "excel", "ipc", "ndjson", "avro"]

    table_settings: InputTableSettings

    @classmethod
    def create_from_path(
        cls, path: str, file_type: Literal["csv", "json", "parquet", "excel", "ipc", "ndjson", "avro"] = "csv"
    ):
        """Creates an instance from a file path string."""
        filename = Path(path).name

        settings_map = {
            "csv": InputCsvTable(),
            "json": InputJsonTable(),
            "parquet": InputParquetTable(),
            "excel": InputExcelTable(),
            "ipc": InputIpcTable(),
            "ndjson": InputNdjsonTable(),
            "avro": InputAvroTable(),
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
        if is_url(self.path):
            self.abs_file_path = self.path
            return
        base_path = Path(self.path).expanduser()
        if not base_path.is_absolute():
            base_path = Path.cwd() / base_path
        if self.name and self.name not in base_path.name:
            base_path = base_path / self.name
        self.abs_file_path = str(base_path.resolve())

    @field_validator("table_settings", mode="before")
    @classmethod
    def validate_table_settings(cls, v, info):
        """Ensures table_settings matches the file_type."""
        if v is None:
            file_type = info.data.get("file_type", "csv")
            settings_map = {
                "csv": InputCsvTable(),
                "json": InputJsonTable(),
                "parquet": InputParquetTable(),
                "excel": InputExcelTable(),
                "ipc": InputIpcTable(),
                "ndjson": InputNdjsonTable(),
                "avro": InputAvroTable(),
            }
            return settings_map.get(file_type, InputCsvTable())

        if isinstance(v, dict) and "file_type" not in v:
            v["file_type"] = info.data.get("file_type", "csv")

        return v

    @model_validator(mode="after")
    def populate_abs_file_path(self):
        """Ensures the absolute file path is populated after validation."""
        if not self.abs_file_path:
            self.set_absolute_filepath()
        return self
