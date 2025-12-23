from pydantic import BaseModel, Field, model_validator, field_validator
from typing import List, Optional, Literal, Annotated
import os
from pathlib import Path


class MinimalFieldInfo(BaseModel):
    name: str
    data_type: str


class InputTableBase(BaseModel):
    """Base settings for input file operations."""
    file_type: str  # Will be overridden with Literal in subclasses


class InputCsvTable(InputTableBase):
    """Defines settings for reading a CSV file."""
    file_type: Literal['csv'] = 'csv'
    reference: str = ''
    starting_from_line: int = 0
    delimiter: str = ','
    has_headers: bool = True
    encoding: str = 'utf-8'
    parquet_ref: Optional[str] = None
    row_delimiter: str = '\n'
    quote_char: str = '"'
    infer_schema_length: int = 10_000
    truncate_ragged_lines: bool = False
    ignore_errors: bool = False


class InputJsonTable(InputCsvTable):
    """Defines settings for reading a JSON file."""
    file_type: Literal['json'] = 'json'


class InputParquetTable(InputTableBase):
    """Defines settings for reading a Parquet file."""
    file_type: Literal['parquet'] = 'parquet'


class InputExcelTable(InputTableBase):
    """Defines settings for reading an Excel file."""
    file_type: Literal['excel'] = 'excel'
    sheet_name: Optional[str] = None
    start_row: int = 0
    start_column: int = 0
    end_row: int = 0
    end_column: int = 0
    has_headers: bool = True
    type_inference: bool = False

    @model_validator(mode='after')
    def validate_range_values(self):
        """Validates that the Excel cell range is logical."""
        for attribute in [self.start_row, self.start_column, self.end_row, self.end_column]:
            if not isinstance(attribute, int) or attribute < 0:
                raise ValueError("Row and column indices must be non-negative integers")
        if (self.end_row > 0 and self.start_row > self.end_row) or \
                (self.end_column > 0 and self.start_column > self.end_column):
            raise ValueError("Start row/column must not be greater than end row/column")
        return self


# Create the discriminated union (similar to OutputTableSettings)
InputTableSettings = Annotated[
    InputCsvTable | InputJsonTable | InputParquetTable | InputExcelTable,
    Field(discriminator='file_type')
]


# Now create the main ReceivedTable model
class ReceivedTable(BaseModel):
    """Model for defining a table received from an external source."""
    # Metadata fields
    id: Optional[int] = None
    name: Optional[str] = None
    path: str  # This can be an absolute or relative path
    directory: Optional[str] = None
    analysis_file_available: bool = False
    status: Optional[str] = None
    fields: List[MinimalFieldInfo] = Field(default_factory=list)
    abs_file_path: Optional[str] = None

    file_type: Literal['csv', 'json', 'parquet', 'excel']

    table_settings: InputTableSettings

    @classmethod
    def create_from_path(cls, path: str, file_type: Literal['csv', 'json', 'parquet', 'excel'] = 'csv'):
        """Creates an instance from a file path string."""
        filename = Path(path).name

        # Create appropriate table_settings based on file_type
        settings_map = {
            'csv': InputCsvTable(),
            'json': InputJsonTable(),
            'parquet': InputParquetTable(),
            'excel': InputExcelTable(),
        }

        return cls(
            name=filename,
            path=path,
            file_type=file_type,
            table_settings=settings_map.get(file_type, InputCsvTable())
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

    @field_validator('table_settings', mode='before')
    @classmethod
    def validate_table_settings(cls, v, info):
        """Ensures table_settings matches the file_type."""
        if v is None:
            file_type = info.data.get('file_type', 'csv')
            # Create default based on file_type
            settings_map = {
                'csv': InputCsvTable(),
                'json': InputJsonTable(),
                'parquet': InputParquetTable(),
                'excel': InputExcelTable(),
            }
            return settings_map.get(file_type, InputCsvTable())

        # If it's a dict, add file_type if missing
        if isinstance(v, dict) and 'file_type' not in v:
            v['file_type'] = info.data.get('file_type', 'csv')

        return v

    @model_validator(mode='after')
    def populate_abs_file_path(self):
        """Ensures the absolute file path is populated after validation."""
        if not self.abs_file_path:
            self.set_absolute_filepath()
        return self

