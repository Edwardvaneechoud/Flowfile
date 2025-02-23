from pydantic import BaseModel, Field, model_validator
from typing import List, Optional
import os
from pathlib import Path


class MinimalFieldInfo(BaseModel):
    name: str
    data_type: str


class ReceivedTableBase(BaseModel):
    id: Optional[int] = None
    name: str
    path: str
    directory: Optional[str] = None
    analysis_file_available: Optional[bool] = False
    status: Optional[str] = None
    file_type: Optional[str] = None
    fields: List[MinimalFieldInfo] = Field(default_factory=list)
    abs_file_path: Optional[str] = None

    @classmethod
    def create_from_path(cls, path: str):
        filename = os.path.basename(path)
        return cls(name=filename, path=path)

    @property
    def file_path(self) -> str:
        if self.name not in self.path:
            return os.path.join(self.path, self.name)
        return self.path

    @model_validator(mode="after")
    def set_abs_file_path(cls, values):
        abs_file_path = getattr(values, "abs_file_path", None)
        if abs_file_path is None:
            path = getattr(values, "path", None)
            if not path:
                raise ValueError("Field 'path' is required to compute abs_file_path")
            setattr(values, "abs_file_path", str(Path(path).absolute()))
        return values


class ReceivedCsvTable(ReceivedTableBase):
    file_type: Optional[str] = 'csv'
    reference: Optional[str] = ''
    starting_from_line: Optional[int] = 0
    delimiter: Optional[str] = ','
    has_headers: Optional[bool] = True
    encoding: Optional[str] = 'utf-8'
    parquet_ref: Optional[str] = None
    row_delimiter: Optional[str] = '\n'
    quote_char: Optional[str] = '"'
    infer_schema_length: Optional[int] = 10_000
    truncate_ragged_lines: Optional[bool] = False
    ignore_errors: Optional[bool] = False


class ReceivedJsonTable(ReceivedCsvTable):
    pass


class ReceivedParquetTable(ReceivedTableBase):
    file_type: Optional[str] = 'parquet'


class ReceivedExcelTable(ReceivedTableBase):
    sheet_name: Optional[str] = None
    start_row: Optional[int] = 0  # optional
    start_column: Optional[int] = 0  # optional
    end_row: Optional[int] = 0  # optional
    end_column: Optional[int] = 0  # optional
    has_headers: Optional[bool] = True  # optional
    type_inference: Optional[bool] = False  # optional

    def validate_range_values(self):
        # Validate that start and end rows/columns are non-negative integers
        for attribute in [self.start_row, self.start_column, self.end_row, self.end_column]:
            if not isinstance(attribute, int) or attribute < 0:
                raise ValueError("Row and column indices must be non-negative integers")

        # Validate that start is before end if end is specified (non-zero)
        if (0 < self.end_row < self.start_row) or \
                (0 < self.end_column < self.start_column):
            raise ValueError("Start row/column must not be greater than end row/column if specified")
