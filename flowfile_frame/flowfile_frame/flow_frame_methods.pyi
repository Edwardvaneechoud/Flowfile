# This file was auto-generated to provide type information for flowfile_frame.flow_frame_methods
# DO NOT MODIFY THIS FILE MANUALLY
# Run `python flowfile_frame/submodule_stub_generator.py` to regenerate
from __future__ import annotations

from typing import Any, Callable, Iterable, Optional, Union

import io
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal
import polars as pl
from polars._typing import IO, CsvEncoding, PolarsDataType, SchemaDict, Sequence
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import cloud_storage_schemas, input_schema, transform_schema
from flowfile_frame.cloud_storage.secret_manager import get_current_user_id
from flowfile_frame.config import logger
from flowfile_frame.expr import col
from flowfile_frame.flow_frame import FlowFrame
from flowfile_frame.utils import create_flow_graph, generate_node_id

def concat(frames: list['FlowFrame'], how: str='vertical', rechunk: bool=False, parallel: bool=True, description: str=None) -> FlowFrame: ...

def count(expr) -> Any: ...

def from_dict(data, flow_graph: FlowGraph=None, description: str=None) -> FlowFrame: ...

def from_raw_data(raw_data: RawData, flow_graph: FlowGraph=None, description: str=None) -> FlowFrame: ...

def max(expr) -> Any: ...

def mean(expr) -> Any: ...

def min(expr) -> Any: ...

def read_csv(source: Union[str, Path, IO[bytes], bytes, list[Union[str, Path, IO[bytes], bytes]]], flow_graph: Any | None=None, separator: str=',', convert_to_absolute_path: bool=True, description: str | None=None, has_header: bool=True, new_columns: list[str] | None=None, comment_prefix: str | None=None, quote_char: str | None='"', skip_rows: int=0, skip_lines: int=0, schema: Mapping[str, Union[ForwardRef('DataTypeClass'), ForwardRef('DataType')]] | None=None, schema_overrides: Mapping[str, Union[ForwardRef('DataTypeClass'), ForwardRef('DataType')]] | Sequence[Union[ForwardRef('DataTypeClass'), ForwardRef('DataType')]] | None=None, null_values: str | list[str] | dict[str, str] | None=None, missing_utf8_is_empty_string: bool=False, ignore_errors: bool=False, try_parse_dates: bool=False, infer_schema: bool=True, infer_schema_length: int | None=100, n_rows: int | None=None, encoding: Literal['utf8', 'utf8-lossy']='utf8', low_memory: bool=False, rechunk: bool=False, storage_options: dict[str, Any] | None=None, skip_rows_after_header: int=0, row_index_name: str | None=None, row_index_offset: int=0, eol_char: str='\n', raise_if_empty: bool=True, truncate_ragged_lines: bool=False, decimal_comma: bool=False, glob: bool=True, cache: bool=True, with_column_names: Callable[[list[str]], list[str]] | None=None, **other_options) -> FlowFrame: ...

def read_excel(source, sheet_name: str | None=None, has_header: bool=True, flow_graph: FlowGraph=None, description: str=None, convert_to_absolute_path: bool=True) -> FlowFrame: ...

def read_parquet(source, flow_graph: FlowGraph=None, description: str=None, convert_to_absolute_path: bool=True, **options) -> FlowFrame: ...

def scan_csv(source: Union[str, Path, IO[bytes], bytes, list[Union[str, Path, IO[bytes], bytes]]], flow_graph: Any | None=None, separator: str=',', convert_to_absolute_path: bool=True, description: str | None=None, has_header: bool=True, new_columns: list[str] | None=None, comment_prefix: str | None=None, quote_char: str | None='"', skip_rows: int=0, skip_lines: int=0, schema: Mapping[str, Union[ForwardRef('DataTypeClass'), ForwardRef('DataType')]] | None=None, schema_overrides: Mapping[str, Union[ForwardRef('DataTypeClass'), ForwardRef('DataType')]] | Sequence[Union[ForwardRef('DataTypeClass'), ForwardRef('DataType')]] | None=None, null_values: str | list[str] | dict[str, str] | None=None, missing_utf8_is_empty_string: bool=False, ignore_errors: bool=False, try_parse_dates: bool=False, infer_schema: bool=True, infer_schema_length: int | None=100, n_rows: int | None=None, encoding: Literal['utf8', 'utf8-lossy']='utf8', low_memory: bool=False, rechunk: bool=False, storage_options: dict[str, Any] | None=None, skip_rows_after_header: int=0, row_index_name: str | None=None, row_index_offset: int=0, eol_char: str='\n', raise_if_empty: bool=True, truncate_ragged_lines: bool=False, decimal_comma: bool=False, glob: bool=True, cache: bool=True, with_column_names: Callable[[list[str]], list[str]] | None=None, **other_options) -> FlowFrame: ...

def scan_csv_from_cloud_storage(source: str, flow_graph: flowfile_core.flowfile.flow_graph.FlowGraph | None=None, connection_name: str | None=None, scan_mode: Literal['single_file', 'directory', None]=None, delimiter: str=';', has_header: bool | None=True, encoding: Union[Literal['utf8', 'utf8-lossy'], None]='utf8') -> FlowFrame: ...

def scan_delta(source: str, flow_graph: flowfile_core.flowfile.flow_graph.FlowGraph | None=None, connection_name: str | None=None, version: int=None) -> FlowFrame: ...

def scan_json_from_cloud_storage(source: str, flow_graph: flowfile_core.flowfile.flow_graph.FlowGraph | None=None, connection_name: str | None=None, scan_mode: Literal['single_file', 'directory', None]=None) -> FlowFrame: ...

def scan_parquet(source, flow_graph: FlowGraph=None, description: str=None, convert_to_absolute_path: bool=True, **options) -> FlowFrame: ...

def scan_parquet_from_cloud_storage(source: str, flow_graph: flowfile_core.flowfile.flow_graph.FlowGraph | None=None, connection_name: str | None=None, scan_mode: Literal['single_file', 'directory', None]=None, description: str | None=None) -> FlowFrame: ...

def sum(expr) -> Any: ...

