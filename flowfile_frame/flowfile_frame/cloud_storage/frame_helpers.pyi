# This file was auto-generated to provide type information for flowfile_frame.cloud_storage.frame_helpers
# DO NOT MODIFY THIS FILE MANUALLY
# Run `python flowfile_frame/submodule_stub_generator.py` to regenerate
from __future__ import annotations

from typing import Any, Callable, Iterable, Optional, Union

from typing import TYPE_CHECKING, Literal
from polars._typing import CsvEncoding
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import cloud_storage_schemas, input_schema
from flowfile_frame.cloud_storage.secret_manager import get_current_user_id
from flowfile_frame.utils import generate_node_id
from flowfile_frame.flow_frame import FlowFrame
from flowfile_frame.flow_frame_methods import scan_csv_from_cloud_storage, scan_delta, scan_json_from_cloud_storage, scan_parquet_from_cloud_storage

def add_write_ff_to_cloud_storage(path: str, flow_graph: FlowGraph | None, depends_on_node_id: int, connection_name: str | None=None, write_mode: Literal['overwrite', 'append']='overwrite', file_format: Literal['csv', 'parquet', 'json', 'delta']='parquet', csv_delimiter: str=';', csv_encoding: CsvEncoding='utf8', parquet_compression: Literal['snappy', 'gzip', 'brotli', 'lz4', 'zstd']='snappy', description: str | None=None) -> int: ...

def read_from_cloud_storage(source: str, file_format: Literal['csv', 'parquet', 'json', 'delta']='parquet', connection_name: str | None=None, scan_mode: Literal['single_file', 'directory'] | None=None, delimiter: str=';', has_header: bool=True, encoding: str='utf8', delta_version: int | None=None) -> FlowFrame: ...

def write_to_cloud_storage(df: FlowFrame, path: str, file_format: Literal['csv', 'parquet', 'json', 'delta']='parquet', connection_name: str | None=None, delimiter: str=';', encoding: CsvEncoding='utf8', compression: Literal['snappy', 'gzip', 'brotli', 'lz4', 'zstd']='snappy', write_mode: Literal['overwrite', 'append']='overwrite') -> None: ...

