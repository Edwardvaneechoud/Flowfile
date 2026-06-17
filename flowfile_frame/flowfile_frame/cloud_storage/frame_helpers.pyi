# Auto-generated stub for flowfile_frame.cloud_storage.frame_helpers — do not edit.
# Run `make stubs` to regenerate from the Python source.
from __future__ import annotations

from typing import Literal
from polars._typing import CsvEncoding
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import input_schema
from flowfile_frame.flow_frame import FlowFrame

def read_from_cloud_storage(source: str, *, file_format: Literal['csv', 'parquet', 'json', 'delta']='parquet', connection_name: str | None=None, scan_mode: Literal['single_file', 'directory'] | None=None, delimiter: str=';', has_header: bool=True, encoding: str='utf8', delta_version: int | None=None, output_field_config: input_schema.OutputFieldConfig | None=None) -> FlowFrame: ...
def write_to_cloud_storage(df: FlowFrame, path: str, *, file_format: Literal['csv', 'parquet', 'json', 'delta']='parquet', connection_name: str | None=None, delimiter: str=';', encoding: CsvEncoding='utf8', compression: Literal['snappy', 'gzip', 'brotli', 'lz4', 'zstd']='snappy', write_mode: Literal['overwrite', 'append']='overwrite', partition_by: list[str] | None=None) -> None: ...
def add_write_ff_to_cloud_storage(path: str, flow_graph: FlowGraph | None, depends_on_node_id: int, *, connection_name: str | None=None, write_mode: Literal['overwrite', 'append']='overwrite', file_format: Literal['csv', 'parquet', 'json', 'delta']='parquet', csv_delimiter: str=';', csv_encoding: CsvEncoding='utf8', parquet_compression: Literal['snappy', 'gzip', 'brotli', 'lz4', 'zstd']='snappy', partition_by: list[str] | None=None, description: str | None=None) -> int: ...
