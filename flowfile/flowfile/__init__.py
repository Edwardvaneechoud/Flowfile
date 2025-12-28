"""
FlowFile: A framework combining visual ETL with a Polars-like API.

This package ties together the FlowFile ecosystem components:
- flowfile_core: Core ETL functionality
- flowfile_frame: Polars-like DataFrame API
- flowfile_worker: Computation engine
"""

from importlib.metadata import version

__version__ = version("Flowfile")

import os
import logging

os.environ['FLOWFILE_WORKER_PORT'] = "63578"
os.environ['FLOWFILE_SINGLE_FILE_MODE'] = "1"

from flowfile.web import start_server as start_web_ui
from flowfile.api import (open_graph_in_editor)
from flowfile_frame.flow_frame import (
    FlowFrame
)
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.schemas import input_schema as node_interface
from flowfile_core.schemas.schemas import FlowSettings, FlowInformation
from flowfile_core.schemas import transform_schema
from flowfile_frame import (read_csv, read_parquet, from_dict, concat, scan_csv, scan_parquet,
                            scan_delta,
                            scan_parquet_from_cloud_storage,
                            scan_json_from_cloud_storage,
                            scan_csv_from_cloud_storage,
                            get_all_available_cloud_storage_connections,
                            create_cloud_storage_connection,
                            del_cloud_storage_connection,
                            create_cloud_storage_connection_if_not_exists,
                            FuzzyMapping)
from flowfile_frame.expr import (
    col, lit, column, cum_count, len,
    sum, min, max, mean, count, when
)
from flowfile_frame.group_frame import GroupByFrame
from flowfile_frame.utils import create_flow_graph
from flowfile_frame.selectors import (
    numeric, float_, integer, string, temporal,
    datetime, date, time, duration, boolean,
    categorical, object_, list_, struct, all_,
    by_dtype, contains, starts_with, ends_with, matches
)

from polars.datatypes import (
    Int8, Int16, Int32, Int64, Int128,
    UInt8, UInt16, UInt32, UInt64,
    Float32, Float64,
    Boolean, String, Utf8, Binary, Null,
    List, Array, Struct, Object,
    Date, Time, Datetime, Duration,
    Categorical, Decimal, Enum, Unknown,
    DataType, DataTypeClass, Field
)

from flowfile_core.flowfile import node_designer


__all__ = [
    # Core FlowFrame classes
    'FlowFrame', 'GroupByFrame', 'FullCloudStorageConnection',

    # Main creation functions
    'read_csv', 'read_parquet', 'from_dict', 'concat', 'scan_csv', 'scan_parquet', 'scan_delta',
    'scan_parquet_from_cloud_storage', 'scan_json_from_cloud_storage',
    'scan_csv_from_cloud_storage', 'get_all_available_cloud_storage_connections', 'create_cloud_storage_connection',
    'del_cloud_storage_connection', 'create_cloud_storage_connection_if_not_exists',
    'FlowGraph', 'FlowDataEngine', 'node_interface', 'FlowSettings', 'transform_schema',
    'FlowNode', 'FlowfileColumn', 'FlowInformation', "FuzzyMapping",

    # Expression API
    'col', 'lit', 'column', 'cum_count', 'len',
    'sum', 'min', 'max', 'mean', 'count', 'when',

    # Selector utilities
    'numeric', 'float_', 'integer', 'string', 'temporal',
    'datetime', 'date', 'time', 'duration', 'boolean',
    'categorical', 'object_', 'list_', 'struct', 'all_',
    'by_dtype', 'contains', 'starts_with', 'ends_with', 'matches',

    "node_designer",

    # Utilities
    'create_flow_graph', 'open_graph_in_editor',

    # Data types from Polars
    'Int8', 'Int16', 'Int32', 'Int64', 'Int128',
    'UInt8', 'UInt16', 'UInt32', 'UInt64',
    'Float32', 'Float64',
    'Boolean', 'String', 'Utf8', 'Binary', 'Null',
    'List', 'Array', 'Struct', 'Object',
    'Date', 'Time', 'Datetime', 'Duration',
    'Categorical', 'Decimal', 'Enum', 'Unknown',
    'DataType', 'DataTypeClass', 'Field',
    'start_web_ui'
]
logging.getLogger("PipelineHandler").setLevel(logging.WARNING)
