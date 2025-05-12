"""
FlowFile: A framework combining visual ETL with a Polars-like API.

This package ties together the FlowFile ecosystem components:
- flowfile_core: Core ETL functionality
- flowfile_frame: Polars-like DataFrame API
- flowfile_worker: Computation engine
"""

__version__ = "0.3.1"

import os
import logging

os.environ['WORKER_PORT'] = "63578"
os.environ['SINGLE_FILE_MODE'] = "1"

from flowfile.web import start_server as start_web_ui
from flowfile.api import open_graph_in_editor
from flowfile_frame.flow_frame import (
    FlowFrame, read_csv, read_parquet, from_dict, concat
)
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

__all__ = [
    # Core FlowFrame classes
    'FlowFrame', 'GroupByFrame',

    # Main creation functions
    'read_csv', 'read_parquet', 'from_dict', 'concat',

    # Expression API
    'col', 'lit', 'column', 'cum_count', 'len',
    'sum', 'min', 'max', 'mean', 'count', 'when',

    # Selector utilities
    'numeric', 'float_', 'integer', 'string', 'temporal',
    'datetime', 'date', 'time', 'duration', 'boolean',
    'categorical', 'object_', 'list_', 'struct', 'all_',
    'by_dtype', 'contains', 'starts_with', 'ends_with', 'matches',

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
