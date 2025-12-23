# flowframe/__init__.py
"""A Polars-like API for building ETL graphs."""

from importlib.metadata import version

# Core classes
from flowfile_frame.flow_frame import FlowFrame   # noqa: F401
from pl_fuzzy_frame_match.models import FuzzyMapping  # noqa: F401

from flowfile_frame.utils import create_flow_graph  # noqa: F401

# Commonly used functions
from flowfile_frame.expr import (  # noqa: F401
    col, lit, column,
    cum_count, len,
    sum, min, max, mean, count, when, implode, last, corr, cov, first
)

from flowfile_frame.lazy import (fold)

# Selector utilities
from flowfile_frame.selectors import (  # noqa: F401
    numeric, float_, integer, string, temporal,
    datetime, date, time, duration, boolean,
    categorical, object_, list_, struct, all_,
    by_dtype, contains, starts_with, ends_with, matches
)

from flowfile_frame.series import Series

# File I/O
from flowfile_frame.flow_frame_methods import (  # noqa: F401
    read_csv, read_parquet, from_dict, concat,  scan_csv, scan_parquet, scan_json_from_cloud_storage,
    scan_parquet_from_cloud_storage,
    scan_csv_from_cloud_storage,
    scan_delta)

from flowfile_frame.cloud_storage.secret_manager import (del_cloud_storage_connection,
                                                         create_cloud_storage_connection,
                                                         get_all_available_cloud_storage_connections,
                                                         create_cloud_storage_connection_if_not_exists)

from polars.datatypes import (  # noqa: F401
    # Integer types
    Int8, Int16, Int32, Int64, Int128,
    UInt8, UInt16, UInt32, UInt64,
    IntegerType,

    # Float types
    Float32, Float64,

    # Other primitive types
    Boolean, String, Utf8, Binary, Null,

    # Complex types
    List, Array, Struct, Object,

    # Date/time types
    Date, Time, Datetime, Duration,
    TemporalType,

    # Special types
    Categorical, Decimal, Enum, Unknown,

    # Type classes
    DataType, DataTypeClass, Field
)

__version__ = version("Flowfile")