# flowframe/__init__.py
"""A Polars-like API for building ETL graphs."""

# Core classes
from flowfile_frame.flow_frame import FlowFrame   # noqa: F401

from flowfile_frame.utils import create_flow_graph  # noqa: F401

# Commonly used functions
from flowfile_frame.expr import (  # noqa: F401
    col, lit, column,
    cum_count, len,
    sum, min, max, mean, count, when
)

# Selector utilities
from flowfile_frame.selectors import (  # noqa: F401
    numeric, float_, integer, string, temporal,
    datetime, date, time, duration, boolean,
    categorical, object_, list_, struct, all_,
    by_dtype, contains, starts_with, ends_with, matches
)

# File I/O
from flowfile_frame.flow_frame import (  # noqa: F401
    read_csv, read_parquet, from_dict, concat
)

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

__version__ = "0.1.0"
