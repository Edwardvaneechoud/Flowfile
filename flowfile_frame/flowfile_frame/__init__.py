# flowframe/__init__.py
"""A Polars-like API for building ETL graphs."""

from importlib.metadata import PackageNotFoundError, version

from pl_fuzzy_frame_match.models import FuzzyMapping  # noqa: F401
from polars.datatypes import (  # noqa: F401
    Array,
    Binary,
    # Other primitive types
    Boolean,
    # Special types
    Categorical,
    # Type classes
    DataType,
    DataTypeClass,
    # Date/time types
    Date,
    Datetime,
    Decimal,
    Duration,
    Enum,
    Field,
    # Float types
    Float32,
    Float64,
    # Integer types
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    IntegerType,
    # Complex types
    List,
    Null,
    Object,
    String,
    Struct,
    TemporalType,
    Time,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Unknown,
    Utf8,
)

from flowfile_frame.cloud_storage.secret_manager import (
    create_cloud_storage_connection,
    create_cloud_storage_connection_if_not_exists,
    del_cloud_storage_connection,
    get_all_available_cloud_storage_connections,
)

# Commonly used functions
from flowfile_frame.expr import (  # noqa: F401
    col,
    column,
    corr,
    count,
    cov,
    cum_count,
    first,
    implode,
    last,
    len,
    lit,
    max,
    mean,
    min,
    sum,
    when,
)

# Core classes
from flowfile_frame.flow_frame import FlowFrame  # noqa: F401

# File I/O
from flowfile_frame.flow_frame_methods import (  # noqa: F401
    concat,
    from_dict,
    read_csv,
    read_parquet,
    scan_csv,
    scan_csv_from_cloud_storage,
    scan_delta,
    scan_json_from_cloud_storage,
    scan_parquet,
    scan_parquet_from_cloud_storage,
)
from flowfile_frame.lazy import fold

# Selector utilities
from flowfile_frame.selectors import (  # noqa: F401
    all_,
    boolean,
    by_dtype,
    categorical,
    contains,
    date,
    datetime,
    duration,
    ends_with,
    float_,
    integer,
    list_,
    matches,
    numeric,
    object_,
    starts_with,
    string,
    struct,
    temporal,
    time,
)
from flowfile_frame.series import Series
from flowfile_frame.utils import create_flow_graph  # noqa: F401

try:
    __version__ = version("Flowfile")
except PackageNotFoundError:
    __version__ = "0.5.0"
