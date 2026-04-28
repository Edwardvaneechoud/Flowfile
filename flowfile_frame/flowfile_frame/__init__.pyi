# This file mirrors __init__.py to provide type information for re-exported symbols.
# Hand-maintained — keep in sync with __init__.py when adding/removing exports.
from __future__ import annotations

from pl_fuzzy_frame_match.models import FuzzyMapping as FuzzyMapping
from polars.datatypes import (
    Array as Array,
    Binary as Binary,
    Boolean as Boolean,
    Categorical as Categorical,
    DataType as DataType,
    DataTypeClass as DataTypeClass,
    Date as Date,
    Datetime as Datetime,
    Decimal as Decimal,
    Duration as Duration,
    Enum as Enum,
    Field as Field,
    Float32 as Float32,
    Float64 as Float64,
    Int8 as Int8,
    Int16 as Int16,
    Int32 as Int32,
    Int64 as Int64,
    Int128 as Int128,
    IntegerType as IntegerType,
    List as List,
    Null as Null,
    Object as Object,
    String as String,
    Struct as Struct,
    TemporalType as TemporalType,
    Time as Time,
    UInt8 as UInt8,
    UInt16 as UInt16,
    UInt32 as UInt32,
    UInt64 as UInt64,
    Unknown as Unknown,
    Utf8 as Utf8,
)

from flowfile_frame.catalog import (
    read_catalog_sql as read_catalog_sql,
    read_catalog_table as read_catalog_table,
    write_catalog_table as write_catalog_table,
)
from flowfile_frame.cloud_storage.frame_helpers import (
    read_from_cloud_storage as read_from_cloud_storage,
    write_to_cloud_storage as write_to_cloud_storage,
)
from flowfile_frame.cloud_storage.secret_manager import (
    create_cloud_storage_connection as create_cloud_storage_connection,
    create_cloud_storage_connection_if_not_exists as create_cloud_storage_connection_if_not_exists,
    del_cloud_storage_connection as del_cloud_storage_connection,
    get_all_available_cloud_storage_connections as get_all_available_cloud_storage_connections,
)
from flowfile_frame.database import (
    create_database_connection as create_database_connection,
    create_database_connection_if_not_exists as create_database_connection_if_not_exists,
    del_database_connection as del_database_connection,
    get_all_available_database_connections as get_all_available_database_connections,
    get_database_connection_by_name as get_database_connection_by_name,
    read_database as read_database,
    write_database as write_database,
)
from flowfile_frame.expr import (
    col as col,
    column as column,
    corr as corr,
    count as count,
    cov as cov,
    cum_count as cum_count,
    first as first,
    implode as implode,
    last as last,
    len as len,
    lit as lit,
    max as max,
    mean as mean,
    min as min,
    sum as sum,
    when as when,
)
from flowfile_frame.flow_frame import FlowFrame as FlowFrame
from flowfile_frame.flow_frame_methods import (
    concat as concat,
    from_dict as from_dict,
    from_raw_data as from_raw_data,
    read_csv as read_csv,
    read_excel as read_excel,
    read_parquet as read_parquet,
    scan_csv as scan_csv,
    scan_csv_from_cloud_storage as scan_csv_from_cloud_storage,
    scan_delta as scan_delta,
    scan_json_from_cloud_storage as scan_json_from_cloud_storage,
    scan_parquet as scan_parquet,
    scan_parquet_from_cloud_storage as scan_parquet_from_cloud_storage,
)
from flowfile_frame.kafka import read_kafka as read_kafka
from flowfile_frame.lazy import fold as fold
from flowfile_frame.selectors import (
    all_ as all_,
    boolean as boolean,
    by_dtype as by_dtype,
    categorical as categorical,
    contains as contains,
    date as date,
    datetime as datetime,
    duration as duration,
    ends_with as ends_with,
    float_ as float_,
    integer as integer,
    list_ as list_,
    matches as matches,
    numeric as numeric,
    object_ as object_,
    starts_with as starts_with,
    string as string,
    struct as struct,
    temporal as temporal,
    time as time,
)
from flowfile_frame.series import Series as Series
from flowfile_frame.utils import create_flow_graph as create_flow_graph

# Compatibility aliases mirroring __init__.py
LazyFrame = FlowFrame
DataFrame = FlowFrame

__version__: str
