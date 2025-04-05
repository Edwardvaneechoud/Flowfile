# Comprehensive mapping from SQLAlchemy types to Polars types
from typing import Dict, Type, Union, cast, TYPE_CHECKING, Any
from pydantic import SecretStr

import polars as pl
from polars import DataType as PolarsType
from sqlalchemy.sql.sqltypes import (
    _Binary, ARRAY, BIGINT, BigInteger, BINARY, BLOB, BOOLEAN, Boolean,
    CHAR, CLOB, Concatenable, DATE, Date, DATETIME, DateTime,
    DECIMAL, DOUBLE, Double, DOUBLE_PRECISION, Enum, FLOAT, Float,
    Indexable, INT, INTEGER, Integer, Interval, JSON, LargeBinary,
    MatchType, NCHAR, NULLTYPE, NullType, NUMERIC, Numeric, NVARCHAR,
    PickleType, REAL, SchemaType, SMALLINT, SmallInteger, String,
    STRINGTYPE, TEXT, Text, TIME, Time, TIMESTAMP, TupleType,
    Unicode, UnicodeText, UUID, Uuid, VARBINARY, VARCHAR
)
from sqlalchemy.sql.type_api import (
    ExternalType, TypeDecorator,
    TypeEngine, UserDefinedType, Variant
)


from typing import Optional
from urllib.parse import quote_plus


if TYPE_CHECKING:
    SqlType = Union[
        Type[_Binary], Type[ARRAY], Type[BIGINT], Type[BigInteger], Type[BINARY],
        Type[BLOB], Type[BOOLEAN], Type[Boolean], Type[CHAR], Type[CLOB],
        Type[Concatenable], Type[DATE], Type[Date], Type[DATETIME], Type[DateTime],
        Type[DECIMAL], Type[DOUBLE], Type[Double], Type[DOUBLE_PRECISION], Type[Enum],
        Type[FLOAT], Type[Float], Type[Indexable], Type[INT], Type[INTEGER],
        Type[Integer], Type[Interval], Type[JSON], Type[LargeBinary], Type[MatchType],
        Type[NCHAR], Type[NULLTYPE], Type[NullType], Type[NUMERIC], Type[Numeric],
        Type[NVARCHAR], Type[PickleType], Type[REAL], Type[SchemaType], Type[SMALLINT],
        Type[SmallInteger], Type[String], Type[STRINGTYPE], Type[TEXT], Type[Text],
        Type[TIME], Type[Time], Type[TIMESTAMP], Type[TupleType], Type[Unicode],
        Type[UnicodeText], Type[UUID], Type[Uuid], Type[VARBINARY], Type[VARCHAR],
        Type[TypeDecorator], Type[TypeEngine], Type[UserDefinedType], Type[Variant],
        Type[ExternalType]
    ]
else:
    SqlType = Any


# Comprehensive mapping from SQLAlchemy types to Polars types
sqlalchemy_to_polars: Dict[SqlType, PolarsType] = {
    # Numeric types
    Integer: pl.Int64,
    INTEGER: pl.Int64,
    INT: pl.Int64,
    SmallInteger: pl.Int16,
    SMALLINT: pl.Int16,
    BigInteger: pl.Int64,
    BIGINT: pl.Int64,
    Float: pl.Float64,
    FLOAT: pl.Float64,
    REAL: pl.Float32,
    DOUBLE: pl.Float64,
    Double: pl.Float64,
    DOUBLE_PRECISION: pl.Float64,
    Numeric: pl.Decimal,
    NUMERIC: pl.Decimal,
    DECIMAL: pl.Decimal,
    Boolean: pl.Boolean,
    BOOLEAN: pl.Boolean,

    # String types
    String: pl.Utf8,
    VARCHAR: pl.Utf8,
    CHAR: pl.Utf8,
    NVARCHAR: pl.Utf8,
    NCHAR: pl.Utf8,
    Text: pl.Utf8,
    TEXT: pl.Utf8,
    CLOB: pl.Utf8,
    STRINGTYPE: pl.Utf8,
    Unicode: pl.Utf8,
    UnicodeText: pl.Utf8,

    # Date/Time types
    Date: pl.Date,
    DATE: pl.Date,
    DateTime: pl.Datetime,
    DATETIME: pl.Datetime,
    TIMESTAMP: pl.Datetime,
    Time: pl.Time,
    TIME: pl.Time,
    Interval: pl.Duration,

    # Binary types
    _Binary: pl.Binary,
    LargeBinary: pl.Binary,
    BINARY: pl.Binary,
    VARBINARY: pl.Binary,
    BLOB: pl.Binary,

    # JSON types
    JSON: pl.Utf8,  # Polars doesn't have a dedicated JSON type, using Utf8

    # UUID types
    UUID: pl.Utf8,  # Mapped to string
    Uuid: pl.Utf8,  # Mapped to string

    # Other types
    ARRAY: pl.List,  # Approx mapping
    Enum: pl.String,  # Approx mapping
    PickleType: pl.Object,  # For storing Python objects
    TupleType: pl.Struct,  # Mapped to struct

    # Special/Abstract types
    NULLTYPE: None,
    NullType: None,
    Concatenable: pl.Utf8,  # Default to string since it's a mixin
    Indexable: pl.List,  # Default to list since it's a mixin
    MatchType: pl.Utf8,  # Default to string
    SchemaType: None,  # Base class, not mappable directly
    TypeDecorator: None,  # Base class, not mappable directly
    TypeEngine: None,  # Base class, not mappable directly
    UserDefinedType: None,  # Base class, not mappable directly
    Variant: pl.Object,  # For variant data
    ExternalType: None,  # Abstract base class
}

# Create string mappings, filtering out None values
sqlalchemy_to_polars_str: Dict[str, str] = {
    k.__name__: v.__name__
    for k, v in sqlalchemy_to_polars.items()
    if v is not None and hasattr(k, '__name__') and hasattr(v, '__name__')
}

# Additional string mappings for common SQL type names
sql_type_name_to_polars: Dict[str, PolarsType] = {
    # PostgreSQL types
    'integer': pl.Int64,
    'bigint': pl.Int64,
    'smallint': pl.Int64,
    'numeric': pl.Decimal,
    'real': pl.Float32,
    'double precision': pl.Float64,
    'boolean': pl.Boolean,
    'varchar': pl.Utf8,
    'character varying': pl.Utf8,
    'character': pl.Utf8,
    'text': pl.Utf8,
    'date': pl.Date,
    'timestamp': pl.Datetime,
    'timestamp without time zone': pl.Datetime,
    'timestamp with time zone': pl.Datetime,
    'time': pl.Time,
    'time without time zone': pl.Time,
    'time with time zone': pl.Time,
    'interval': pl.Duration,
    'bytea': pl.Binary,
    'jsonb': pl.Utf8,
    'json': pl.Utf8,
    'uuid': pl.Utf8,
    'cidr': pl.Utf8,
    'inet': pl.Utf8,
    'macaddr': pl.Utf8,
    'bit': pl.Utf8,
    'bit varying': pl.Utf8,
    'money': pl.Decimal,
    'xml': pl.Utf8,
    'tsquery': pl.Utf8,
    'tsvector': pl.Utf8,
    'hstore': pl.Utf8,

    # MySQL types
    'int': pl.Int32,
    'int unsigned': pl.UInt64,
    'bigint unsigned': pl.UInt64,
    'smallint unsigned': pl.UInt16,
    'tinyint': pl.Int8,
    'tinyint unsigned': pl.UInt8,
    'mediumint': pl.Int32,
    'mediumint unsigned': pl.UInt32,
    'decimal': pl.Decimal,
    'float': pl.Float32,
    'double': pl.Float64,
    'bit': pl.Boolean,
    'char': pl.Utf8,
    'varchar': pl.Utf8,
    'binary': pl.Binary,
    'varbinary': pl.Binary,
    'tinyblob': pl.Binary,
    'blob': pl.Binary,
    'mediumblob': pl.Binary,
    'longblob': pl.Binary,
    'tinytext': pl.Utf8,
    'text': pl.Utf8,
    'mediumtext': pl.Utf8,
    'longtext': pl.Utf8,
    'datetime': pl.Datetime,
    'timestamp': pl.Datetime,
    'year': pl.Int16,
    'enum': pl.String,
    'set': pl.List,
    'json': pl.Utf8,

    # SQLite types
    'integer': pl.Int64,  # SQLite's INTEGER is 64-bit
    'real': pl.Float64,
    'text': pl.Utf8,
    'blob': pl.Binary,
    'null': None,

    # Oracle types
    'number': pl.Decimal,
    'float': pl.Float64,
    'binary_float': pl.Float32,
    'binary_double': pl.Float64,
    'varchar2': pl.Utf8,
    'nvarchar2': pl.Utf8,
    'char': pl.Utf8,
    'nchar': pl.Utf8,
    'clob': pl.Utf8,
    'nclob': pl.Utf8,
    'long': pl.Utf8,
    'raw': pl.Binary,
    'long raw': pl.Binary,
    'rowid': pl.Utf8,
    'urowid': pl.Utf8,
    'date': pl.Datetime,  # Oracle DATE includes time
    'timestamp': pl.Datetime,
    'timestamp with time zone': pl.Datetime,
    'timestamp with local time zone': pl.Datetime,
    'interval year to month': pl.Duration,
    'interval day to second': pl.Duration,
    'bfile': pl.Binary,
    'xmltype': pl.Utf8,

    # SQL Server types
    'bit': pl.Boolean,
    'tinyint': pl.Int8,
    'smallint': pl.Int16,
    'int': pl.Int32,
    'bigint': pl.Int64,
    'numeric': pl.Decimal,
    'decimal': pl.Decimal,
    'smallmoney': pl.Decimal,
    'money': pl.Decimal,
    'float': pl.Float64,
    'real': pl.Float32,
    'datetime': pl.Datetime,
    'datetime2': pl.Datetime,
    'smalldatetime': pl.Datetime,
    'date': pl.Date,
    'time': pl.Time,
    'datetimeoffset': pl.Datetime,
    'char': pl.Utf8,
    'varchar': pl.Utf8,
    'text': pl.Utf8,
    'nchar': pl.Utf8,
    'nvarchar': pl.Utf8,
    'ntext': pl.Utf8,
    'binary': pl.Binary,
    'varbinary': pl.Binary,
    'image': pl.Binary,
    'uniqueidentifier': pl.Utf8,
    'xml': pl.Utf8,
    'sql_variant': pl.Object,
    'hierarchyid': pl.Utf8,
    'geometry': pl.Utf8,
    'geography': pl.Utf8,

    # Common abbreviations and aliases
    'int4': pl.Int32,
    'int8': pl.Int64,
    'float4': pl.Float32,
    'float8': pl.Float64,
    'bool': pl.Boolean,
    'serial': pl.Int32,  # PostgreSQL auto-incrementing integer
    'bigserial': pl.Int64,  # PostgreSQL auto-incrementing bigint
    'smallserial': pl.Int16,  # PostgreSQL auto-incrementing smallint
}

# String to string mapping
sql_type_name_to_polars_str: Dict[str, str] = {
    k: v.__name__ for k, v in sql_type_name_to_polars.items() if v is not None
}


def get_polars_type(sqlalchemy_type: Union[SqlType, str]):
    """
    Get the corresponding Polars type from a SQLAlchemy type or string type name.

    Parameters:
    -----------
    sqlalchemy_type : SQLAlchemy type object or string
        The SQLAlchemy type or SQL type name string

    Returns:
    --------
    polars_type : polars.DataType
        The corresponding Polars data type, or None if no mapping exists
    """
    if isinstance(sqlalchemy_type, type):
        # For SQLAlchemy type classes
        return sqlalchemy_to_polars.get(cast(SqlType, sqlalchemy_type), pl.Utf8)
    elif isinstance(sqlalchemy_type, str):
        # For string type names (lowercase for case-insensitive matching)
        return sql_type_name_to_polars.get(sqlalchemy_type.lower(), pl.Utf8)
    else:
        # For SQLAlchemy type instances
        instance_type = type(sqlalchemy_type)
        return sqlalchemy_to_polars.get(cast(SqlType, instance_type), pl.Utf8)


def construct_sql_uri(
        database_type: str = "postgresql",
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[SecretStr] = None,
        database: Optional[str] = None,
        url: Optional[str] = None,
        **kwargs
) -> str:
    """
    Constructs a SQL URI string from the provided parameters.

    Args:
        database_type: Database type (postgresql, mysql, sqlite, etc.)
        host: Database host address
        port: Database port number
        username: Database username
        password: Database password as SecretStr
        database: Database name
        url: Complete database URL (overrides other parameters if provided)
        **kwargs: Additional connection parameters

    Returns:
        str: Formatted database URI

    Raises:
        ValueError: If insufficient information is provided
    """
    # If URL is explicitly provided, return it directly
    if url:
        return url

    # For SQLite, we handle differently since it uses a file path
    if database_type.lower() == "sqlite":
        # For SQLite, database is the path to the file
        path = database or "./database.db"
        return f"sqlite:///{path}"

    # Validate that minimum required fields are present for other databases
    if not host:
        raise ValueError("Host is required to create a URI")

    # Create credential part if username is provided
    credentials = ""
    if username:
        credentials = username
        if password:
            # Get raw password from SecretStr and encode it
            password_value = password.get_secret_value()
            encoded_password = quote_plus(password_value)
            credentials += f":{encoded_password}"
        credentials += "@"

    # Add port if specified
    port_section = f":{port}" if port else ""

    # Create base URI
    if database:
        base_uri = f"{database_type}://{credentials}{host}{port_section}/{database}"
    else:
        base_uri = f"{database_type}://{credentials}{host}{port_section}"

    # Add any additional connection parameters
    if kwargs:
        params = "&".join(f"{key}={quote_plus(str(value))}" for key, value in kwargs.items())
        base_uri += f"?{params}"

    return base_uri