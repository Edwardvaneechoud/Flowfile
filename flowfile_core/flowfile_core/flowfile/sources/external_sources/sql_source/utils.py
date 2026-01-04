# Comprehensive mapping from SQLAlchemy types to Polars types
from typing import TYPE_CHECKING, Any, Union, cast
from urllib.parse import quote_plus

import polars as pl
from polars import DataType as PolarsType
from pydantic import SecretStr
from sqlalchemy.sql.sqltypes import (
    ARRAY,
    BIGINT,
    BINARY,
    BLOB,
    BOOLEAN,
    CHAR,
    CLOB,
    DATE,
    DATETIME,
    DECIMAL,
    DOUBLE,
    DOUBLE_PRECISION,
    FLOAT,
    INT,
    INTEGER,
    JSON,
    NCHAR,
    NULLTYPE,
    NUMERIC,
    NVARCHAR,
    REAL,
    SMALLINT,
    STRINGTYPE,
    TEXT,
    TIME,
    TIMESTAMP,
    UUID,
    VARBINARY,
    VARCHAR,
    BigInteger,
    Boolean,
    Concatenable,
    Date,
    DateTime,
    Double,
    Enum,
    Float,
    Indexable,
    Integer,
    Interval,
    LargeBinary,
    MatchType,
    NullType,
    Numeric,
    PickleType,
    SchemaType,
    SmallInteger,
    String,
    Text,
    Time,
    TupleType,
    Unicode,
    UnicodeText,
    Uuid,
    _Binary,
)
from sqlalchemy.sql.type_api import ExternalType, TypeDecorator, TypeEngine, UserDefinedType, Variant

if TYPE_CHECKING:
    SqlType = Union[
        type[_Binary],
        type[ARRAY],
        type[BIGINT],
        type[BigInteger],
        type[BINARY],
        type[BLOB],
        type[BOOLEAN],
        type[Boolean],
        type[CHAR],
        type[CLOB],
        type[Concatenable],
        type[DATE],
        type[Date],
        type[DATETIME],
        type[DateTime],
        type[DECIMAL],
        type[DOUBLE],
        type[Double],
        type[DOUBLE_PRECISION],
        type[Enum],
        type[FLOAT],
        type[Float],
        type[Indexable],
        type[INT],
        type[INTEGER],
        type[Integer],
        type[Interval],
        type[JSON],
        type[LargeBinary],
        type[MatchType],
        type[NCHAR],
        type[NULLTYPE],
        type[NullType],
        type[NUMERIC],
        type[Numeric],
        type[NVARCHAR],
        type[PickleType],
        type[REAL],
        type[SchemaType],
        type[SMALLINT],
        type[SmallInteger],
        type[String],
        type[STRINGTYPE],
        type[TEXT],
        type[Text],
        type[TIME],
        type[Time],
        type[TIMESTAMP],
        type[TupleType],
        type[Unicode],
        type[UnicodeText],
        type[UUID],
        type[Uuid],
        type[VARBINARY],
        type[VARCHAR],
        type[TypeDecorator],
        type[TypeEngine],
        type[UserDefinedType],
        type[Variant],
        type[ExternalType],
    ]
else:
    SqlType = Any


# Comprehensive mapping from SQLAlchemy types to Polars types
sqlalchemy_to_polars: dict[SqlType, PolarsType] = {
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
sqlalchemy_to_polars_str: dict[str, str] = {
    k.__name__: v.__name__
    for k, v in sqlalchemy_to_polars.items()
    if v is not None and hasattr(k, "__name__") and hasattr(v, "__name__")
}

# Additional string mappings for common SQL type names
sql_type_name_to_polars: dict[str, PolarsType] = {
    # --- Integers ---
    "int": pl.Int32,
    "integer": pl.Int64,
    "int4": pl.Int32,
    "int8": pl.Int64,
    "bigint": pl.Int64,
    "short": pl.Int16,
    "smallint": pl.Int16,
    "tinyint": pl.Int8,
    "mediumint": pl.Int32,
    "serial": pl.Int32,
    "bigserial": pl.Int64,
    "smallserial": pl.Int16,
    # Unsigned (MySQL specific)
    "int unsigned": pl.UInt64,
    "bigint unsigned": pl.UInt64,
    "smallint unsigned": pl.UInt16,
    "tinyint unsigned": pl.UInt8,
    "mediumint unsigned": pl.UInt32,
    "year": pl.Int16,

    # --- Floats & Decimals ---
    "numeric": pl.Decimal,
    "decimal": pl.Decimal,
    "number": pl.Decimal,  # Oracle
    "money": pl.Decimal,
    "smallmoney": pl.Decimal,
    "real": pl.Float32,
    "float": pl.Float64,
    "float4": pl.Float32,
    "float8": pl.Float64,
    "double": pl.Float64,
    "double precision": pl.Float64,
    "binary_float": pl.Float32,  # Oracle
    "binary_double": pl.Float64,  # Oracle

    # --- Booleans ---
    "boolean": pl.Boolean,
    "bool": pl.Boolean,
    "bit": pl.Boolean,  # Note: PostgreSQL 'bit' is varying, but MSSQL/MySQL 'bit' is boolean. Defaulting to Bool.

    # --- Strings / Text ---
    "varchar": pl.Utf8,
    "varchar2": pl.Utf8,  # Oracle
    "nvarchar": pl.Utf8,
    "nvarchar2": pl.Utf8,  # Oracle
    "char": pl.Utf8,
    "nchar": pl.Utf8,
    "character": pl.Utf8,
    "character varying": pl.Utf8,
    "text": pl.Utf8,
    "tinytext": pl.Utf8,
    "mediumtext": pl.Utf8,
    "longtext": pl.Utf8,
    "ntext": pl.Utf8,
    "clob": pl.Utf8,
    "nclob": pl.Utf8,
    "long": pl.Utf8,  # Oracle
    "enum": pl.String,
    "set": pl.List,
    "rowid": pl.Utf8,  # Oracle
    "urowid": pl.Utf8,  # Oracle
    "uniqueidentifier": pl.Utf8,  # MSSQL
    "xml": pl.Utf8,
    "xmltype": pl.Utf8,
    "json": pl.Utf8,
    "jsonb": pl.Utf8,

    # --- Network / Specialized Strings (Postgres) ---
    "uuid": pl.Utf8,
    "cidr": pl.Utf8,
    "inet": pl.Utf8,
    "macaddr": pl.Utf8,
    "tsquery": pl.Utf8,
    "tsvector": pl.Utf8,
    "hstore": pl.Utf8,
    "geometry": pl.Utf8,
    "geography": pl.Utf8,
    "hierarchyid": pl.Utf8,
    "bit varying": pl.Utf8,

    # --- Dates & Times ---
    "date": pl.Date,
    "datetime": pl.Datetime,
    "datetime2": pl.Datetime,  # MSSQL
    "smalldatetime": pl.Datetime,  # MSSQL
    "timestamp": pl.Datetime,
    "timestamp without time zone": pl.Datetime,
    "timestamp with time zone": pl.Datetime,
    "timestamp with local time zone": pl.Datetime,
    "datetimeoffset": pl.Datetime,  # MSSQL
    "time": pl.Time,
    "time without time zone": pl.Time,
    "time with time zone": pl.Time,

    # --- Durations / Intervals ---
    "interval": pl.Duration,
    "interval year to month": pl.Duration,  # Oracle
    "interval day to second": pl.Duration,  # Oracle

    # --- Binary ---
    "bytea": pl.Binary,  # Postgres
    "binary": pl.Binary,
    "varbinary": pl.Binary,
    "blob": pl.Binary,
    "tinyblob": pl.Binary,
    "mediumblob": pl.Binary,
    "longblob": pl.Binary,
    "raw": pl.Binary,  # Oracle
    "long raw": pl.Binary,  # Oracle
    "bfile": pl.Binary,  # Oracle
    "image": pl.Binary,  # MSSQL

    # --- Other ---
    "null": None,
    "sql_variant": pl.Object,
}

# String to string mapping
sql_type_name_to_polars_str: dict[str, str] = {
    k: v.__name__ for k, v in sql_type_name_to_polars.items() if v is not None
}


def get_polars_type(sqlalchemy_type: SqlType | str):
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
    host: str | None = None,
    port: int | None = None,
    username: str | None = None,
    password: SecretStr | None = None,
    database: str | None = None,
    url: str | None = None,
    **kwargs,
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
