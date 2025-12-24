# types.py - Public API for type specifications
"""
Public type system for column selection and data type specification.

Usage:
    from flowfile_core.types import Types

    # Use type groups
    ColumnSelector(data_types=Types.Numeric)
    ColumnSelector(data_types=Types.String)

    # Use specific types
    ColumnSelector(data_types=Types.Int64)
    ColumnSelector(data_types=Types.Float)

    # Mix and match
    ColumnSelector(data_types=[Types.Numeric, Types.String])
"""

from enum import Enum
from typing import List, Literal, Union
import polars as pl


DataTypeStr = Literal[
    "Int8", "Int16", "Int32", "Int64",
    "UInt8", "UInt16", "UInt32", "UInt64",
    "Float32", "Float64", "Decimal",
    "String",
    "Date", "Datetime", "Time", "Duration",
    "Boolean", "Binary", "List", "Struct", "Array", "Integer", "Double", "Utf8"
]


class TypeGroup(str, Enum):
    """High-level type groups for column selection."""
    Numeric = "Numeric"
    String = "String"
    Date = "Date"
    Boolean = "Boolean"
    Binary = "Binary"
    Complex = "Complex"
    All = "ALL"

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return f"Types.{self.name}"


class DataType(str, Enum):
    """Specific data types for fine-grained control."""
    # Numeric types
    Int8 = "Int8"
    Int16 = "Int16"
    Int32 = "Int32"
    Int64 = "Int64"
    UInt8 = "UInt8"
    UInt16 = "UInt16"
    UInt32 = "UInt32"
    UInt64 = "UInt64"
    Float32 = "Float32"
    Float64 = "Float64"
    Decimal = "Decimal"

    # String types
    String = "String"
    Categorical = "Categorical"

    # Date types
    Date = "Date"
    Datetime = "Datetime"
    Time = "Time"
    Duration = "Duration"

    # Other types
    Boolean = "Boolean"
    Binary = "Binary"
    List = "List"
    Struct = "Struct"
    Array = "Array"

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return f"Types.{self.name}"


class Types:
    """
    Main entry point for type specifications.

    Examples:
        Types.Numeric     # All numeric columns
        Types.String      # All string columns
        Types.Int64       # 64-bit integers only
        Types.Float       # Alias for Float64
        Types.All         # All column types
    """

    # Type groups (most common use case)
    Numeric = TypeGroup.Numeric
    String = TypeGroup.String
    AnyDate = TypeGroup.Date
    Boolean = TypeGroup.Boolean
    Binary = TypeGroup.Binary
    Complex = TypeGroup.Complex
    All = TypeGroup.All

    # Specific numeric types
    Int = DataType.Int64  # Default integer
    Int8 = DataType.Int8
    Int16 = DataType.Int16
    Int32 = DataType.Int32
    Int64 = DataType.Int64
    UInt8 = DataType.UInt8
    UInt16 = DataType.UInt16
    UInt32 = DataType.UInt32
    UInt64 = DataType.UInt64

    Float = DataType.Float64  # Default float
    Float32 = DataType.Float32
    Float64 = DataType.Float64
    Decimal = DataType.Decimal

    # String types
    Str = DataType.String
    Text = DataType.String  # Alias
    Categorical = DataType.Categorical
    Cat = DataType.Categorical  # Short alias

    # Date/time types
    Date = DataType.Date
    Datetime = DataType.Datetime
    Time = DataType.Time
    Duration = DataType.Duration

    # Other types
    Bool = DataType.Boolean
    Bytes = DataType.Binary
    List = DataType.List
    Struct = DataType.Struct
    Array = DataType.Array


# Type alias for better type hints
TypeSpec = Union[
    TypeGroup,
    DataType,
    str,
    List[Union[TypeGroup, DataType, str, type[pl.DataType], pl.DataType]],
    type[pl.DataType],
    pl.DataType
]
