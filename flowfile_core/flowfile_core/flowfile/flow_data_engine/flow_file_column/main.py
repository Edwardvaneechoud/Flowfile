from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import polars as pl

from flowfile_core.flowfile.flow_data_engine.flow_file_column.interface import DataTypeGroup, ReadableDataTypeGroup
from flowfile_core.flowfile.flow_data_engine.flow_file_column.polars_type import PlType
from flowfile_core.flowfile.flow_data_engine.flow_file_column.type_registry import convert_pl_type_to_string
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import cast_str_to_polars_type
from flowfile_core.schemas import input_schema

MAX_STAT_VALUE_LENGTH = 200


@dataclass
class FlowfileColumn:
    column_name: str
    data_type: str
    size: int
    max_value: str
    min_value: str
    col_index: int
    number_of_empty_values: int
    number_of_unique_values: int
    example_values: str
    data_type_group: ReadableDataTypeGroup
    __sql_type: Any | None
    __is_unique: bool | None
    __nullable: bool | None
    __has_values: bool | None
    average_value: str | None
    __perc_unique: float | None
    __stats_applied: bool

    def __init__(self, polars_type: PlType):
        self.data_type = convert_pl_type_to_string(polars_type.pl_datatype)
        self.size = polars_type.count - polars_type.null_count
        self.max_value = polars_type.max
        self.min_value = polars_type.min
        self.number_of_unique_values = polars_type.n_unique
        self.number_of_empty_values = polars_type.null_count
        self.example_values = polars_type.examples
        self.column_name = polars_type.column_name
        self.average_value = polars_type.mean
        self.col_index = polars_type.col_index
        self.__has_values = None
        self.__nullable = None
        self.__is_unique = None
        self.__sql_type = None
        self.__perc_unique = None
        self.__stats_applied = False
        self.data_type_group = self.get_readable_datatype_group()

    def __repr__(self):
        """
        Provides a concise, developer-friendly representation of the object.
        Ideal for debugging and console inspection.
        """
        return (
            f"FlowfileColumn(name='{self.column_name}', "
            f"type={self.data_type}, "
            f"size={self.size}, "
            f"nulls={self.number_of_empty_values})"
        )

    def __str__(self):
        """
        Provides a detailed, readable summary of the column's metadata.
        It conditionally omits any attribute that is None, ensuring a clean output.
        """
        # --- Header (Always Shown) ---
        header = f"<FlowfileColumn: '{self.column_name}'>"
        lines = []

        # --- Core Attributes (Conditionally Shown) ---
        if self.data_type is not None:
            lines.append(f"  Type: {self.data_type}")
        if self.size is not None:
            lines.append(f"  Non-Nulls: {self.size}")

        if self.size is not None and self.number_of_empty_values is not None:
            total_entries = self.size + self.number_of_empty_values
            if total_entries > 0:
                null_perc = (self.number_of_empty_values / total_entries) * 100
                null_info = f"{self.number_of_empty_values} ({null_perc:.1f}%)"
            else:
                null_info = "0 (0.0%)"
            lines.append(f"  Nulls: {null_info}")

        if self.number_of_unique_values is not None:
            lines.append(f"  Unique: {self.number_of_unique_values}")

        # --- Conditional Stats Section ---
        stats = []
        if self.min_value is not None:
            stats.append(f"    Min: {self.min_value}")
        if self.max_value is not None:
            stats.append(f"    Max: {self.max_value}")
        if self.average_value is not None:
            stats.append(f"    Mean: {self.average_value}")

        if stats:
            lines.append("  Stats:")
            lines.extend(stats)

        # --- Conditional Examples Section ---
        if self.example_values:
            example_str = str(self.example_values)
            if len(example_str) > 70:
                example_str = example_str[:67] + "..."
            lines.append(f"  Examples: {example_str}")

        return f"{header}\n" + "\n".join(lines)

    @classmethod
    def create_from_polars_type(cls, polars_type: PlType, **kwargs) -> "FlowfileColumn":
        for k, v in kwargs.items():
            if hasattr(polars_type, k):
                setattr(polars_type, k, v)
        return cls(polars_type)

    @classmethod
    def from_input(cls, column_name: str, data_type: str, **kwargs) -> "FlowfileColumn":
        pl_type = cast_str_to_polars_type(data_type)
        if pl_type is not None:
            data_type = pl_type
        return cls(PlType(column_name=column_name, pl_datatype=data_type, **kwargs))

    @classmethod
    def create_from_polars_dtype(cls, column_name: str, data_type: pl.DataType, **kwargs):
        return cls(PlType(column_name=column_name, pl_datatype=data_type, **kwargs))

    def get_minimal_field_info(self) -> input_schema.MinimalFieldInfo:
        return input_schema.MinimalFieldInfo(name=self.column_name, data_type=self.data_type)

    @classmethod
    def create_from_minimal_field_info(cls, minimal_field_info: input_schema.MinimalFieldInfo) -> "FlowfileColumn":
        return cls.from_input(column_name=minimal_field_info.name, data_type=minimal_field_info.data_type)

    @property
    def is_unique(self) -> bool:
        if self.__is_unique is None:
            if self.has_values:
                self.__is_unique = self.number_of_unique_values == self.number_of_filled_values
            else:
                self.__is_unique = False
        return self.__is_unique

    @property
    def perc_unique(self) -> float:
        if self.__perc_unique is None:
            self.__perc_unique = self.number_of_unique_values / self.number_of_filled_values
        return self.__perc_unique

    @property
    def has_values(self) -> bool:
        if not self.__has_values:
            self.__has_values = self.number_of_unique_values > 0
        return self.__has_values

    @property
    def number_of_filled_values(self):
        return self.size

    @property
    def nullable(self):
        if self.__nullable is None:
            self.__nullable = self.number_of_empty_values > 0
        return self.__nullable

    @property
    def name(self):
        return self.column_name

    def apply_statistics(
        self,
        total_rows: int,
        null_count: int,
        n_unique: int | None = None,
        min_value: Any | None = None,
        max_value: Any | None = None,
        average_value: Any | None = None,
    ) -> None:
        """Applies exactly-computed statistics onto this column.

        Replaces the schema-time sentinels with real values (from the on-demand
        column_stats pass) and resets the lazily-derived caches (is_unique,
        perc_unique, …) so they re-derive from the new counts. Every stat field
        is overwritten: a value not in this batch (count-only retry, an
        unsupported dtype) becomes unknown again instead of surviving from an
        earlier state. Values are stringified and bounded so a long-text
        min/max can't blow up payloads.
        """
        self.size = total_rows - null_count
        self.number_of_empty_values = null_count
        self.number_of_unique_values = -1 if n_unique is None else n_unique
        self.min_value = None if min_value is None else str(min_value)[:MAX_STAT_VALUE_LENGTH]
        self.max_value = None if max_value is None else str(max_value)[:MAX_STAT_VALUE_LENGTH]
        if isinstance(average_value, float):
            average_value = round(average_value, 4)
        self.average_value = None if average_value is None else str(average_value)[:MAX_STAT_VALUE_LENGTH]
        self.__stats_applied = True
        self.__is_unique = None
        self.__perc_unique = None
        self.__has_values = None
        self.__nullable = None

    @property
    def stats_applied(self) -> bool:
        """True once exact stats were written via apply_statistics."""
        return self.__stats_applied

    @staticmethod
    def _known_count(value: int | None) -> int | None:
        return None if value is None or value < 0 else value

    def get_column_repr(self):
        # Sentinel stat values (-1 counts, "" bounds — never computed) go out as
        # None so the wire model doesn't dress "unknown" up as data. Once exact
        # stats were applied, an empty string is a real bound ("" is a legal min
        # of a String column) and ships as-is.
        null_count = self._known_count(self.number_of_empty_values)
        empty_is_sentinel = not self.__stats_applied

        def bound_repr(value):
            if value is None or (value == "" and empty_is_sentinel):
                return None
            return str(value)

        return dict(
            name=self.name,
            size=None if null_count is None else self.size,
            data_type=str(self.data_type),
            data_type_group=self.data_type_group,
            has_values=self.has_values,
            is_unique=self.is_unique,
            max_value=bound_repr(self.max_value),
            min_value=bound_repr(self.min_value),
            number_of_unique_values=self._known_count(self.number_of_unique_values),
            number_of_filled_values=None if null_count is None else self.number_of_filled_values,
            number_of_empty_values=null_count,
            average_value=None if self.average_value in (None, "") else str(self.average_value),
        )

    def generic_datatype(self) -> DataTypeGroup:
        if self.data_type in ("Utf8", "VARCHAR", "CHAR", "NVARCHAR", "String"):
            return "str"
        elif self.data_type in (
            "fixed_decimal",
            "decimal",
            "float",
            "integer",
            "boolean",
            "double",
            "Int8",
            "Int16",
            "Int32",
            "Int64",
            "Int128",
            "Float16",
            "Float32",
            "Float64",
            "Decimal",
            "Binary",
            "Boolean",
            "Uint8",
            "Uint16",
            "Uint32",
            "Uint64",
            "UInt8",
            "UInt16",
            "UInt32",
            "UInt64",
            "UInt128",
        ):
            return "numeric"
        elif self.data_type in ("datetime", "date", "Date", "Datetime", "Time"):
            return "date"
        else:
            return "str"

    def get_readable_datatype_group(self) -> ReadableDataTypeGroup:
        # Parameterized dtypes stringify with their inner type, e.g. "List(Int64)"
        # or "Datetime(time_unit='us', ...)" — match on the base token.
        base = self.data_type.split("(", 1)[0]
        if base in ("Utf8", "VARCHAR", "CHAR", "NVARCHAR", "String"):
            return "String"
        elif base in ("boolean", "Boolean"):
            return "Boolean"
        elif base in ("binary", "Binary"):
            return "Binary"
        elif base in ("list", "struct", "array", "List", "Struct", "Array"):
            return "Complex"
        elif base in (
            "fixed_decimal",
            "decimal",
            "float",
            "integer",
            "double",
            "Int8",
            "Int16",
            "Int32",
            "Int64",
            "Int128",
            "Float16",
            "Float32",
            "Float64",
            "Decimal",
            "Uint8",
            "Uint16",
            "Uint32",
            "Uint64",
            "UInt8",
            "UInt16",
            "UInt32",
            "UInt64",
            "UInt128",
        ):
            return "Numeric"
        elif base in ("datetime", "date", "Date", "Datetime", "Time", "time", "Duration", "duration"):
            return "Date"
        else:
            return "Other"

    def get_polars_type(self) -> PlType:
        pl_datatype = cast_str_to_polars_type(self.data_type)
        pl_type = PlType(pl_datatype=pl_datatype, **self.__dict__)
        return pl_type

    def update_type_from_polars_type(self, pl_type: PlType):
        self.data_type = str(pl_type.pl_datatype.base_type())


def convert_stats_to_column_info(stats: list[dict]) -> list[FlowfileColumn]:
    return [FlowfileColumn.create_from_polars_type(PlType(**c)) for c in stats]


def convert_pl_schema_to_raw_data_format(pl_schema: pl.Schema) -> list[input_schema.MinimalFieldInfo]:
    return [
        FlowfileColumn.create_from_polars_type(PlType(column_name=k, pl_datatype=v)).get_minimal_field_info()
        for k, v in pl_schema.items()
    ]


def assert_if_flowfile_schema(obj: Iterable) -> bool:
    """
    Assert that the object is a valid iterable of FlowfileColumn objects.
    """
    if isinstance(obj, list | set | tuple):
        return all(isinstance(item, FlowfileColumn) for item in obj)
    return False
