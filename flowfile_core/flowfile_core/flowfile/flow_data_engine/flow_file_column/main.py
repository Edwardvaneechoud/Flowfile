
from dataclasses import dataclass
from typing import Optional, Any, List, Dict, Literal, Iterable

from flowfile_core.schemas import input_schema
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import cast_str_to_polars_type
from flowfile_core.flowfile.flow_data_engine.flow_file_column.polars_type import PlType
import polars as pl
# TODO: rename flow_file_column to flowfile_column
DataTypeGroup = Literal['numeric', 'str', 'date']


def convert_pl_type_to_string(pl_type: pl.DataType, inner: bool = False) -> str:
    if isinstance(pl_type, pl.List):
        inner_str = convert_pl_type_to_string(pl_type.inner, inner=True)
        return f"pl.List({inner_str})"
    elif isinstance(pl_type, pl.Array):
        inner_str = convert_pl_type_to_string(pl_type.inner, inner=True)
        return f"pl.Array({inner_str})"
    elif isinstance(pl_type, pl.Decimal):
        precision = pl_type.precision if hasattr(pl_type, 'precision') else None
        scale = pl_type.scale if hasattr(pl_type, 'scale') else None
        if precision is not None and scale is not None:
            return f"pl.Decimal({precision}, {scale})"
        elif precision is not None:
            return f"pl.Decimal({precision})"
        else:
            return "pl.Decimal()"
    elif isinstance(pl_type, pl.Struct):
        # Handle Struct with field definitions
        fields = []
        if hasattr(pl_type, 'fields'):
            for field in pl_type.fields:
                field_name = field.name
                field_type = convert_pl_type_to_string(field.dtype, inner=True)
                fields.append(f'pl.Field("{field_name}", {field_type})')
        field_str = ", ".join(fields)
        return f"pl.Struct([{field_str}])"
    else:
        # For base types, we want the full pl.TypeName format
        return str(pl_type.base_type()) if not inner else f"pl.{pl_type}"


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
    __sql_type: Optional[Any]
    __is_unique: Optional[bool]
    __nullable: Optional[bool]
    __has_values: Optional[bool]
    average_value: Optional[str]
    __perc_unique: Optional[float]

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
        return cls.from_input(column_name=minimal_field_info.name,
                              data_type=minimal_field_info.data_type)

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

    def get_column_repr(self):
        return dict(name=self.name,
                    size=self.size,
                    data_type=str(self.data_type),
                    has_values=self.has_values,
                    is_unique=self.is_unique,
                    max_value=str(self.max_value),
                    min_value=str(self.min_value),
                    number_of_unique_values=self.number_of_unique_values,
                    number_of_filled_values=self.number_of_filled_values,
                    number_of_empty_values=self.number_of_empty_values,
                    average_size=self.average_value)

    def generic_datatype(self) -> DataTypeGroup:
        if self.data_type in ('Utf8', 'VARCHAR', 'CHAR', 'NVARCHAR', 'String'):
            return 'str'
        elif self.data_type in ('fixed_decimal', 'decimal', 'float', 'integer', 'boolean', 'double', 'Int16', 'Int32',
                                'Int64', 'Float32', 'Float64', 'Decimal', 'Binary', 'Boolean', 'Uint8', 'Uint16',
                                'Uint32', 'Uint64'):
            return 'numeric'
        elif self.data_type in ('datetime', 'date', 'Date', 'Datetime', 'Time'):
            return 'date'

    def get_polars_type(self) -> PlType:
        pl_datatype = cast_str_to_polars_type(self.data_type)
        pl_type = PlType(pl_datatype=pl_datatype, **self.__dict__)
        return pl_type

    def update_type_from_polars_type(self, pl_type: PlType):
        self.data_type = str(pl_type.pl_datatype.base_type())


def convert_stats_to_column_info(stats: List[Dict]) -> List[FlowfileColumn]:
    return [FlowfileColumn.create_from_polars_type(PlType(**c)) for c in stats]


def convert_pl_schema_to_raw_data_format(pl_schema: pl.Schema) -> List[input_schema.MinimalFieldInfo]:
    return [FlowfileColumn.create_from_polars_type(PlType(column_name=k, pl_datatype=v)).get_minimal_field_info()
            for k, v in pl_schema.items()]


def assert_if_flowfile_schema(obj: Iterable) -> bool:
    """
    Assert that the object is a valid iterable of FlowfileColumn objects.
    """
    if isinstance(obj, (list, set, tuple)):
        return all(isinstance(item, FlowfileColumn) for item in obj)
    return False
