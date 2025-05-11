from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union
from pydantic import BaseModel, field_validator, ConfigDict
import polars as pl
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import type_to_polars_str
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.schemas.input_schema import MinimalFieldInfo
from flowfile_core.flowfile.flow_data_engine.flow_file_column.polars_type import PlType
from flowfile_core.configs import logger

# Use TYPE_CHECKING to avoid circular imports
if TYPE_CHECKING:
    from airbyte import Source
else:
    Source = Any


class LazyAirbyteSource:
    """Lazy wrapper for airbyte Source class."""
    _source_class = None

    @classmethod
    def get_source_class(cls):
        if cls._source_class is None:
            logger.info("Importing airbyte Source class")
            from airbyte import Source
            cls._source_class = Source
        return cls._source_class


class JsonSchema(BaseModel):
    type: Optional[Union[str, List[str]]]
    airbyte_type: Optional[Union[str, List[str]]] = None
    format: Optional[str] = None

    def get_pl_type(self) -> pl.DataType:
        if self.format:
            format_mapping = {
                'date-time': 'datetime',
                'date': 'date',
                'time': 'time'
            }
            dtype = format_mapping.get(self.format, 'string')
        else:
            type_mapping = {
                'string': 'string',
                'boolean': 'bool',
                'integer': 'int',
                'number': 'float',
                'array': 'string',
                'object': 'string'
            }
            if isinstance(self.type, list) and len(self.type) >= 1:
                _type_mappings = (type_mapping.get(t) for t in self.type)
                dtype = next((t for t in _type_mappings if t is not None), self.type[0])
            elif isinstance(self.type, list) and len(self.type) == 0:
                dtype = 'string'
            else:
                dtype = type_mapping.get(self.type[0] if isinstance(self.type, list) else self.type, 'string')
        return type_to_polars_str(dtype)


class AirbyteProperty(BaseModel):
    name: str
    json_schema: JsonSchema

    def get_pl_type(self) -> PlType:
        return PlType(column_name=self.name, pl_datatype=self.json_schema.get_pl_type())


class AirbyteResponse(BaseModel):
    source: Any  # Using Any to avoid direct Source import
    properties: list[AirbyteProperty]

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_validator('source')
    @classmethod
    def validate_source(cls, v: Any) -> Any:
        source_class = LazyAirbyteSource.get_source_class()
        if not isinstance(v, source_class):
            raise ValueError(f"Source must be an instance of airbyte.Source, got {type(v)}")
        return v

    def get_flow_file_columns(self) -> List[FlowfileColumn]:
        return [
            FlowfileColumn.create_from_polars_type(c.get_pl_type(), col_index=i)
            for i, c in enumerate(self.properties)
        ]


class GenericProperties(BaseModel):
    type: str
    title: Optional[str] = None
    description: Optional[str] = None
    order: Optional[int] = None
    required: Optional[List[str]] = None
    airbyte_secret: Optional[bool] = None
    pattern: Optional[str] = None
    pattern_descriptor: Optional[str] = None
    format: Optional[str] = None
    examples: Optional[List[Any]] = None
    enum: Optional[List[str]] = None
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    items: Optional[Any] = None
    properties: Optional[Dict[str, Any]] = None

    @field_validator('items', 'properties')
    @classmethod
    def validate_nested(cls, value: Any) -> Any:
        if isinstance(value, dict):
            if 'type' in value:
                return GenericProperties(**value)
            return {k: GenericProperties(**v) if isinstance(v, dict) else v for k, v in value.items()}
        return value


class GenericSchema(BaseModel):
    title: str
    type: str
    required: Optional[List[str]] = None
    additionalProperties: Optional[bool] = None
    properties: Dict[str, GenericProperties]


class FieldProperty(BaseModel):
    title: Optional[str] = None
    type: str
    key: str
    description: Optional[str] = None
    airbyte_secret: Optional[bool] = None
    input_value: Optional[str] = None
    default: Any


class OverallFieldProperty(BaseModel):
    title: Optional[str] = None
    type: str
    key: str
    required: bool
    properties: List[FieldProperty]
    items: Optional[List[FieldProperty]]
    isOpen: bool
    description: Optional[str] = None
    input_value: Optional[str] = None
    airbyte_secret: Optional[bool] = None
    default: Any


class AirbyteConfigTemplate(BaseModel):
    source_name: str
    docs_url: Optional[str] = None
    config_spec: Dict
    available_streams: Optional[List[str]] = None


class AirbyteSettings(BaseModel):
    source_name: str
    stream: str
    config_ref: Optional[str] = None
    config: Optional[Dict] = None
    fields: Optional[List[MinimalFieldInfo]] = None
    enforce_full_refresh: Optional[bool] = True
    flowfile_flow_id: int
    flowfile_node_id: int
    version: Optional[str] = None


def get_source_instance(*args, **kwargs) -> 'Source':
    """Helper function to get a Source instance with lazy loading."""
    source_class = LazyAirbyteSource.get_source_class()
    return source_class(*args, **kwargs)