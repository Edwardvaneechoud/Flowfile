from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union
from pydantic import BaseModel, field_validator, ConfigDict
from flowfile_worker.configs import logger

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


class AirbyteProperty(BaseModel):
    name: str
    json_schema: JsonSchema


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
    fields: Optional[List] = None
    enforce_full_refresh: Optional[bool] = True
    flowfile_flow_id: int = 1
    flowfile_node_id: int | str = -1
    version: Optional[str] = None


def get_source_instance(*args, **kwargs) -> 'Source':
    """Helper function to get a Source instance with lazy loading."""
    source_class = LazyAirbyteSource.get_source_class()
    return source_class(*args, **kwargs)