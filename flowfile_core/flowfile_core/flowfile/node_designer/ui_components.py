from typing import List, Optional, Any, Literal, Tuple, Union, Type, Dict
from pydantic import BaseModel, Field
import polars as pl

# Define placeholder Polars data types for validation
DataType = Any
Date = pl.Date
Datetime = pl.Datetime
Int64 = pl.Int64
Float64 = pl.Float64

InputType = Literal["text", "number", "secret", "array", "date", "boolean"]

# --- Base Component ---


class IncomingColumn(BaseModel):
    """Represents a column from the input dataframe."""
    column_name: str
    data_type: DataType


class IncomingColumns:
    """A marker class to indicate that a component's options should be populated with columns from the input dataframe."""
    pass


class FlowfileInComponent(BaseModel):
    """Base class for all UI components in the node settings panel."""
    component_type: str = Field(..., description="Type of the UI component")
    value: Any = None
    label: Optional[str] = None
    input_type: InputType

    def set_value(self, value: Any):
        """Sets the value of the component, received from the frontend."""
        self.value = value
        return self

    def to_frontend_dict(self) -> dict:
        """Convert component to frontend-ready dictionary."""
        data = self.model_dump(exclude_none=True)

        # Handle special cases for options
        if 'options' in data:
            if data['options'] == IncomingColumns or (
                    isinstance(data['options'], type) and issubclass(data['options'], IncomingColumns)
            ):
                data['options'] = {"__type__": "IncomingColumns"}

        return data


class ColumnSelector(FlowfileInComponent):
    """A specialized component to select one or more columns with data type filtering."""
    component_type: Literal["ColumnSelector"] = "ColumnSelector"
    required: bool = False
    data_types: Union[List[DataType], Literal["ALL"]] = "ALL"
    multiple: bool = False


class TextInput(FlowfileInComponent):
    """A standard text input field."""
    component_type: Literal["TextInput"] = "TextInput"
    default: Optional[str] = ""
    placeholder: Optional[str] = ""
    input_type: InputType = "text"

    def __init__(self, **data):
        super().__init__(**data)
        if self.value is None and self.default is not None:
            self.value = self.default

    def __str__(self):
        return str(self.value) if self.value is not None else ""


class NumericInput(FlowfileInComponent):
    """Numeric input with validation."""
    component_type: Literal["NumericInput"] = "NumericInput"
    default: Optional[float] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    input_type: InputType = "number"

    def __init__(self, **data):
        super().__init__(**data)
        if self.value is None and self.default is not None:
            self.value = self.default


class SliderInput(FlowfileInComponent):
    """A slider for selecting a numeric value within a range."""
    component_type: Literal["SliderInput"] = "SliderInput"
    min_value: float
    max_value: float
    default: Optional[float] = None
    step: float = 1.0
    input_type: InputType = "number"

    def __init__(self, **data):
        super().__init__(**data)
        if self.value is None and self.default is not None:
            self.value = self.default
        elif self.value is None:
            self.value = self.min_value

    def __float__(self):
        return float(self.value)

    def __int__(self):
        return int(self.value)


class ToggleSwitch(FlowfileInComponent):
    """A boolean toggle switch."""
    component_type: Literal["ToggleSwitch"] = "ToggleSwitch"
    default: bool = False
    description: Optional[str] = None
    input_type: InputType = "boolean"

    def __init__(self, **data):
        super().__init__(**data)
        if self.value is None:
            self.value = self.default

    def __bool__(self):
        return bool(self.value)


class SingleSelect(FlowfileInComponent):
    """A dropdown for selecting a single option."""
    component_type: Literal["SingleSelect"] = "SingleSelect"
    options: Union[List[Union[str, Tuple[str, Any]]], Type[IncomingColumns]]
    default: Optional[Any] = None
    input_type: InputType = "text"

    def __init__(self, **data):
        super().__init__(**data)
        if self.value is None and self.default is not None:
            self.value = self.default

    def __str__(self):
        return str(self.value) if self.value is not None else ""


class MultiSelect(FlowfileInComponent):
    """A multi-select dropdown for choosing multiple options."""
    component_type: Literal["MultiSelect"] = "MultiSelect"
    options: Union[List[Union[str, Tuple[str, Any]]], Type[IncomingColumns]]
    default: List[Any] = Field(default_factory=list)
    input_type: InputType = "array"

    def __init__(self, **data):
        super().__init__(**data)
        if self.value is None:
            self.value = self.default if self.default else []

    def __iter__(self):
        return iter(self.value if self.value else [])


# --- Section and NodeSettings ---

class Section(BaseModel):
    """A container for UI components. Accepts components as keyword arguments."""
    class Config:
        extra = 'allow'
        arbitrary_types_allowed = True

    def to_frontend_dict(self) -> dict:
        """Convert section to frontend-ready dictionary."""
        result = {}
        for key, value in self.__dict__.items():
            if key.startswith('_'):
                continue
            if isinstance(value, FlowfileInComponent):
                result[key] = value.to_frontend_dict()
            elif isinstance(value, BaseModel):
                result[key] = value.model_dump(exclude_none=True)
            else:
                result[key] = value
        return result


class NodeSettings(BaseModel):
    """The top-level container for all sections in a node's UI."""
    class Config:
        extra = 'allow'
        arbitrary_types_allowed = True

    def to_frontend_dict(self) -> dict:
        """Convert all settings to frontend-ready dictionary."""
        result = {}
        for key, value in self.__dict__.items():
            if key.startswith('_'):
                continue
            if isinstance(value, Section):
                result[key] = value.to_frontend_dict()
            elif isinstance(value, BaseModel):
                result[key] = value.model_dump(exclude_none=True)
            else:
                result[key] = value
        return result

    def populate_values(self, values: Dict[str, Any]) -> 'NodeSettings':
        """
        Populate the settings with values from a dictionary.
        The dictionary should have the same nested structure as the settings.
        """
        for section_name, section in self.__dict__.items():
            if isinstance(section, Section) and section_name in values:
                section_values = values[section_name]
                for component_name, component in section.__dict__.items():
                    if isinstance(component, FlowfileInComponent) and component_name in section_values:
                        component.set_value(section_values[component_name])
        return self


# --- Custom Node Base ---

class CustomNodeBase:
    """
    The base class for a custom node.

    Developers should subclass this and define the 'settings_schema' attribute
    by composing Section and NodeSettings objects.
    """
    node_name: str
    node_category: str = "Custom"
    node_icon: str = ""

    # The developer defines the UI schema
    settings_schema: NodeSettings = None

    def __init__(self, initial_values: Optional[Dict[str, Any]] = None):
        """
        Initialize the node with optional initial values.

        Args:
            initial_values: A dictionary with the same structure as settings_schema
                           to populate initial values
        """
        if self.settings_schema and initial_values:
            self.settings_schema.populate_values(initial_values)

    def get_frontend_schema(self) -> dict:
        """Get the frontend-ready schema with current values."""
        if self.settings_schema:
            return self.settings_schema.to_frontend_dict()
        return {}

    def update_settings(self, values: Dict[str, Any]) -> 'CustomNodeBase':
        """Update the settings with new values from frontend."""
        if self.settings_schema:
            self.settings_schema.populate_values(values)
        return self

    def process(self, inputs: list[pl.DataFrame], settings: NodeSettings) -> pl.DataFrame:
        """
        The core transformation logic for the node.

        'settings' is a populated NodeSettings instance whose structure
        matches the 'settings_schema' defined for the class.
        """
        raise NotImplementedError


# --- Helper function for backward compatibility ---
def to_frontend_schema(settings: NodeSettings) -> dict:
    """
    Convert a NodeSettings instance to a frontend-ready dictionary.
    This is kept for backward compatibility.
    """
    return settings.to_frontend_dict()
