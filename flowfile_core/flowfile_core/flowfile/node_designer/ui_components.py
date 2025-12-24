# ui_components.py - Updated ColumnSelector

from typing import List, Optional, Any, Literal, Union, Type, Tuple, Dict

from pydantic import Field, BaseModel, computed_field

from flowfile_core.flowfile.node_designer._type_registry import normalize_type_spec
# Public API import
from flowfile_core.types import DataType, TypeSpec

InputType = Literal["text", "number", "secret", "array", "date", "boolean"]


def normalize_input_to_data_types(
    v: Any
) -> Union[Literal["ALL"], List[DataType]]:
    """
    Normalizes a wide variety of inputs to either 'ALL' or a sorted list of DataType enums.
    This function is used as a Pydantic BeforeValidator.

    Args:
        v: The input value to normalize. Can be a string, a list of strings,
           a DataType, a TypeGroup, or a list of those.

    Returns:
        Either the string "ALL" or a sorted list of unique DataType enums.
    """
    if v == "ALL":
        return "ALL"
    if isinstance(v, list) and all(isinstance(item, DataType) for item in v):
        return v

    normalized_set = normalize_type_spec(v)

    if normalized_set == set(DataType):
        return "ALL"

    return sorted(list(normalized_set), key=lambda x: x.value)


class FlowfileInComponent(BaseModel):
    """
    Base class for all UI components in the node settings panel.

    This class provides the common attributes and methods that all UI components share.
    It's not meant to be used directly, but rather to be inherited by specific
    component classes.
    """
    component_type: str = Field(..., description="Type of the UI component")
    value: Any = None
    label: Optional[str] = None
    input_type: InputType

    def set_value(self, value: Any):
        """
        Sets the value of the component, received from the frontend.

        This method is used internally by the framework to populate the component's
        value when a user interacts with the UI.

        Args:
            value: The new value for the component.

        Returns:
            The component instance with the updated value.
        """
        self.value = value
        return self


class IncomingColumns:
    """
    A marker class used in `SingleSelect` and `MultiSelect` components.

    When `options` is set to this class, the component will be dynamically
    populated with the column names from the node's input dataframe.
    This allows users to select from the available columns at runtime.

    Example:
        class MyNodeSettings(NodeSettings):
            column_to_process = SingleSelect(
                label="Select a column",
                options=IncomingColumns
            )
    """
    pass


class ColumnSelector(FlowfileInComponent):
    """
    A UI component that allows users to select one or more columns from the
    input dataframe, with an optional filter based on column data types.

    This is particularly useful when a node operation should only be applied
    to columns of a specific type (e.g., numeric, string, date).
    """
    component_type: Literal["ColumnSelector"] = "ColumnSelector"
    required: bool = False
    multiple: bool = False
    input_type: InputType = "text"

    # Normalized output: either "ALL" or list of DataType enums
    data_type_filter_input: TypeSpec = Field(
        default="ALL",
        alias="data_types",
        repr=False,
        exclude=True
    )

    class Config:
        arbitrary_types_allowed = True

    @computed_field
    @property
    def data_types_filter(self) -> Union[Literal["ALL"], List[DataType]]:
        """
        A computed field that normalizes the `data_type_filter_input` into a
        standardized format for the frontend.
        """
        return normalize_input_to_data_types(self.data_type_filter_input)

    def model_dump(self, **kwargs) -> dict:
        """
        Overrides the default `model_dump` to ensure `data_types` is in the
        correct format for the frontend.
        """
        data = super().model_dump(**kwargs)
        if 'data_types_filter' in data and data['data_types_filter'] != "ALL":
            data['data_types'] = sorted([dt.value for dt in data['data_types_filter']])
        return data


class TextInput(FlowfileInComponent):
    """A standard text input field for capturing string values."""
    component_type: Literal["TextInput"] = "TextInput"
    default: Optional[str] = ""
    placeholder: Optional[str] = ""
    input_type: InputType = "text"

    def __init__(self, **data):
        super().__init__(**data)
        if self.value is None and self.default is not None:
            self.value = self.default


class NumericInput(FlowfileInComponent):
    """A numeric input field with optional minimum and maximum value validation."""
    component_type: Literal["NumericInput"] = "NumericInput"
    default: Optional[float] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    input_type: InputType = "number"

    def __init__(self, **data):
        super().__init__(**data)
        if self.value is None and self.default is not None:
            self.value = self.default


class ToggleSwitch(FlowfileInComponent):
    """A boolean toggle switch, typically used for enabling or disabling a feature."""
    component_type: Literal["ToggleSwitch"] = "ToggleSwitch"
    default: bool = False
    description: Optional[str] = None
    input_type: InputType = "boolean"

    def __init__(self, **data):
        super().__init__(**data)
        if self.value is None:
            self.value = self.default

    def __bool__(self):
        """Allows the component instance to be evaluated as a boolean."""
        return bool(self.value)


class SingleSelect(FlowfileInComponent):
    """
    A dropdown menu for selecting a single option from a list.

    The options can be a static list of strings or tuples, or they can be
    dynamically populated from the input dataframe's columns by using the
    `IncomingColumns` marker.
    """
    component_type: Literal["SingleSelect"] = "SingleSelect"
    options: Union[List[Union[str, Tuple[str, Any]]], Type[IncomingColumns]]
    default: Optional[Any] = None
    input_type: InputType = "text"

    def __init__(self, **data):
        super().__init__(**data)
        if self.value is None and self.default is not None:
            self.value = self.default


class MultiSelect(FlowfileInComponent):
    """
    A multi-select dropdown for choosing multiple options from a list.

    Like `SingleSelect`, the options can be static or dynamically populated
    from the input columns using the `IncomingColumns` marker.
    """
    component_type: Literal["MultiSelect"] = "MultiSelect"
    options: Union[List[Union[str, Tuple[str, Any]]], Type[IncomingColumns]]
    default: List[Any] = Field(default_factory=list)
    input_type: InputType = "array"

    def __init__(self, **data):
        super().__init__(**data)
        if self.value is None:
            self.value = self.default if self.default else []


class Section(BaseModel):
    """
    A container for grouping related UI components in the node settings panel.

    Sections help organize the UI by grouping components under a common title
    and description. Components can be added as keyword arguments during
    initialization or afterward.

    Example:
        main_section = Section(
            title="Main Settings",
            description="Configure the primary behavior of the node.",
            my_text_input=TextInput(label="Enter a value")
        )
    """
    title: Optional[str] = None
    description: Optional[str] = None
    hidden: bool = False

    class Config:
        extra = 'allow'
        arbitrary_types_allowed = True

    def __init__(self, **data):
        """
        Initialize a Section with components as keyword arguments.
        """
        super().__init__(**data)

    def __call__(self, **kwargs) -> 'Section':
        """
        Allows adding components to the section after initialization.

        This makes it possible to build up a section dynamically.
        """
        for key, value in kwargs.items():
            setattr(self, key, value)
        return self

    def get_components(self) -> Dict[str, FlowfileInComponent]:
        """
        Get all FlowfileInComponent instances from the section.

        This method collects all the UI components that have been added to the
        section, whether as defined fields or as extra fields.

        Returns:
            A dictionary mapping component names to their instances.
        """
        components = {}

        # Get from extra fields
        for key, value in getattr(self, '__pydantic_extra__', {}).items():
            if isinstance(value, FlowfileInComponent):
                components[key] = value

        # Get from defined fields (excluding metadata)
        for field_name in self.model_fields:
            if field_name not in {'title', 'description', 'hidden'}:
                value = getattr(self, field_name, None)
                if isinstance(value, FlowfileInComponent):
                    components[field_name] = value

        return components
