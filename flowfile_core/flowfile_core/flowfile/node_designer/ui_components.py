from typing import Any, Literal

from pydantic import BaseModel, Field, SecretStr, computed_field

from flowfile_core.flowfile.node_designer._type_registry import normalize_type_spec
from flowfile_core.secret_manager.secret_manager import decrypt_secret, get_encrypted_secret

# Public API import
from flowfile_core.types import DataType, TypeSpec

InputType = Literal["text", "number", "secret", "array", "date", "boolean"]


def normalize_input_to_data_types(v: Any) -> Literal["ALL"] | list[DataType]:
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
    label: str | None = None
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
    data_type_filter_input: TypeSpec = Field(default="ALL", alias="data_types", repr=False, exclude=True)

    class Config:
        arbitrary_types_allowed = True

    @computed_field
    @property
    def data_types_filter(self) -> Literal["ALL"] | list[DataType]:
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
        if "data_types_filter" in data and data["data_types_filter"] != "ALL":
            data["data_types"] = sorted([dt.value for dt in data["data_types_filter"]])
        return data


class TextInput(FlowfileInComponent):
    """A standard text input field for capturing string values."""

    component_type: Literal["TextInput"] = "TextInput"
    default: str | None = ""
    placeholder: str | None = ""
    input_type: InputType = "text"

    def __init__(self, **data):
        super().__init__(**data)
        if self.value is None and self.default is not None:
            self.value = self.default


class NumericInput(FlowfileInComponent):
    """A numeric input field with optional minimum and maximum value validation."""

    component_type: Literal["NumericInput"] = "NumericInput"
    default: float | None = None
    min_value: float | None = None
    max_value: float | None = None
    input_type: InputType = "number"

    def __init__(self, **data):
        super().__init__(**data)
        if self.value is None and self.default is not None:
            self.value = self.default


class SliderInput(FlowfileInComponent):
    """A slider input for selecting a numeric value within a range."""

    component_type: Literal["SliderInput"] = "SliderInput"
    default: float | None = None
    min_value: float = 0
    max_value: float = 100
    step: float = 1
    input_type: InputType = "number"

    def __init__(self, **data):
        super().__init__(**data)
        if self.value is None and self.default is not None:
            self.value = self.default
        elif self.value is None:
            self.value = self.min_value


class ToggleSwitch(FlowfileInComponent):
    """A boolean toggle switch, typically used for enabling or disabling a feature."""

    component_type: Literal["ToggleSwitch"] = "ToggleSwitch"
    default: bool = False
    description: str | None = None
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
    options: list[str | tuple[str, Any]] | type[IncomingColumns]
    default: Any | None = None
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
    options: list[str | tuple[str, Any]] | type[IncomingColumns]
    default: list[Any] = Field(default_factory=list)
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

    title: str | None = None
    description: str | None = None
    hidden: bool = False

    class Config:
        extra = "allow"
        arbitrary_types_allowed = True

    def __init__(self, **data):
        """
        Initialize a Section with components as keyword arguments.
        """
        super().__init__(**data)

    def __call__(self, **kwargs) -> "Section":
        """
        Allows adding components to the section after initialization.

        This makes it possible to build up a section dynamically.
        """
        for key, value in kwargs.items():
            setattr(self, key, value)
        return self

    def get_components(self) -> dict[str, FlowfileInComponent]:
        """
        Get all FlowfileInComponent instances from the section.

        This method collects all the UI components that have been added to the
        section, whether as defined fields or as extra fields.

        Returns:
            A dictionary mapping component names to their instances.
        """
        components = {}

        # Get from extra fields
        for key, value in getattr(self, "__pydantic_extra__", {}).items():
            if isinstance(value, FlowfileInComponent):
                components[key] = value

        # Get from defined fields (excluding metadata)
        for field_name in self.model_fields:
            if field_name not in {"title", "description", "hidden"}:
                value = getattr(self, field_name, None)
                if isinstance(value, FlowfileInComponent):
                    components[field_name] = value

        return components


class AvailableSecrets:
    """
    A marker class used in `SecretSelector` components.

    When `options` is set to this class, the component will be dynamically
    populated with the secret names available to the current user.
    This allows users to select from available secrets at runtime.

    Example:
        class MyNodeSettings(NodeSettings):
            api_key = SecretSelector(
                label="Select an API Key",
                options=AvailableSecrets
            )
    """

    pass


class SecretSelector(FlowfileInComponent):
    component_type: Literal["SecretSelector"] = "SecretSelector"
    options: type[AvailableSecrets] = AvailableSecrets
    required: bool = False
    description: str | None = None
    input_type: InputType = "secret"
    name_prefix: str | None = None

    # Private fields for runtime context
    _user_id: int | None = None
    _accessed_secrets: set | None = None  # Reference to node's tracking set

    def set_execution_context(self, user_id: int, accessed_secrets: set):
        """Called by framework before process() runs."""
        self._user_id = user_id
        self._accessed_secrets = accessed_secrets

    @property
    def secret_value(self) -> SecretStr | None:
        """
        Get the decrypted secret value.

        Can only be called during node execution (after context is set).
        Returns None if no secret is selected.
        """
        if self.value is None:
            return None

        if self._user_id is None:
            raise ValueError(
                "Secret can only be accessed during node execution. "
                "Ensure you're calling this from within the process() method."
            )

        encrypted = get_encrypted_secret(current_user_id=self._user_id, secret_name=self.value)

        if encrypted is None:
            raise ValueError(
                f"Secret '{self.value}' not found for user. " f"Please ensure the secret exists in your secrets store."
            )

        decrypted = decrypt_secret(encrypted)

        if self._accessed_secrets is not None:
            self._accessed_secrets.add(decrypted.get_secret_value())
        else:
            self._accessed_secrets = {decrypted.get_secret_value()}
        return decrypted

    def model_dump(self, **kwargs) -> dict:
        """
        Overrides the default `model_dump` to signal to the frontend
        that this needs dynamic population from available secrets.
        """
        data = super().model_dump(**kwargs)
        # Signal to frontend that options should be fetched from /secrets endpoint
        data["options"] = {"__type__": "AvailableSecrets"}
        if self.name_prefix:
            data["name_prefix"] = self.name_prefix
        return data


class ColumnActionInput(FlowfileInComponent):
    """
    A generic UI component for configuring column-based transformations.

    This component allows users to select columns, choose an action/transformation,
    and optionally rename the output. It can be configured for many use cases:
    rolling windows, aggregations, string transformations, type conversions, etc.

    The component displays:
    - A list of available columns (filterable by data type)
    - A table of configured operations with: Column, Action, Output Name
    - Optional group by and order by selectors

    Example - Rolling Window:
        ColumnActionInput(
            label="Rolling Calculations",
            actions=["sum", "mean", "min", "max"],
            output_name_template="{column}_rolling_{action}",
            show_group_by=True,
            show_order_by=True,
            data_types="Numeric"
        )

    Example - String Transformations:
        ColumnActionInput(
            label="String Operations",
            actions=["upper", "lower", "trim", "reverse"],
            output_name_template="{column}_{action}",
            data_types="String"
        )

    Example - Aggregations:
        ColumnActionInput(
            label="Aggregations",
            actions=[("sum", "Sum"), ("count", "Count"), ("mean", "Average")],
            output_name_template="{column}_{action}",
            show_group_by=True
        )
    """

    component_type: Literal["ColumnActionInput"] = "ColumnActionInput"
    input_type: InputType = "array"

    # Configurable actions - list of action names or (value, label) tuples
    actions: list[str | tuple[str, str]] = Field(default_factory=list)
    """Actions available for selection. Can be strings or (value, label) tuples."""

    # Template for auto-generating output names
    # Supports placeholders: {column}, {action}
    output_name_template: str = "{column}_{action}"
    """Template for generating default output names. Use {column} and {action} placeholders."""

    # Optional grouping/ordering support
    show_group_by: bool = False
    """Whether to show the group by column selector."""

    show_order_by: bool = False
    """Whether to show the order by column selector."""

    # Type filtering for column selection
    data_type_filter_input: TypeSpec = Field(default="ALL", alias="data_types", repr=False, exclude=True)
    """Filter columns by data type. Defaults to ALL."""

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        super().__init__(**data)
        # Initialize value if not set
        if self.value is None:
            self.value = {
                "rows": [],
                "group_by_columns": [],
                "order_by_column": None,
            }

    def set_value(self, value: Any):
        """
        Sets the value from frontend.
        """
        self.value = value
        return self

    @computed_field
    @property
    def data_types_filter(self) -> Literal["ALL"] | list[DataType]:
        """
        A computed field that normalizes the `data_type_filter_input` into a
        standardized format for the frontend.
        """
        return normalize_input_to_data_types(self.data_type_filter_input)

    def model_dump(self, **kwargs) -> dict:
        """
        Serializes the component for the frontend.
        """
        data = super().model_dump(**kwargs)
        # Normalize actions to list of {value, label} objects for frontend
        normalized_actions = []
        for action in self.actions:
            if isinstance(action, tuple):
                normalized_actions.append({"value": action[0], "label": action[1]})
            else:
                normalized_actions.append({"value": action, "label": action})
        data["actions"] = normalized_actions
        data["output_name_template"] = self.output_name_template
        data["show_group_by"] = self.show_group_by
        data["show_order_by"] = self.show_order_by
        if "data_types_filter" in data and data["data_types_filter"] != "ALL":
            data["data_types"] = sorted([dt.value for dt in data["data_types_filter"]])
        else:
            data["data_types"] = "ALL"
        return data
