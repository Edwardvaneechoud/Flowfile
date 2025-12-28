# Fixed custom_node.py with proper type hints

import polars as pl
from pydantic import BaseModel
from typing import Any, Dict, Optional, TypeVar
from flowfile_core.flowfile.node_designer.ui_components import FlowfileInComponent, IncomingColumns, Section
from flowfile_core.schemas.schemas import NodeTemplate, NodeTypeLiteral, TransformTypeLiteral


def to_frontend_schema(model_instance: BaseModel) -> dict:
    """
    Recursively converts a Pydantic model instance into a JSON-serializable
    dictionary suitable for the frontend.

    This function handles special marker classes like `IncomingColumns` and
    nested `Section` and `FlowfileInComponent` instances.

    Args:
        model_instance: The Pydantic model instance to convert.

    Returns:
        A dictionary representation of the model.
    """
    result = {}
    extra_fields = getattr(model_instance, '__pydantic_extra__', {})
    model_fields = {k: getattr(model_instance, k) for k in model_instance.model_fields.keys()}
    for key, value in (extra_fields|model_fields).items():
        result[key] = _convert_value(value)
    return result


def _convert_value(value: Any) -> Any:
    """
    Helper function to convert any value to a frontend-ready format.
    """
    if isinstance(value, Section):
        section_data = value.model_dump(
            include={'title', 'description', 'hidden'},
            exclude_none=True
        )
        section_data["component_type"] = "Section"
        section_data["components"] = {
            key: _convert_value(comp)
            for key, comp in value.get_components().items()
        }
        return section_data

    elif isinstance(value, FlowfileInComponent):
        component_dict = value.model_dump(exclude_none=True)
        if 'options' in component_dict:
            if component_dict['options'] is IncomingColumns or (
                    isinstance(component_dict['options'], type) and
                    issubclass(component_dict['options'], IncomingColumns)
            ):
                component_dict['options'] = {"__type__": "IncomingColumns"}
        return component_dict
    elif isinstance(value, BaseModel):
        return to_frontend_schema(value)
    elif isinstance(value, list):
        return [_convert_value(item) for item in value]
    elif isinstance(value, dict):
        return {k: _convert_value(v) for k, v in value.items()}
    elif isinstance(value, tuple):
        return tuple(_convert_value(item) for item in value)
    else:
        return value


# Type variable for the Section factory
T = TypeVar('T', bound=Section)


def create_section(**components: FlowfileInComponent) -> Section:
    """
    Factory function to create a Section with proper type hints.

    This is a convenience function that makes it easier to create `Section`
    objects with autocomplete and type checking in modern editors.

    Usage:
        advanced_config_section = create_section(
            case_sensitive=case_sensitive_toggle
        )

    Args:
        **components: Keyword arguments where each key is the component name
                      and the value is a `FlowfileInComponent` instance.

    Returns:
        A new `Section` instance containing the provided components.
    """
    return Section(**components)


class NodeSettings(BaseModel):
    """
    The top-level container for all sections in a node's UI.

    This class holds all the `Section` objects that make up the settings panel
    for a custom node.

    Example:
        class MyNodeSettings(NodeSettings):
            main_config = main_config_section
            advanced_options = advanced_config_section
    """
    class Config:
        extra = 'allow'
        arbitrary_types_allowed = True

    def __init__(self, **sections):
        """
        Initialize NodeSettings with sections as keyword arguments.
        """
        super().__init__(**sections)

    def populate_values(self, values: Dict[str, Any]) -> 'NodeSettings':
        """
        Populates the settings with values received from the frontend.

        This method is used internally to update the node's state based on
        user input in the UI.

        Args:
            values: A dictionary of values from the frontend, where keys are
                    section names and values are dictionaries of component
                    values.

        Returns:
            The `NodeSettings` instance with updated component values.
        """
        # Handle both extra fields and defined fields
        all_sections = {}

        # Get extra fields
        extra_fields = getattr(self, '__pydantic_extra__', {})
        all_sections.update(extra_fields)

        # Get defined fields that are Sections
        for field_name in self.model_fields:
            field_value = getattr(self, field_name, None)
            if isinstance(field_value, Section):
                all_sections[field_name] = field_value

        for section_name, section in all_sections.items():
            if section_name in values:
                section_values = values[section_name]
                for component_name, component in section.get_components().items():
                    if component_name in section_values:
                        component.set_value(section_values[component_name])
        return self


def create_node_settings(**sections: Section) -> NodeSettings:
    """
    Factory function to create NodeSettings with proper type hints.

    This is a convenience function for creating `NodeSettings` instances.

    Usage:
        FilterNodeSchema = create_node_settings(
            main_config=main_config_section,
            advanced_options=advanced_config_section
        )

    Args:
        **sections: Keyword arguments where each key is the section name
                    and the value is a `Section` instance.

    Returns:
        A new `NodeSettings` instance containing the provided sections.
    """
    return NodeSettings(**sections)


class SectionBuilder:
    """
    A builder pattern for creating `Section` objects with proper type hints.

    This provides a more fluent and readable way to construct complex sections,
    especially when the number of components is large.

    Usage:
        builder = SectionBuilder(title="Advanced Settings")
        builder.add_component("timeout", NumericInput(label="Timeout (s)"))
        builder.add_component("retries", NumericInput(label="Number of Retries"))
        advanced_section = builder.build()
    """

    def __init__(self, title: Optional[str] = None, description: Optional[str] = None, hidden: bool = False):
        self._section = Section(title=title, description=description, hidden=hidden)

    def add_component(self, name: str, component: FlowfileInComponent) -> 'SectionBuilder':
        """Add a component to the section."""
        setattr(self._section, name, component)
        extra = getattr(self._section, '__pydantic_extra__', {})
        extra[name] = component
        return self

    def build(self) -> Section:
        """Build and return the Section."""
        return self._section


class NodeSettingsBuilder:
    """
    A builder pattern for creating `NodeSettings` objects.

    Provides a fluent interface for constructing the entire settings schema
    for a custom node.

    Usage:
        settings_builder = NodeSettingsBuilder()
        settings_builder.add_section("main", main_section)
        settings_builder.add_section("advanced", advanced_section)
        my_node_settings = settings_builder.build()
    """

    def __init__(self):
        self._settings = NodeSettings()

    def add_section(self, name: str, section: Section) -> 'NodeSettingsBuilder':
        """Add a section to the node settings."""
        setattr(self._settings, name, section)
        extra = getattr(self._settings, '__pydantic_extra__', {})
        extra[name] = section
        return self

    def build(self) -> NodeSettings:
        """Build and return the NodeSettings."""
        return self._settings


class CustomNodeBase(BaseModel):
    """
    The base class for creating a custom node in Flowfile.

    To create a new node, you should inherit from this class and define its
    attributes and the `process` method.
    """
    # Core node properties
    node_name: str
    node_category: str = "Custom"
    node_icon: str = "user-defined-icon.png"
    settings_schema: Optional[NodeSettings] = None

    # I/O configuration
    number_of_inputs: int = 1
    number_of_outputs: int = 1

    # Display properties in the UI
    node_group: Optional[str] = "custom"
    title: Optional[str] = "Custom Node"
    intro: Optional[str] = "A custom node for data processing"

    # Behavior properties
    node_type: NodeTypeLiteral = "process"
    transform_type: TransformTypeLiteral = "wide"

    @property
    def item(self):
        """A unique identifier for the node, derived from its name."""
        return self.node_name.replace(" ", "_").lower()

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        """
        Initialize the node, optionally populating settings from initial values.
        """
        initial_values = data.pop('initial_values', None)
        super().__init__(**data)
        if self.settings_schema and initial_values:
            self.settings_schema.populate_values(initial_values)

    def get_frontend_schema(self) -> dict:
        """
        Get the frontend-ready schema with current values.

        This method is called by the backend to send the node's UI definition
        and current state to the frontend.

        Returns:
            A dictionary representing the node's schema and values.
        """
        schema = {
            "node_name": self.node_name,
            "node_category": self.node_category,
            "node_icon": self.node_icon,
            "number_of_inputs": self.number_of_inputs,
            "number_of_outputs": self.number_of_outputs,
            "node_group": self.node_group,
            "title": self.title,
            "intro": self.intro,
        }

        if self.settings_schema:
            schema["settings_schema"] = to_frontend_schema(self.settings_schema)
        else:
            schema["settings_schema"] = {}

        return schema

    @classmethod
    def from_frontend_schema(cls, schema: dict) -> 'CustomNodeBase':
        """
        Create a node instance from a frontend schema.

        This is used when loading a node from a saved flow.
        """
        settings_values = schema.pop('settings_schema', {})
        node = cls(**schema)
        if settings_values and node.settings_schema:
            node.settings_schema.populate_values(settings_values)
        return node

    @classmethod
    def from_settings(cls, settings_values: dict) -> 'CustomNodeBase':
        """
        Create a node instance with just its settings values.

        Useful for creating a configured node instance programmatically.
        """
        node = cls()
        if settings_values and node.settings_schema:
            node.settings_schema.populate_values(settings_values)
        return node

    def update_settings(self, values: Dict[str, Any]) -> 'CustomNodeBase':
        """
        Update the settings with new values from the frontend.
        """
        if self.settings_schema:
            self.settings_schema.populate_values(values)
        return self

    def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
        """
        The main data processing logic for the node.

        This method must be implemented by all subclasses. It receives one or
        more Polars DataFrames as input and should return a single DataFrame
        as output.

        Args:
            *inputs: A variable number of Polars DataFrames, corresponding to
                     the inputs connected to the node.

        Returns:
            A Polars DataFrame containing the processed data.
        """
        raise NotImplementedError

    def to_node_template(self) -> NodeTemplate:
        """
        Convert the node to a `NodeTemplate` for storage or transmission.
        """
        return NodeTemplate(
            name=self.node_name,
            item=self.item,
            input=self.number_of_inputs,
            output=self.number_of_outputs,
            image=self.node_icon,
            node_group=self.node_group,
            drawer_title=self.title,
            drawer_intro=self.intro,
            node_type=self.node_type,
            transform_type=self.transform_type,
            custom_node=True
        )
