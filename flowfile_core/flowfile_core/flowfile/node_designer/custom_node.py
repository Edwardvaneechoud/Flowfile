# Fixed custom_node.py with proper type hints

import polars as pl
from pydantic import BaseModel, create_model
from typing import Any, Dict, Optional, Type, TypeVar
from flowfile_core.flowfile.node_designer.ui_components import FlowfileInComponent, IncomingColumns
from flowfile_core.schemas.schemas import NodeTemplate, NodeTypeLiteral, TransformTypeLiteral


def to_frontend_schema(model_instance: BaseModel) -> dict:
    """
    Recursively converts a Pydantic model instance into a JSON-serializable
    dictionary, handling special marker classes and nested components.
    """
    result = {}
    extra_fields = getattr(model_instance, '__pydantic_extra__', {})
    for key, value in extra_fields.items():
        result[key] = _convert_value(value)
    return result


def _convert_value(value: Any) -> Any:
    """Helper function to convert any value to frontend-ready format."""
    if isinstance(value, Section):
        # Get defined fields like title, description, and hidden
        section_data = value.model_dump(
            include={'title', 'description', 'hidden'},
            exclude_none=True
        )
        section_data["component_type"] = "Section"

        # Recursively convert the components stored in the 'extra' fields
        components_dict = {}
        extra_fields = getattr(value, '__pydantic_extra__', {})
        for key, comp_value in extra_fields.items():
            components_dict[key] = _convert_value(comp_value)

        section_data["components"] = components_dict
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


class Section(BaseModel):
    """Section container that accepts components as keyword arguments."""
    title: Optional[str] = None
    description: Optional[str] = None
    hidden: bool = False

    class Config:
        extra = 'allow'
        arbitrary_types_allowed = True

    def __init__(self, **data):
        """Initialize Section with components as keyword arguments."""
        super().__init__(**data)

    def __call__(self, **kwargs) -> 'Section':
        """Allow adding components after initialization."""
        for key, value in kwargs.items():
            setattr(self, key, value)
        return self


# Type variable for the Section factory
T = TypeVar('T', bound=Section)


def create_section(**components: FlowfileInComponent) -> Section:
    """
    Factory function to create a Section with type hints preserved.

    Usage:
        advanced_config_section = create_section(
            case_sensitive=case_sensitive_toggle
        )
    """
    return Section(**components)


class NodeSettings(BaseModel):
    """The top-level container for all sections in a node's UI."""
    class Config:
        extra = 'allow'
        arbitrary_types_allowed = True

    def __init__(self, **sections):
        """Initialize NodeSettings with sections as keyword arguments."""
        super().__init__(**sections)

    def populate_values(self, values: Dict[str, Any]) -> 'NodeSettings':
        """Populate settings with values from frontend."""
        # Access extra fields from __pydantic_extra__
        extra_fields = getattr(self, '__pydantic_extra__', {})
        for section_name, section in extra_fields.items():
            if isinstance(section, Section) and section_name in values:
                section_values = values[section_name]
                section_extra = getattr(section, '__pydantic_extra__', {})
                for component_name, component in section_extra.items():
                    if isinstance(component, FlowfileInComponent) and component_name in section_values:
                        component.set_value(section_values[component_name])
        return self


def create_node_settings(**sections: Section) -> NodeSettings:
    """
    Factory function to create NodeSettings with type hints preserved.

    Usage:
        FilterNodeSchema = create_node_settings(
            main_config=main_config_section,
            advanced_options=advanced_config_section
        )
    """
    return NodeSettings(**sections)


class SectionBuilder:
    """Builder pattern for creating Sections with proper type hints."""

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
    """Builder pattern for creating NodeSettings with proper type hints."""

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
    The base class for a custom node.
    """
    node_name: str
    node_category: str = "Custom"
    node_icon: str = ""
    settings_schema: Optional[NodeSettings] = None
    number_of_inputs: int = 1
    number_of_outputs: int = 1
    node_group: Optional[str] = "aggregate"
    title: Optional[str] = "Custom Node"
    intro: Optional[str] = "A custom node for data processing"
    node_type: NodeTypeLiteral = "process"
    transform_type: TransformTypeLiteral = "wide"

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        """Initialize with optional values."""
        initial_values = data.pop('initial_values', None)
        super().__init__(**data)
        if self.settings_schema and initial_values:
            self.settings_schema.populate_values(initial_values)

    def get_frontend_schema(self) -> dict:
        """Get the frontend-ready schema with current values."""
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
        """Create a node instance from frontend schema."""
        settings_values = schema.pop('settings_schema', None)
        node = cls(**schema)
        if settings_values and node.settings_schema:
            node.settings_schema.populate_values(settings_values)
        return node

    def update_settings(self, values: Dict[str, Any]) -> 'CustomNodeBase':
        """Update the settings with new values from frontend."""
        if self.settings_schema:
            self.settings_schema.populate_values(values)
        return self

    def process(self, inputs: list[pl.DataFrame], settings: Any) -> pl.DataFrame:
        raise NotImplementedError

    def to_node_template(self) -> NodeTemplate:
        """Convert to a NodeTemplate for storage or transmission."""
        return NodeTemplate(
            name=self.node_name,
            item=self.node_name.replace(" ", "_").lower(),
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
