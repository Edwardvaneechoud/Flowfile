"""
Code generator for converting CustomNodeBase instances to executable Python code.

This module provides functionality to export custom nodes as standalone Python scripts
that can be executed outside the FlowFile visual interface while maintaining a
dependency on Flowfile.
"""

from __future__ import annotations

import ast
import inspect
import textwrap
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from flowfile_core.flowfile.node_designer.custom_node import CustomNodeBase
    from flowfile_core.flowfile.node_designer.ui_components import (
        FlowfileInComponent,
        Section,
    )


class CustomNodeCodeGenerator:
    """
    Converts a CustomNodeBase instance to executable Python code.

    This generator extracts the class definition, settings values, and creates
    example usage code that can be run as a standalone Python script.

    Args:
        node: The CustomNodeBase instance to generate code for.
        settings_values: Optional dictionary of settings values to embed in the
            generated code. If not provided, extracts current settings from the node.
        source_file_path: Optional path to the source file containing the node class.
            Used to extract the original class definition.

    Example:
        >>> generator = CustomNodeCodeGenerator(my_node)
        >>> code = generator.generate(include_example=True)
        >>> print(code)
    """

    def __init__(
        self,
        node: "CustomNodeBase",
        settings_values: dict | None = None,
        source_file_path: Path | None = None,
    ):
        self.node = node
        self.settings_values = settings_values or self._extract_current_settings()
        self.source_file_path = source_file_path

    def generate(self, include_example: bool = True) -> str:
        """
        Generate the complete Python code for the custom node.

        Args:
            include_example: If True, includes example usage with sample data.

        Returns:
            A string containing executable Python code.
        """
        parts = [
            self._generate_module_docstring(),
            "",
            self._generate_imports(),
            "",
            self._generate_class_definition(),
        ]

        if include_example:
            parts.extend([
                "",
                self._generate_example_usage(),
            ])

        return "\n".join(parts)

    def _generate_module_docstring(self) -> str:
        """Generate the module-level docstring."""
        lines = [
            '"""',
            f"Custom Node: {self.node.node_name}",
            f"Category: {self.node.node_category}",
            "Generated from FlowFile",
            "",
        ]

        if self.node.intro:
            lines.append("Description:")
            # Wrap the intro text for readability
            wrapped = textwrap.wrap(self.node.intro, width=70)
            for line in wrapped:
                lines.append(f"    {line}")

        lines.append('"""')
        return "\n".join(lines)

    def _generate_imports(self) -> str:
        """Generate required import statements."""
        imports = [
            "import polars as pl",
            "from flowfile_core.flowfile.node_designer import (",
            "    CustomNodeBase,",
            "    NodeSettings,",
            "    Section,",
        ]

        # Detect which UI components are used in the node
        used_components = self._detect_used_components()
        for component_type in sorted(used_components):
            imports.append(f"    {component_type},")

        imports.append(")")

        # Add IncomingColumns or AvailableSecrets if needed
        special_markers = self._detect_special_markers()
        if special_markers:
            imports.append("from flowfile_core.flowfile.node_designer.ui_components import (")
            for marker in sorted(special_markers):
                imports.append(f"    {marker},")
            imports.append(")")

        # Add Types import if ColumnSelector with data_types is used
        if self._uses_column_selector_with_types():
            imports.append("from flowfile_core.types import Types")

        return "\n".join(imports)

    def _detect_used_components(self) -> set[str]:
        """Detect which UI component types are used in the node settings."""
        components: set[str] = set()

        if not self.node.settings_schema:
            return components

        all_components = self.node.settings_schema.get_all_components()
        for component in all_components.values():
            component_type = type(component).__name__
            if component_type not in ("Section", "NodeSettings"):
                components.add(component_type)

        return components

    def _detect_special_markers(self) -> set[str]:
        """Detect special marker classes used in options."""
        markers: set[str] = set()

        if not self.node.settings_schema:
            return markers

        from flowfile_core.flowfile.node_designer.ui_components import (
            AvailableSecrets,
            IncomingColumns,
        )

        all_components = self.node.settings_schema.get_all_components()
        for component in all_components.values():
            options = getattr(component, "options", None)
            if options is IncomingColumns or (
                isinstance(options, type) and issubclass(options, IncomingColumns)
            ):
                markers.add("IncomingColumns")
            elif options is AvailableSecrets or (
                isinstance(options, type) and issubclass(options, AvailableSecrets)
            ):
                markers.add("AvailableSecrets")

        return markers

    def _uses_column_selector_with_types(self) -> bool:
        """Check if the node uses ColumnSelector with data_types filter."""
        if not self.node.settings_schema:
            return False

        from flowfile_core.flowfile.node_designer.ui_components import ColumnSelector

        all_components = self.node.settings_schema.get_all_components()
        for component in all_components.values():
            if isinstance(component, ColumnSelector):
                if component.data_types_filter != "ALL":
                    return True

        return False

    def _generate_class_definition(self) -> str:
        """
        Generate the class definition for the custom node.

        Tries to extract the original source code from the class.
        Falls back to reconstructing from metadata if source isn't available.
        """
        # Try to get the source code from the class
        try:
            source = self._extract_class_source()
            if source:
                return source
        except Exception:
            pass

        # Fall back to reconstructing from the node instance
        return self._reconstruct_class_definition()

    def _extract_class_source(self) -> str | None:
        """
        Try to extract the class source code using inspect or from file.

        Returns:
            The class source code as a string, or None if not available.
        """
        node_class = type(self.node)

        # First, try inspect.getsource
        try:
            source = inspect.getsource(node_class)
            # Remove leading whitespace from each line
            lines = source.split("\n")
            if lines:
                # Find the minimum indentation
                min_indent = float("inf")
                for line in lines:
                    if line.strip():
                        indent = len(line) - len(line.lstrip())
                        min_indent = min(min_indent, indent)

                if min_indent < float("inf"):
                    source = "\n".join(
                        line[int(min_indent):] if line.strip() else line
                        for line in lines
                    )
            return source
        except (OSError, TypeError):
            pass

        # If a source file path is provided, try to extract from there
        if self.source_file_path and self.source_file_path.exists():
            return self._extract_class_from_file()

        return None

    def _extract_class_from_file(self) -> str | None:
        """Extract the class definition from the source file using AST."""
        if not self.source_file_path:
            return None

        try:
            content = self.source_file_path.read_text(encoding="utf-8")
            tree = ast.parse(content)

            # Find the class that matches our node_name
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    # Check if this class has our node_name
                    for item in node.body:
                        if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                            if item.target.id == "node_name":
                                if (
                                    item.value
                                    and isinstance(item.value, ast.Constant)
                                    and item.value.value == self.node.node_name
                                ):
                                    # Extract the class source
                                    start_line = node.lineno - 1
                                    end_line = node.end_lineno if hasattr(node, "end_lineno") else start_line + 50
                                    lines = content.split("\n")
                                    return "\n".join(lines[start_line:end_line])

        except Exception:
            pass

        return None

    def _reconstruct_class_definition(self) -> str:
        """
        Reconstruct the class definition from the node instance.

        This is a fallback when source code isn't available.
        """
        node_class = type(self.node)
        class_name = node_class.__name__

        lines = [
            f"class {class_name}(CustomNodeBase):",
            f'    """',
        ]

        if self.node.intro:
            wrapped = textwrap.wrap(self.node.intro, width=70)
            for line in wrapped:
                lines.append(f"    {line}")
        else:
            lines.append(f"    A custom node: {self.node.node_name}")

        lines.append('    """')
        lines.append("")

        # Add class attributes
        lines.append(f'    node_name: str = "{self.node.node_name}"')
        lines.append(f'    node_category: str = "{self.node.node_category}"')

        if self.node.node_icon != "user-defined-icon.png":
            lines.append(f'    node_icon: str = "{self.node.node_icon}"')

        if self.node.title:
            lines.append(f'    title: str = "{self.node.title}"')

        if self.node.intro:
            lines.append(f'    intro: str = "{self.node.intro}"')

        lines.append(f"    number_of_inputs: int = {self.node.number_of_inputs}")
        lines.append(f"    number_of_outputs: int = {self.node.number_of_outputs}")

        if self.node.node_group and self.node.node_group != "custom":
            lines.append(f'    node_group: str = "{self.node.node_group}"')

        # Add settings schema if present
        if self.node.settings_schema and self.node.settings_schema.has_sections():
            lines.append("")
            lines.append("    # Settings schema definition")
            settings_code = self._generate_settings_schema_code()
            lines.append(settings_code)

        # Add process method
        lines.append("")
        process_code = self._generate_process_method()
        lines.append(process_code)

        return "\n".join(lines)

    def _generate_settings_schema_code(self) -> str:
        """Generate code for the settings schema definition."""
        if not self.node.settings_schema:
            return "    settings_schema: NodeSettings | None = None"

        lines = ["    settings_schema: NodeSettings = NodeSettings("]

        # Get all sections from the settings schema
        sections = self._get_sections_from_settings()

        for section_name, section in sections.items():
            section_lines = self._generate_section_code(section_name, section)
            lines.append(section_lines)

        lines.append("    )")

        return "\n".join(lines)

    def _get_sections_from_settings(self) -> dict[str, "Section"]:
        """Extract all sections from the settings schema."""
        from flowfile_core.flowfile.node_designer.ui_components import Section

        sections: dict[str, "Section"] = {}

        if not self.node.settings_schema:
            return sections

        # Get from extra fields
        extra = getattr(self.node.settings_schema, "__pydantic_extra__", {}) or {}
        for name, value in extra.items():
            if isinstance(value, Section):
                sections[name] = value

        # Get from model fields
        for field_name in self.node.settings_schema.model_fields:
            value = getattr(self.node.settings_schema, field_name, None)
            if isinstance(value, Section):
                sections[field_name] = value

        return sections

    def _generate_section_code(self, section_name: str, section: "Section") -> str:
        """Generate code for a single section."""
        lines = [f"        {section_name}=Section("]

        if section.title:
            lines.append(f'            title="{section.title}",')

        if section.description:
            lines.append(f'            description="{section.description}",')

        if section.hidden:
            lines.append("            hidden=True,")

        # Add components
        components = section.get_components()
        for comp_name, component in components.items():
            comp_code = self._generate_component_code(component)
            lines.append(f"            {comp_name}={comp_code},")

        lines.append("        ),")

        return "\n".join(lines)

    def _generate_component_code(self, component: "FlowfileInComponent") -> str:
        """Generate code for a single UI component."""
        from flowfile_core.flowfile.node_designer.ui_components import (
            AvailableSecrets,
            ColumnSelector,
            IncomingColumns,
            MultiSelect,
            NumericInput,
            SecretSelector,
            SingleSelect,
            SliderInput,
            TextInput,
            ToggleSwitch,
        )

        component_type = type(component).__name__
        params: list[str] = []

        if component.label:
            params.append(f'label="{component.label}"')

        if isinstance(component, TextInput):
            if component.default:
                params.append(f'default="{component.default}"')
            if component.placeholder:
                params.append(f'placeholder="{component.placeholder}"')

        elif isinstance(component, NumericInput):
            if component.default is not None:
                params.append(f"default={component.default}")
            if component.min_value is not None:
                params.append(f"min_value={component.min_value}")
            if component.max_value is not None:
                params.append(f"max_value={component.max_value}")

        elif isinstance(component, SliderInput):
            params.append(f"min_value={component.min_value}")
            params.append(f"max_value={component.max_value}")
            if component.step != 1:
                params.append(f"step={component.step}")
            if component.default is not None:
                params.append(f"default={component.default}")

        elif isinstance(component, ToggleSwitch):
            if component.default:
                params.append(f"default={component.default}")
            if component.description:
                params.append(f'description="{component.description}"')

        elif isinstance(component, (SingleSelect, MultiSelect)):
            options = getattr(component, "options", None)
            if options is IncomingColumns or (
                isinstance(options, type) and issubclass(options, IncomingColumns)
            ):
                params.append("options=IncomingColumns")
            elif isinstance(options, list):
                params.append(f"options={options!r}")

            if isinstance(component, SingleSelect) and component.default is not None:
                params.append(f'default="{component.default}"')
            elif isinstance(component, MultiSelect) and component.default:
                params.append(f"default={component.default!r}")

        elif isinstance(component, ColumnSelector):
            if component.required:
                params.append("required=True")
            if component.multiple:
                params.append("multiple=True")
            if component.data_types_filter != "ALL":
                # Use Types.Numeric, etc. for common type groups
                params.append("data_types=Types.Numeric")  # Simplified

        elif isinstance(component, SecretSelector):
            if component.options is AvailableSecrets or (
                isinstance(component.options, type)
                and issubclass(component.options, AvailableSecrets)
            ):
                params.append("options=AvailableSecrets")
            if component.required:
                params.append("required=True")
            if component.name_prefix:
                params.append(f'name_prefix="{component.name_prefix}"')

        params_str = ", ".join(params)
        return f"{component_type}({params_str})"

    def _generate_process_method(self) -> str:
        """
        Generate the process method code.

        Tries to extract from source, falls back to a template.
        """
        node_class = type(self.node)

        # Try to get the process method source
        try:
            process_method = getattr(node_class, "process")
            source = inspect.getsource(process_method)

            # Clean up indentation
            lines = source.split("\n")
            if lines:
                min_indent = float("inf")
                for line in lines:
                    if line.strip():
                        indent = len(line) - len(line.lstrip())
                        min_indent = min(min_indent, indent)

                if min_indent < float("inf"):
                    # Add 4-space indent for class method
                    source = "\n".join(
                        "    " + line[int(min_indent):] if line.strip() else line
                        for line in lines
                    )
            return source
        except (OSError, TypeError):
            pass

        # Fallback to template based on number of inputs
        if self.node.number_of_inputs == 0:
            return self._generate_source_node_process()
        else:
            return self._generate_transform_node_process()

    def _generate_source_node_process(self) -> str:
        """Generate a process method template for source nodes (0 inputs)."""
        lines = [
            '    def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:',
            '        """',
            '        Generate data for this source node.',
            '',
            '        Returns:',
            '            A Polars DataFrame containing the generated data.',
            '        """',
            '        # TODO: Implement your data generation logic here',
            '        return pl.DataFrame()',
        ]
        return "\n".join(lines)

    def _generate_transform_node_process(self) -> str:
        """Generate a process method template for transform nodes."""
        lines = [
            '    def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:',
            '        """',
            '        Process the input data.',
            '',
            '        Args:',
            '            *inputs: Input DataFrames from connected nodes.',
            '',
            '        Returns:',
            '            A Polars DataFrame containing the processed data.',
            '        """',
            '        if not inputs:',
            '            return pl.DataFrame()',
            '',
            '        input_df = inputs[0]',
            '',
            '        # TODO: Implement your processing logic here',
            '',
            '        return input_df',
        ]
        return "\n".join(lines)

    def _extract_current_settings(self) -> dict[str, Any]:
        """Extract current settings values from the node."""
        if not self.node.settings_schema:
            return {}

        result: dict[str, Any] = {}
        sections = self._get_sections_from_settings()

        for section_name, section in sections.items():
            section_values: dict[str, Any] = {}
            components = section.get_components()

            for comp_name, component in components.items():
                if component.value is not None:
                    section_values[comp_name] = component.value

            if section_values:
                result[section_name] = section_values

        return result

    def _generate_settings_dict(self) -> str:
        """Generate a dictionary literal from current settings values."""
        if not self.settings_values:
            return "{}"

        return self._format_dict(self.settings_values, indent=4)

    def _format_dict(self, d: dict, indent: int = 0) -> str:
        """Format a dictionary as a Python literal with proper indentation."""
        if not d:
            return "{}"

        lines = ["{"]
        indent_str = " " * indent
        inner_indent = " " * (indent + 4)

        for key, value in d.items():
            if isinstance(value, dict):
                formatted_value = self._format_dict(value, indent + 4)
                lines.append(f'{inner_indent}"{key}": {formatted_value},')
            elif isinstance(value, str):
                lines.append(f'{inner_indent}"{key}": "{value}",')
            elif isinstance(value, list):
                lines.append(f'{inner_indent}"{key}": {value!r},')
            elif isinstance(value, bool):
                lines.append(f'{inner_indent}"{key}": {value},')
            elif value is None:
                lines.append(f'{inner_indent}"{key}": None,')
            else:
                lines.append(f'{inner_indent}"{key}": {value},')

        lines.append(f"{indent_str}}}")
        return "\n".join(lines)

    def _generate_example_usage(self) -> str:
        """Generate example code showing how to use the node."""
        node_class_name = type(self.node).__name__
        settings_dict = self._generate_settings_dict()

        lines = [
            "# " + "=" * 76,
            "# Example Usage",
            "# " + "=" * 76,
            "",
            'if __name__ == "__main__":',
        ]

        # Add settings configuration
        if self.settings_values:
            lines.extend([
                "    # Configuration (extracted from node settings)",
                f"    settings = {settings_dict}",
                "",
                "    # Create and configure the node",
                f"    node = {node_class_name}.from_settings(settings)",
            ])
        else:
            lines.extend([
                "    # Create the node (no settings required)",
                f"    node = {node_class_name}()",
            ])

        # Add input data based on number of inputs
        if self.node.number_of_inputs >= 1:
            lines.extend([
                "",
                "    # Sample input data",
                "    input_df = pl.DataFrame({",
                '        "id": [1, 2, 3],',
                '        "name": ["Alice", "Bob", "Charlie"],',
                '        "value": [100.0, 200.0, 300.0],',
                "    })",
                "",
                "    # Run the node",
                "    result = node.process(input_df)",
                "",
                '    print("Input DataFrame:")',
                "    print(input_df)",
                '    print("\\nOutput DataFrame:")',
                "    print(result)",
            ])
        else:
            # Source node (0 inputs)
            lines.extend([
                "",
                "    # Run the source node",
                "    result = node.process()",
                "",
                '    print("Output DataFrame:")',
                "    print(result)",
            ])

        return "\n".join(lines)


def generate_node_code(
    node: "CustomNodeBase",
    settings_values: dict | None = None,
    include_example: bool = True,
    source_file_path: Path | None = None,
) -> str:
    """
    Convenience function to generate code for a custom node.

    Args:
        node: The CustomNodeBase instance to generate code for.
        settings_values: Optional dictionary of settings values to embed.
        include_example: If True, includes example usage code.
        source_file_path: Optional path to the node's source file.

    Returns:
        A string containing executable Python code.

    Example:
        >>> from flowfile_core.flowfile.node_designer import generate_node_code
        >>> code = generate_node_code(my_node, include_example=True)
        >>> print(code)
    """
    generator = CustomNodeCodeGenerator(
        node=node,
        settings_values=settings_values,
        source_file_path=source_file_path,
    )
    return generator.generate(include_example=include_example)
