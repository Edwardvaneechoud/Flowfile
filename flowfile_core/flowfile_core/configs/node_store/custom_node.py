# Example SimpleFilterNode - clean implementation

import polars as pl
from typing import Any, Dict

from flowfile_core.flowfile.node_designer.custom_node import CustomNodeBase, Section, NodeSettings
from flowfile_core.flowfile.node_designer.ui_components import MultiSelect, ToggleSwitch, IncomingColumns, TextInput

# Create the components
filter_columns_input = MultiSelect(
    label="Columns to Filter",
    options=IncomingColumns
)
filter_value_input = TextInput(
    label="Filter Value",
    placeholder="Enter the value to match..."
)
case_sensitive_toggle = ToggleSwitch(
    label="Case Sensitive",
    default=True
)

# Compose into sections
main_config_section = Section(
    columns=filter_columns_input,
    value=filter_value_input
)

advanced_config_section = Section(
    case_sensitive=case_sensitive_toggle
)

# Create NodeSettings
FilterNodeSchema = NodeSettings(
    main_config=main_config_section,
    advanced_options=advanced_config_section
)


class SimpleFilterNode(CustomNodeBase):
    """A custom node to filter a DataFrame."""
    node_name: str = "Simple Filter"
    node_category: str = "Transforms"
    node_icon: str = "filter.png"
    title: str = "Simple Filter"
    intro: str = "A simple node to filter rows based on column values."

    settings_schema: NodeSettings = FilterNodeSchema

    def process(self, inputs: list[pl.DataFrame], settings: Any) -> pl.DataFrame:
        df = inputs[0].clone()

        # Access values through the component's value property
        columns_to_filter = settings.main_config.columns.value
        filter_val = settings.main_config.value.value
        is_case_sensitive = settings.advanced_options.case_sensitive.value

        if not columns_to_filter:
            return df

        filter_expr = None
        for col_name in columns_to_filter:
            current_expr = (
                pl.col(col_name).str.to_lowercase() == str(filter_val).lower()
                if not is_case_sensitive and df[col_name].dtype == pl.Utf8
                else pl.col(col_name) == filter_val
            )
            if filter_expr is None:
                filter_expr = current_expr
            else:
                filter_expr = filter_expr | current_expr

        return df.filter(filter_expr)

