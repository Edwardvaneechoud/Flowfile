import polars as pl

from flowfile_core.flowfile.node_designer import ColumnSelector, CustomNodeBase, NodeSettings, Section
from flowfile_core.flowfile.node_designer.ui_components import TextInput, ToggleSwitch


class _OneHotSettings(NodeSettings):
    main_section: Section = Section(
        title="One-Hot Encode",
        description="Expand selected categorical columns into one-hot indicator columns.",
        columns=ColumnSelector(
            label="Columns to encode",
            required=True,
            multiple=True,
        ),
        separator=TextInput(label="Separator", default="_", placeholder="_"),
        drop_first=ToggleSwitch(label="Drop first category (avoid collinearity)", default=False),
    )


class OneHotEncode(CustomNodeBase):
    node_name: str = "One-Hot Encode"
    node_category: str = "Data Science"
    node_group: str = "data_science"
    title: str = "One-hot encode columns"
    intro: str = "Convert selected categorical columns into binary indicator columns."
    number_of_inputs: int = 1
    number_of_outputs: int = 1
    settings_schema: _OneHotSettings = _OneHotSettings()

    def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
        df = inputs[0]
        cols = self.settings_schema.main_section.columns.value or []
        if not cols:
            return df
        sep = self.settings_schema.main_section.separator.value or "_"
        drop_first = bool(self.settings_schema.main_section.drop_first.value)
        return df.to_dummies(columns=cols, separator=sep, drop_first=drop_first)
