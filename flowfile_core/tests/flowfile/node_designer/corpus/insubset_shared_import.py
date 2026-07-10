import polars as pl

from shared.node_designer import ColumnSelector, CustomNodeBase, NodeSettings, Section


class TrimSettings(NodeSettings):
    main: Section = Section(
        title="Main",
        column=ColumnSelector(label="Column", data_types="String", required=True),
    )


class TrimNode(CustomNodeBase):
    node_name: str = "Trimmer"
    settings_schema: TrimSettings = TrimSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        col = self.settings_schema.main.column.value
        return inputs[0].with_columns(pl.col(col).str.strip_chars())
