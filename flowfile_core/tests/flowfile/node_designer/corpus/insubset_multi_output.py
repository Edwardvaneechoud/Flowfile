import polars as pl

from flowfile import node_designer as nd


class SplitterSettings(nd.NodeSettings):
    main: nd.Section = nd.Section(
        title="Split",
        split_column=nd.ColumnSelector(label="Split Column", data_types="Boolean", required=True),
    )


class SplitterNode(nd.CustomNodeBase):
    node_name: str = "Row Splitter"
    number_of_outputs: int = 2
    output_names: list[str] = ["pass", "fail"]
    settings_schema: SplitterSettings = SplitterSettings()

    def process(self, *inputs: pl.LazyFrame) -> dict[str, pl.LazyFrame]:
        col = self.settings_schema.main.split_column.value
        return {"pass": inputs[0].filter(pl.col(col)), "fail": inputs[0].filter(~pl.col(col))}
