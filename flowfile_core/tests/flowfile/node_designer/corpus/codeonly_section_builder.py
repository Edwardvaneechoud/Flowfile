import polars as pl

from flowfile.node_designer import CustomNodeBase, NodeSettings, NumericInput, SectionBuilder

builder = SectionBuilder(title="Advanced")
builder.add_component("timeout", NumericInput(label="Timeout (s)", default=30))
advanced_section = builder.build()


class BuilderSettings(NodeSettings):
    advanced = advanced_section


class BuilderNode(CustomNodeBase):
    node_name: str = "Builder Node"
    settings_schema: BuilderSettings = BuilderSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
