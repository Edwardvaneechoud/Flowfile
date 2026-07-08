import polars as pl

from flowfile import node_designer as nd


class FancySettings(nd.NodeSettings):
    main: nd.Section = nd.Section(
        title="Main",
        widget=nd.HoloDeck(label="Widget"),
    )


class FancyNode(nd.CustomNodeBase):
    node_name: str = "Fancy Node"
    settings_schema: FancySettings = FancySettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
