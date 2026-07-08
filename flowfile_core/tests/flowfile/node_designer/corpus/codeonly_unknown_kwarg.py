import polars as pl

from flowfile import node_designer as nd


class WobbleSettings(nd.NodeSettings):
    main: nd.Section = nd.Section(
        title="Main",
        text=nd.TextInput(label="Text", wobble=3),
    )


class WobbleNode(nd.CustomNodeBase):
    node_name: str = "Wobble Node"
    settings_schema: WobbleSettings = WobbleSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
