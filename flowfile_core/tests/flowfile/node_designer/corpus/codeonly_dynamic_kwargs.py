import polars as pl

from flowfile import node_designer as nd

BRAND = "Acme"


class DynamicSettings(nd.NodeSettings):
    main: nd.Section = nd.Section(
        title=f"{BRAND} Settings",
        prefix=nd.TextInput(label="Prefix", default=BRAND),
    )


class DynamicNode(nd.CustomNodeBase):
    node_name: str = "Dynamic Node"
    settings_schema: DynamicSettings = DynamicSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
