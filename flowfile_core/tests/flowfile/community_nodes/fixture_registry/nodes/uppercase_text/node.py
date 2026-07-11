import polars as pl

from flowfile import node_designer as nd


class UppercaseText(nd.CustomNodeBase):
    node_name: str = "Uppercase Text"
    node_category: str = "Text"
    title: str = "Uppercase Text"
    intro: str = "Uppercase every string column in the input."

    author: str = "edwardvaneechoud"
    version: str = "1.0.0"
    tags: list[str] = ["text", "cleaning"]

    example_inputs: list[dict[str, list]] = [
        {"city": ["amsterdam", "berlin"], "country": ["nl", "de"]},
    ]
    example_settings: dict[str, dict] = {}

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0].with_columns(pl.col(pl.String).str.to_uppercase())
