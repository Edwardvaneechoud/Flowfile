import polars as pl

from flowfile import node_designer as nd


class TrimText(nd.CustomNodeBase):
    node_name: str = "Trim Text"
    node_category: str = "Text"
    title: str = "Trim Text"
    intro: str = "Strip leading and trailing whitespace from every string column."

    author: str = "edwardvaneechoud"
    version: str = "1.0.0"
    tags: list[str] = ["text", "cleaning"]

    example_inputs: list[dict[str, list]] = [
        {"city": ["  amsterdam ", "berlin  "], "country": [" nl", "de "]},
    ]
    example_settings: dict[str, dict] = {}

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0].with_columns(pl.col(pl.String).str.strip_chars())
