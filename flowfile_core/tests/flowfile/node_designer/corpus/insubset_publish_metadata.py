import polars as pl

from flowfile import node_designer as nd


class GreeterNode(nd.CustomNodeBase):
    node_name: str = "Greeter"
    title: str = "Greeter"
    intro: str = "Adds a greeting column"
    author: str = "Jane Doe"
    version: str = "1.2.0"
    tags: list[str] = ["text", "demo"]

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0].with_columns(pl.lit("hello").alias("greeting"))
