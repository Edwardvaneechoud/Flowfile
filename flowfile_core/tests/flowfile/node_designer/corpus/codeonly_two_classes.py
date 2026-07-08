import polars as pl

from flowfile import node_designer as nd


class FirstNode(nd.CustomNodeBase):
    node_name: str = "First"

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]


class SecondNode(nd.CustomNodeBase):
    node_name: str = "Second"

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
