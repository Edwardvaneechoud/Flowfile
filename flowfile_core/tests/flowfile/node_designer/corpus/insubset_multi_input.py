import polars as pl

from flowfile import node_designer as nd


class StackNode(nd.CustomNodeBase):
    node_name: str = "Frame Stacker"
    number_of_inputs: int = 2
    number_of_outputs: int = 1

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return pl.concat([inputs[0], inputs[1]], how="diagonal")
