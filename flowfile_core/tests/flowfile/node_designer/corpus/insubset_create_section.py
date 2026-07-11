import polars as pl

from flowfile.node_designer import CustomNodeBase, NumericInput, create_node_settings, create_section

main_section = create_section(
    title="Main",
    factor=NumericInput(label="Factor", default=2, min_value=0, max_value=10),
)


class ScalerNode(CustomNodeBase):
    node_name: str = "Scaler"
    settings_schema = create_node_settings(main=main_section)

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        factor = self.settings_schema.main.factor.value
        return inputs[0].with_columns(pl.all() * factor)
