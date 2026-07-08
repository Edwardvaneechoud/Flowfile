import polars as pl

from flowfile import node_designer as nd


class ScorerSettings(nd.NodeSettings):
    main: nd.Section = nd.Section(
        title="Model",
        target=nd.ColumnSelector(label="Target Column", data_types="Numeric", required=True),
    )


class ScorerNode(nd.CustomNodeBase):
    node_name: str = "Sklearn Scorer"
    environment: str = "kernel"
    dependencies: list[str] = ["scikit-learn>=1.4", "xgboost"]
    settings_schema: ScorerSettings = ScorerSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
