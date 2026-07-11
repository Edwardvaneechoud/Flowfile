import polars as pl

from flowfile import node_designer as nd


class CleanerSettings(nd.NodeSettings):
    columns: nd.Section = nd.Section(
        title="Columns",
        target_columns=nd.MultiSelect(label="Target Columns", options=nd.IncomingColumns),
        method=nd.SingleSelect(label="Method", options=["drop", "fill"], default="drop"),
    )
    tuning: nd.Section = nd.Section(
        title="Tuning",
        threshold=nd.SliderInput(label="Threshold", min_value=0, max_value=1, step=0.1, default=0.5),
        strict=nd.ToggleSwitch(label="Strict", default=True, description="Fail on unknown columns"),
    )


class CleanerNode(nd.CustomNodeBase):
    node_name: str = "Null Cleaner"
    node_category: str = "Data Quality"
    title: str = "Clean null values"
    intro: str = "Drop or fill null values in selected columns."
    settings_schema: CleanerSettings = CleanerSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
