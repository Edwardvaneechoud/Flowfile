import polars as pl

from flowfile_core.flowfile.node_designer import ColumnSelector, CustomNodeBase, NodeSettings, Section
from flowfile_core.flowfile.node_designer.ui_components import NumericInput


class _ZScoreSettings(NodeSettings):
    main_section: Section = Section(
        title="Z-Score Anomaly",
        description="Add per-column z-scores plus a single boolean is_anomaly column.",
        columns=ColumnSelector(
            label="Numeric columns to score",
            required=True,
            multiple=True,
            data_types="Numeric",
        ),
        threshold=NumericInput(
            label="Anomaly threshold (|z| >)",
            default=3.0,
            min_value=0.0,
        ),
    )


class ZScoreAnomaly(CustomNodeBase):
    node_name: str = "Z-Score Anomaly"
    node_category: str = "Data Science"
    node_group: str = "data_science"
    node_icon: str = "data_science_transform.svg"
    title: str = "Z-score anomaly flagging"
    intro: str = "Append <col>_zscore for each selected column and an is_anomaly flag."
    number_of_inputs: int = 1
    number_of_outputs: int = 1
    settings_schema: _ZScoreSettings = _ZScoreSettings()

    def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
        df = inputs[0]
        cols = self.settings_schema.main_section.columns.value or []
        if not cols:
            return df
        threshold = float(self.settings_schema.main_section.threshold.value or 3.0)

        zscore_exprs = [((pl.col(c) - pl.col(c).mean()) / pl.col(c).std()).alias(f"{c}_zscore") for c in cols]
        df = df.with_columns(zscore_exprs)

        anomaly = pl.lit(False)
        for c in cols:
            anomaly = anomaly | (pl.col(f"{c}_zscore").abs() > threshold)
        return df.with_columns(anomaly.alias("is_anomaly"))
