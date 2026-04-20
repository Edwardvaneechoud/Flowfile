import polars as pl

from flowfile_core.flowfile.node_designer import ColumnSelector, CustomNodeBase, NodeSettings, Section


class _MinMaxSettings(NodeSettings):
    main_section: Section = Section(
        title="Min-Max Scale",
        description="Rescale columns to the [0, 1] range using (x - min) / (max - min).",
        columns=ColumnSelector(
            label="Numeric columns to scale",
            required=True,
            multiple=True,
            data_types="Numeric",
        ),
    )


class MinMaxScale(CustomNodeBase):
    node_name: str = "Min-Max Scale"
    node_category: str = "Data Science"
    node_group: str = "data_science"
    title: str = "Min-Max scale columns"
    intro: str = "Rescale selected numeric columns into [0, 1]."
    number_of_inputs: int = 1
    number_of_outputs: int = 1
    settings_schema: _MinMaxSettings = _MinMaxSettings()

    def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
        df = inputs[0]
        cols = self.settings_schema.main_section.columns.value or []
        if not cols:
            return df
        exprs = []
        for c in cols:
            col = pl.col(c)
            denom = col.max() - col.min()
            # When max == min the range is zero; emit zeros to keep the column shape.
            scaled = pl.when(denom == 0).then(pl.lit(0.0)).otherwise((col - col.min()) / denom)
            exprs.append(scaled.alias(c))
        return df.with_columns(exprs)
