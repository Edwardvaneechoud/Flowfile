import polars as pl

from flowfile_core.flowfile.node_designer import ColumnSelector, CustomNodeBase, NodeSettings, Section


class _StandardizeSettings(NodeSettings):
    main_section: Section = Section(
        title="Standardize",
        description="Subtract mean and divide by standard deviation per column.",
        columns=ColumnSelector(
            label="Numeric columns to standardize",
            required=True,
            multiple=True,
            data_types="Numeric",
        ),
    )


class Standardize(CustomNodeBase):
    node_name: str = "Standardize"
    node_category: str = "Data Science"
    node_group: str = "data_science"
    node_icon: str = "data_science_transform.svg"
    title: str = "Standardize columns"
    intro: str = "Z-score scale selected numeric columns: (x - mean) / std."
    number_of_inputs: int = 1
    number_of_outputs: int = 1
    settings_schema: _StandardizeSettings = _StandardizeSettings()

    def process(self, *inputs: pl.LazyFrame | pl.DataFrame) -> pl.LazyFrame:
        df = inputs[0]
        lf = df.lazy() if isinstance(df, pl.DataFrame) else df
        cols = self.settings_schema.main_section.columns.value or []
        if not cols:
            return lf
        exprs = [((pl.col(c) - pl.col(c).mean()) / pl.col(c).std()).alias(c) for c in cols]
        return lf.with_columns(exprs)
