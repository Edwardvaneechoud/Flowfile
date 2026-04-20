import polars as pl

from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import fetch_unique_values
from flowfile_core.flowfile.node_designer import ColumnSelector, CustomNodeBase, NodeSettings, Section
from flowfile_core.flowfile.node_designer.ui_components import TextInput, ToggleSwitch


class _OneHotSettings(NodeSettings):
    main_section: Section = Section(
        title="One-Hot Encode",
        description="Expand selected categorical columns into one-hot indicator columns.",
        columns=ColumnSelector(
            label="Columns to encode",
            required=True,
            multiple=True,
        ),
        separator=TextInput(label="Separator", default="_", placeholder="_"),
        drop_first=ToggleSwitch(label="Drop first category (avoid collinearity)", default=False),
    )


class OneHotEncode(CustomNodeBase):
    node_name: str = "One-Hot Encode"
    node_category: str = "Data Science"
    node_group: str = "data_science"
    node_icon: str = "data_science_transform.svg"
    title: str = "One-hot encode columns"
    intro: str = "Convert selected categorical columns into binary indicator columns."
    number_of_inputs: int = 1
    number_of_outputs: int = 1
    settings_schema: _OneHotSettings = _OneHotSettings()

    def process(self, *inputs: pl.LazyFrame | pl.DataFrame) -> pl.LazyFrame:
        df = inputs[0]
        lf = df.lazy() if isinstance(df, pl.DataFrame) else df

        cols = self.settings_schema.main_section.columns.value or []
        if not cols:
            return lf

        sep = self.settings_schema.main_section.separator.value or "_"
        drop_first = bool(self.settings_schema.main_section.drop_first.value)

        # Pivot-style: one cheap lazy unique-values query per column, then build
        # pure expressions against those values. The main data path stays lazy.
        # ``fetch_unique_values`` runs its own ``.unique()`` internally so we
        # can't rely on the input plan's sort surviving — sort the returned
        # list explicitly to get stable (and ``drop_first``-meaningful) output.
        dummy_exprs: list[pl.Expr] = []
        for col in cols:
            distinct_lf = lf.select(pl.col(col).cast(pl.String).alias(col))
            values = sorted(fetch_unique_values(distinct_lf))
            if drop_first and values:
                values = values[1:]
            dummy_exprs.extend(
                (pl.col(col).cast(pl.String) == v).cast(pl.UInt8).alias(f"{col}{sep}{v}") for v in values
            )

        return lf.with_columns(dummy_exprs).drop(cols)
