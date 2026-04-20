import polars as pl

from flowfile_core.flowfile.node_designer import ColumnSelector, CustomNodeBase, NodeSettings, Section
from flowfile_core.flowfile.node_designer.ui_components import NumericInput, TextInput


class _KMeansLabelSettings(NodeSettings):
    main_section: Section = Section(
        title="KMeans Label",
        description="Fit KMeans on the selected feature columns and append a cluster label.",
        feature_columns=ColumnSelector(
            label="Feature columns",
            required=True,
            multiple=True,
            data_types="Numeric",
        ),
        n_clusters=NumericInput(label="Number of clusters", default=3.0, min_value=2.0),
        seed=NumericInput(label="Random seed", default=0.0),
        label_column=TextInput(label="Output column name", default="cluster", placeholder="cluster"),
    )


class KMeansLabel(CustomNodeBase):
    node_name: str = "KMeans Label"
    node_category: str = "Data Science"
    node_group: str = "data_science"
    node_icon: str = "data_science_transform.svg"
    title: str = "KMeans cluster labels (in-place)"
    intro: str = "Run KMeans on the selected feature columns and append a cluster label column."
    number_of_inputs: int = 1
    number_of_outputs: int = 1
    settings_schema: _KMeansLabelSettings = _KMeansLabelSettings()

    def process(self, *inputs: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame:
        # Imported lazily so that this module can be imported in environments
        # without scikit-learn (the node still appears in the registry but
        # raises a clear error only when actually executed).
        from sklearn.cluster import KMeans

        df = inputs[0]
        # KMeans needs materialised features; the framework hands us a
        # LazyFrame so collect before slicing.
        if isinstance(df, pl.LazyFrame):
            df = df.collect()
        feature_cols = self.settings_schema.main_section.feature_columns.value or []
        if not feature_cols:
            return df

        n_clusters = int(self.settings_schema.main_section.n_clusters.value or 3)
        seed = int(self.settings_schema.main_section.seed.value or 0)
        label_col = self.settings_schema.main_section.label_column.value or "cluster"

        features = df.select(feature_cols).to_numpy()
        model = KMeans(n_clusters=n_clusters, random_state=seed, n_init="auto")
        labels = model.fit_predict(features)
        return df.with_columns(pl.Series(name=label_col, values=labels))
