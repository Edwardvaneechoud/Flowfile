import polars as pl
from flowfile import node_designer as nd


class TrainerSettings(nd.NodeSettings):
    main: nd.Section = nd.Section(
        title="Model",
        base_model=nd.SingleSelect(
            label="Base Model",
            options=nd.AvailableArtifacts(
                scope="global",
                type=[
                    "sklearn.*",
                ],
            ),
        ),
    )


class TrainerNode(nd.CustomNodeBase):
    node_name: str = "Model Trainer"
    environment: str = "kernel"
    dependencies: list[str] = [
        "scikit-learn>=1.4",
    ]
    publishes: list[nd.Artifact] = [
        nd.Artifact("model"),
        nd.Artifact("scaler", type="sklearn.preprocessing.StandardScaler"),
    ]
    settings_schema: TrainerSettings = TrainerSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
