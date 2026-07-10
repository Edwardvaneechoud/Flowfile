import polars as pl

from flowfile import node_designer as nd


class KernelishSettings(nd.NodeSettings):
    main: nd.Section = nd.Section(
        title="Model",
        target=nd.ColumnSelector(label="Target", required=True),
    )


class LegacyKernelNode(nd.CustomNodeBase):
    node_name: str = "Legacy Kernel Node"
    requires_kernel: bool = True
    kernel_id: str = "kernel-abc"
    number_of_outputs: int = 2
    output_names: list[str] = ["main", "metrics"]
    settings_schema: KernelishSettings = KernelishSettings()

    def process(self, *inputs: pl.LazyFrame) -> dict[str, pl.LazyFrame]:
        return {"main": inputs[0], "metrics": inputs[0]}
