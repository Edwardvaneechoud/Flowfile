import polars as pl

from flowfile import node_designer as nd


class GreeterSettings(nd.NodeSettings):
    main: nd.Section = nd.Section(
        title="Main",
        name_column=nd.ColumnSelector(label="Name Column", data_types=nd.Types.String, required=True),
        greeting=nd.TextInput(label="Greeting", default="Hello"),
    )


class ExampleGreeter(nd.CustomNodeBase):
    node_name: str = "Example Greeter"
    settings_schema: GreeterSettings = GreeterSettings()
    example_inputs: list[dict[str, list]] = [
        {"name": ["Alice", "Bob"], "age": [30, 40]},
    ]
    example_settings: dict[str, dict] = {
        "main": {"name_column": "name", "greeting": "Hi"},
    }

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        greeting = self.settings_schema.main.greeting.value
        name_col = self.settings_schema.main.name_column.value
        return inputs[0].with_columns(
            pl.concat_str([pl.lit(f"{greeting}, "), pl.col(name_col)]).alias("greeting")
        )
