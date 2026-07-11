import polars as pl

from flowfile import node_designer as nd


class GreetingSettings(nd.NodeSettings):
    main_config: nd.Section = nd.Section(
        title="Greeting Configuration",
        description="Configure how to greet each row",
        name_column=nd.ColumnSelector(
            label="Name Column",
            data_types=nd.Types.String,
            required=True,
        ),
        greeting=nd.SingleSelect(
            label="Greeting",
            options=[
                ("formal", "Hello"),
                ("casual", "Hey"),
            ],
            default="casual",
        ),
    )


class GreetingNode(nd.CustomNodeBase):
    node_name: str = "Greeting Generator"
    node_category: str = "Text Processing"
    title: str = "Add greetings"
    intro: str = "Prefix a name column with a greeting."

    settings_schema: GreetingSettings = GreetingSettings()

    def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
        df = inputs[0]
        name_col = self.settings_schema.main_config.name_column.value
        style = self.settings_schema.main_config.greeting.value
        word = "Hello" if style == "formal" else "Hey"
        return df.with_columns(
            pl.concat_str([pl.lit(f"{word}, "), pl.col(name_col)]).alias("greeting")
        )
