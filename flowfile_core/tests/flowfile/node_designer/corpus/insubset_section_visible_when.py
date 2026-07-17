import polars as pl

from flowfile import node_designer as nd


class GreetingSettings(nd.NodeSettings):
    main: nd.Section = nd.Section(
        title="Greeting",
        name_column=nd.ColumnSelector(
            label="Name column",
            required=True,
        ),
        show_advanced=nd.ToggleSwitch(
            label="Customize greeting",
        ),
    )
    advanced: nd.Section = nd.Section(
        title="Advanced",
        visible_when=nd.VisibleWhen(
            field="main.show_advanced",
        ),
        word=nd.TextInput(
            label="Greeting word",
            default="Hello",
        ),
    )
    inverted: nd.Section = nd.Section(
        title="Basic",
        visible_when=nd.VisibleWhen(
            field="main.show_advanced",
            equals=False,
        ),
        note=nd.TextInput(label="Note"),
    )


class GreetingNode(nd.CustomNodeBase):
    node_name: str = "Greeting"

    settings_schema: GreetingSettings = GreetingSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
