import polars as pl

from flowfile import node_designer as nd


class UnpackSettings(nd.NodeSettings):
    main: nd.Section = nd.Section(
        title="Unpack",
        column=nd.ColumnSelector(label="Column to unpack", data_types=nd.Types.Complex, required=True),
    )


class UnpackNode(nd.CustomNodeBase):
    node_name: str = "Unpack Nested"
    settings_schema: UnpackSettings = UnpackSettings()
    example_inputs: list[dict[str, list]] = [
        {
            "station_id": ["a", "b"],
            "num_docks_available": [10, 4],
            "vehicle_types_available": [
                [{"vehicle_type_id": "1", "count": 3}, {"vehicle_type_id": "2", "count": 1}],
                [{"vehicle_type_id": "1", "count": 0}, {"vehicle_type_id": "2", "count": 2}],
            ],
            "meta": [{"region": "north", "tags": ["a", "b"]}, {"region": "south", "tags": []}],
        },
    ]
    example_settings: dict[str, dict] = {
        "main": {"column": "vehicle_types_available"},
    }

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        column = self.settings_schema.main.column.value
        return inputs[0].explode(column)
