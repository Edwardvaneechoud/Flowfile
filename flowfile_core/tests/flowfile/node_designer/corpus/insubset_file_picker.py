import polars as pl
from flowfile import node_designer as nd


class FileReaderSettings(nd.NodeSettings):
    source: nd.Section = nd.Section(
        title="Source",
        input_file=nd.FilePicker(
            label="Input file",
            placeholder="/data/input.csv",
            file_types=[
                "csv",
                "parquet",
            ],
        ),
        target_dir=nd.FilePicker(
            label="Target",
            mode="create",
            allow_directory=True,
        ),
    )


class FileReader(nd.CustomNodeBase):
    node_name: str = "File Reader"
    settings_schema: FileReaderSettings = FileReaderSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        path = self.settings_schema.source.input_file.value
        return pl.scan_csv(path)
