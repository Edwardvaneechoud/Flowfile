import os
from ast import literal_eval
import polars as pl
from typing import Any, Dict, Generator, List, Optional
from flowfile_core.configs import logger
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.sources.external_sources.base_class import ExternalDataSource
from flowfile_core.flowfile.sources.external_sources.airbyte_sources.models import (
    AirbyteProperty, JsonSchema, AirbyteResponse, AirbyteSettings
)


class LazyAirbyteImporter:
    """Lazy importer for airbyte module."""
    _airbyte = None

    @classmethod
    def get_airbyte(cls):
        if cls._airbyte is None:
            logger.info("Importing airbyte module")
            import airbyte as ab
            cls._airbyte = ab
        return cls._airbyte


class AirbyteSource(ExternalDataSource):
    stream: str
    source_name: str
    cache_store: Optional['airbyte.results.ReadResult'] = None
    _type: str
    is_collected: bool
    _airbyte_response: Optional[AirbyteResponse] = None
    _airbyte_module = None
    _enforce_full_refresh: Optional[bool] = True
    version: Optional[str] = None

    def __init__(self, airbyte_settings: AirbyteSettings):
        self.is_collected = False
        self._airbyte_response = None
        self.stream = airbyte_settings.stream
        self.source_name = airbyte_settings.source_name
        self._enforce_full_refresh = airbyte_settings.enforce_full_refresh
        if hasattr(airbyte_settings, 'version'):
            self.version = airbyte_settings.version

        # Handle config
        if airbyte_settings.config_ref and not airbyte_settings.config:
            logger.info(f"Getting config from {airbyte_settings.config_ref}")
            config = literal_eval(os.environ.get(airbyte_settings.config_ref))
        else:
            logger.info(f"Using provided config")
            config = airbyte_settings.config

        if config is None:
            raise ValueError("Config must be provided")

        self.config = config
        self._type = 'airbyte'
        self.read_result = None

        # Only load source if fields aren't provided
        if not airbyte_settings.fields:
            self.load_source(airbyte_settings)
        else:
            logger.info('Using provided schema')
            self.schema = [
                FlowfileColumn.from_input(column_name=col.name, data_type=col.data_type)
                for col in airbyte_settings.fields
            ]

    def load_source(self, airbyte_settings: AirbyteSettings):
        logger.info(f"Loading source {self.source_name}")
        if airbyte_settings.fields is not None and len(airbyte_settings.fields) > 0:
            logger.info('Using provided schema')
            self.schema = [
                FlowfileColumn.from_input(column_name=col.name, data_type=col.data_type)
                for col in airbyte_settings.fields
            ]
        else:
            logger.info('Using airbyte schema')
            logger.info(f"Loading source {self.source_name}")
            _ = self.airbyte_response

    @property
    def airbyte_response(self) -> AirbyteResponse:
        if self._airbyte_response is None:
            # Lazy import airbyte
            ab = LazyAirbyteImporter.get_airbyte()

            source = ab.get_source(
                name=self.source_name,
                config=self.config,
                streams=self.stream,
                docker_image=True,
                version=self.version
            )

            try:
                source.check()
            except Exception:
                logger.warning('Source check failed, trying to continue')

            logger.info(f'Source check passed, starting to load data for {self.stream}')

            json_schema = source.get_stream_json_schema(self.stream)['properties']
            properties = [
                AirbyteProperty(name=name, json_schema=JsonSchema(**schema))
                for name, schema in json_schema.items()
            ]

            logger.info(f"Loaded source {self.source_name}")
            self._airbyte_response = AirbyteResponse(properties=properties, source=source)
            self.schema = self.parse_schema(self._airbyte_response)

        return self._airbyte_response

    def get_initial_data(self):
        return []

    def get_iter(self) -> Generator[Dict[str, Any], None, None]:
        logger.warning('Getting data in iteration, this is suboptimal')
        data = self.data_getter()
        for row in data:
            yield row
        self.is_collected = True

    def get_sample(self, n: int = 10000):
        logger.warning('Getting sample in iteration, this is suboptimal')
        data = self.get_iter()
        for i in range(n):
            try:
                yield next(data)
            except StopIteration:
                break

    @staticmethod
    def parse_schema(airbyte_response: AirbyteResponse) -> List[FlowfileColumn]:
        return airbyte_response.get_flow_file_columns()

    def get_df(self):
        if self.read_result is None:
            self.read_result = self.airbyte_response.source.read()

        df = self.read_result[self.stream].to_pandas()
        drop_cols = [c for c in df.columns if c.startswith('_airbyte')]
        df.drop(drop_cols, axis=1, inplace=True)
        return df

    def get_pl_df(self) -> pl.DataFrame:
        self.is_collected = True
        return pl.from_pandas(self.get_df())

    def data_getter(self) -> List[Dict]:
        return self.get_df().to_dict(orient='records')

    @classmethod
    def create_from_frontend_input(cls, config: Any, stream_name: str, source_name: str):
        # Implementation details to be added
        pass
