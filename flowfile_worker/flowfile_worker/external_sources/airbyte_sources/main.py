import os
from ast import literal_eval
import polars as pl
from typing import Optional
from flowfile_worker.configs import logger
from flowfile_worker.external_sources.airbyte_sources.models import (
    AirbyteProperty, JsonSchema, AirbyteResponse, AirbyteSettings
)
from airbyte import get_source, DuckDBCache
from time import sleep


import os
os.getcwd()

cache = DuckDBCache(
    db_path='/Users/edwardvanechoud/.flowfile/.cache_worker1',
    schema_name="main",
    clean_up=True
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
self =         airbyte_settings = AirbyteSettings(**{'source_name': 'source-faker', 'stream': 'users', 'config_ref': None,
                                              'config': {'count': 1000, 'seed': -1, 'records_per_slice': 1000,
                                                         'always_updated': True, 'parallelism': 4}, 'fields': None,
                                              'enforce_full_refresh': True})

def test_read_airbyte():
    try:
        airbyte_settings = AirbyteSettings(**{'source_name': 'source-faker', 'stream': 'users', 'config_ref': None,
                                              'config': {'count': 1000, 'seed': -1, 'records_per_slice': 1000,
                                                         'always_updated': True, 'parallelism': 4}, 'fields': None,
                                              'enforce_full_refresh': True})
        data = read_airbyte_source(airbyte_settings)
        del airbyte_settings

    except Exception as e:
        print(e)
        assert False


def read_airbyte_source(airbyte_settings: AirbyteSettings) -> pl.DataFrame:
    airbyte_getter = AirbyteGetter(airbyte_settings)
    return airbyte_getter()


class AirbyteGetter:
    stream: str
    source_name: str
    _type: str
    _airbyte_module = None
    _enforce_full_refresh: Optional[bool] = True

    def __init__(self, airbyte_settings: AirbyteSettings):
        self._airbyte_response = None
        self.stream = airbyte_settings.stream
        self.source_name = airbyte_settings.source_name
        self._enforce_full_refresh = airbyte_settings.enforce_full_refresh

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

    @property
    def airbyte_response(self) -> AirbyteResponse:
        if self._airbyte_response is None:
            # Lazy import airbyte
            ab = LazyAirbyteImporter.get_airbyte()
            from pathlib import Path
            source = ab.get_source(
                name=self.source_name,
                config=self.config,
                streams=self.stream,
                docker_image=True,
                install_root=Path('/Users/edwardvanechoud/.flowfile')
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

        return self._airbyte_response

    def __call__(self) -> pl.DataFrame:
        if self.read_result is None:
            self.read_result = self.airbyte_response.source.read(cache=False, force_full_refresh=self._enforce_full_refresh)

        df = self.read_result[self.stream].to_pandas()
        drop_cols = [c for c in df.columns if c.startswith('_airbyte')]
        df.drop(drop_cols, axis=1, inplace=True)
        return pl.from_pandas(df)


# if __name__ == '__main__':
#     test_read_airbyte()
#     sleep(100)