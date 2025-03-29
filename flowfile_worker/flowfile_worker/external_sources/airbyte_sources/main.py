from ast import literal_eval
import polars as pl
from typing import Optional
import airbyte as ab
import os
from airbyte import exceptions as exc


from flowfile_worker.configs import logger
from flowfile_worker.external_sources.airbyte_sources.models import AirbyteSettings
from flowfile_worker.external_sources.airbyte_sources.cache_manager import DuckDBCacheManager


class AirbyteGetter:
    stream: str
    source_name: str
    _type: str
    _airbyte_module = None
    _enforce_full_refresh: Optional[bool] = True
    version: Optional[str] = None

    def __init__(self, airbyte_settings: AirbyteSettings):
        self._airbyte_response = None
        self.stream = airbyte_settings.stream
        self.source_name = airbyte_settings.source_name
        self._enforce_full_refresh = airbyte_settings.enforce_full_refresh
        self.cache_manager = DuckDBCacheManager()
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

    def __call__(self) -> pl.DataFrame:
        with self.cache_manager.get_cache() as cache:
            if self.read_result is None:
                # Lazy import airbyte

                try:
                    source = ab.get_source(
                        name=self.source_name,
                        config=self.config,
                        streams=self.stream,
                        docker_image=True,
                        version=self.version
                    )

                    logger.debug(f'Starting to load data for {self.stream}')  # Changed to debug level

                    self.read_result = source.read(
                        cache=cache,
                        force_full_refresh=self._enforce_full_refresh
                    )

                except Exception as e:
                    logger.error(f"Error during source operation: {str(e)}")
                    raise

            df = self.read_result[self.stream].to_pandas()
            drop_cols = [c for c in df.columns if c.startswith('_airbyte')]
            df.drop(drop_cols, axis=1, inplace=True)
            return pl.from_pandas(df)


def read_airbyte_source(airbyte_settings: AirbyteSettings) -> pl.DataFrame:
    """
    Read data from an Airbyte source and return it as a Polars DataFrame.
    Args:
        airbyte_settings (): The settings for the Airbyte source.

    Returns: The data as a Polars DataFrame.
    """
    airbyte_getter = AirbyteGetter(airbyte_settings)
    logger.info('Getting data from Airbyte')
    data = airbyte_getter()
    logger.info('Data retrieved from Airbyte')
    return data
