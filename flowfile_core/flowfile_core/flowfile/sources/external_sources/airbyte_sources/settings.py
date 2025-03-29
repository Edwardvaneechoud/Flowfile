from typing import List, Dict, Optional, Any, Type
from flowfile_core.configs import logger
from flowfile_core.flowfile.sources.external_sources.airbyte_sources.models import AirbyteConfigTemplate, \
    AirbyteSettings
from flowfile_core.schemas.external_sources.airbyte_schemas import AirbyteConfig
from flowfile_core.flowfile.connection_manager import connection_manager
from flowfile_core.flowfile.connection_manager._connection_manager import Connection


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


class AirbyteConfigHandler:
    _available_connectors: Optional[List[str]] = None
    configs: Dict[str, AirbyteConfigTemplate]
    _airbyte = None

    def __init__(self):
        self.configs = {}
        self._available_connectors = None

    @property
    def airbyte(self):
        """Lazy load airbyte module when needed."""
        if self._airbyte is None:
            self._airbyte = LazyAirbyteImporter.get_airbyte()
        return self._airbyte

    @property
    def available_connectors(self) -> List[str]:
        if self._available_connectors is None:
            self._available_connectors = [
                c.replace('source-', '')
                for c in self.airbyte.get_available_connectors()
                if c.startswith('source-')
            ]
        return self._available_connectors

    @property
    def available_configs(self) -> List[str]:
        return list(self.configs.keys())

    def get_config(self, config_name: str) -> AirbyteConfigTemplate:
        """Get configuration for a specific source."""
        logger.info(f"Getting config for {config_name}")

        if config_name not in self.configs:
            try:
                source = self.airbyte.get_source(
                    name=config_name,
                    install_if_missing=True,
                    docker_image=True
                )
                logger.info(f"Got source {config_name}")

                self.configs[config_name] = AirbyteConfigTemplate(
                    config_spec=source.config_spec,
                    docs_url=source.docs_url,
                    source_name=config_name
                )
            except Exception as e:
                logger.error(f"Error getting config for {config_name}: {str(e)}")
                raise

        return self.configs[config_name]

    def get_available_streams(self, config_name: str, config_settings: Any) -> List[str]:
        """Get available streams for a specific configuration."""
        if config_name not in self.configs:
            raise ValueError(f"Config {config_name} not found")

        logger.info(f'Getting available streams for {config_name}')

        try:
            source = self.airbyte.get_source(
                name=config_name,
                install_if_missing=True,
                config=config_settings,
                docker_image=True
            )
            streams = source.get_available_streams()
            if len(streams) == 0 or streams is None:
                raise ValueError(f"No streams found for {config_name}")
            self.configs[config_name].available_streams = streams
            return self.configs[config_name].available_streams

        except Exception as e:
            logger.error(f"Error getting streams for {config_name}: {str(e)}")
            raise


# Create singleton instance
airbyte_config_handler = AirbyteConfigHandler()


class AirbyteHandler:
    """Handler for Airbyte configurations and connections."""
    config: AirbyteConfig

    def __init__(self, airbyte_config: AirbyteConfig):
        self.config = airbyte_config

    def set_airbyte_config(self, airbyte_config_in: AirbyteConfig) -> AirbyteConfig:
        """Update the current configuration."""
        self.config.mapped_config_spec = airbyte_config_in.mapped_config_spec
        self.config.parsed_config = airbyte_config_in.parsed_config
        return airbyte_config_in

    def get_available_streams(self) -> List[str]:
        """Get available streams for the current configuration."""
        config_template = airbyte_config_handler.configs.get(self.config.full_source_name)

        if not config_template:
            logger.warning(
                f"Config {self.config.source_name} not found, trying to recreate the config"
            )
            try:
                _ = airbyte_config_handler.get_config(self.config.full_source_name)
                logger.info(f"Config {self.config.source_name} recreated")
            except Exception as e:
                logger.error(f"Error recreating config: {str(e)}")
                raise

        return airbyte_config_handler.get_available_streams(
            self.config.full_source_name,
            self.config.mapped_config_spec
        )

    def save_connection(self, connection_name: str) -> None:
        """Save the current configuration as a connection."""
        connection = Connection(
            group=self.config.source_name,
            name=connection_name,
            config_setting=self.config,
            type='airbyte'
        )

        connection_manager.add_connection(
            self.config.source_name,
            connection_name=connection_name,
            connection=connection
        )


def airbyte_settings_from_config(airbyte_config: AirbyteConfig, flow_id: int, node_id: int|str) -> AirbyteSettings:
    """Create AirbyteSettings from an AirbyteConfig."""
    if airbyte_config.config_mode == 'key_vault':
        connection = connection_manager.get_connection(
            connection_group=airbyte_config.source_name,
            connection_name=airbyte_config.connection_name
        )
        config = connection.config_setting.mapped_config_spec
    else:
        config = airbyte_config.mapped_config_spec

    return AirbyteSettings(
        source_name=airbyte_config.full_source_name,
        stream=airbyte_config.selected_stream,
        config=config,
        flowfile_flow_id=flow_id,
        flowfile_node_id=node_id,
        version=airbyte_config.version
    )
