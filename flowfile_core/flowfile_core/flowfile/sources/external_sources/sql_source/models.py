from pydantic import BaseModel, SecretStr
from flowfile_core.schemas.input_schema import DatabaseConnection, NodeDatabaseReader, FullDatabaseConnection


class ExtDatabaseConnection(DatabaseConnection):
    """Database connection configuration with password handling."""
    password: str = None


class DatabaseExternalReadSettings(BaseModel):
    """Settings for SQL source."""
    connection: ExtDatabaseConnection
    query: str
    flowfile_flow_id: int = 1
    flowfile_node_id: int | str = -1

    @classmethod
    def create_from_from_node_database_reader(cls, node_database_reader: NodeDatabaseReader,
                                              password: SecretStr,
                                              query: str,
                                              database_reference_settings: FullDatabaseConnection = None) -> 'DatabaseExternalReadSettings':
        """
        Create DatabaseExternalReadSettings from NodeDatabaseReader.
        Args:
            node_database_reader (NodeDatabaseReader): an instance of NodeDatabaseReader
            password (SecretStr): the password for the database connection
            query (str): the SQL query to be executed
            database_reference_settings (FullDatabaseConnection): optional database reference settings
        Returns:
            DatabaseExternalReadSettings: an instance of DatabaseExternalReadSettings
        """
        if node_database_reader.database_settings.connection_mode == "inline":
            database_connection = node_database_reader.database_settings.database_connection.model_dump()
        else:
            database_connection = {k: v for k, v in database_reference_settings.model_dump().items() if k != "password"}

        ext_database_connection = ExtDatabaseConnection(**database_connection,
                                                        password=password.get_secret_value())
        return cls(connection=ext_database_connection,
                   query=query,
                   flowfile_flow_id=node_database_reader.flow_id,
                   flowfile_node_id=node_database_reader.node_id)
