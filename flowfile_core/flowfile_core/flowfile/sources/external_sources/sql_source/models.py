from pydantic import BaseModel, SecretStr
from flowfile_core.schemas.input_schema import DatabaseConnection, NodeDatabaseReader


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
                                              query: str) -> 'DatabaseExternalReadSettings':
        """
        Create DatabaseExternalReadSettings from NodeDatabaseReader.
        Args:
            node_database_reader (NodeDatabaseReader): an instance of NodeDatabaseReader
            password (SecretStr): the password for the database connection
            query (str): the SQL query to be executed

        Returns:
            DatabaseExternalReadSettings: an instance of DatabaseExternalReadSettings
        """
        ext_database_connection = ExtDatabaseConnection(**node_database_reader.database_settings.database_connection.model_dump(),
                                                        password=password.get_secret_value())
        return cls(connection=ext_database_connection,
                   query=query,
                   flowfile_flow_id=node_database_reader.flow_id,
                   flowfile_node_id=node_database_reader.node_id)
