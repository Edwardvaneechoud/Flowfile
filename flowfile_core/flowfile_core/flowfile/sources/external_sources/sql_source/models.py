from typing import Literal, Optional, TYPE_CHECKING
from pydantic import BaseModel, SecretStr
from flowfile_core.schemas.input_schema import (DatabaseConnection,
                                                NodeDatabaseReader,
                                                FullDatabaseConnection,
                                                NodeDatabaseWriter)
import base64
import polars as pl


class ExtDatabaseConnection(DatabaseConnection):
    """Database connection configuration with password handling."""
    password: str = None


class DatabaseExternalWriteSettings(BaseModel):
    """Settings for SQL sink."""
    connection: ExtDatabaseConnection
    table_name: str
    if_exists: Optional[Literal['append', 'replace', 'fail']] = 'append'
    flowfile_flow_id: int = 1
    flowfile_node_id: int | str = -1
    operation: str

    @classmethod
    def create_from_from_node_database_writer(cls, node_database_writer: NodeDatabaseWriter,
                                              password: str,
                                              table_name: str,
                                              lf: pl.LazyFrame,
                                              database_reference_settings: FullDatabaseConnection = None,
                                              ) -> 'DatabaseExternalWriteSettings':
        """
        Create DatabaseExternalWriteSettings from NodeDatabaseWriter.
        Args:
            node_database_writer (NodeDatabaseWriter): an instance of NodeDatabaseWriter
            password (str): the password for the database connection
            table_name (str): the table name to be used for writing
            lf (pl.LazyFrame): the LazyFrame to be written to the database
            database_reference_settings (FullDatabaseConnection): optional database reference settings
        Returns:
            DatabaseExternalReadSettings: an instance of DatabaseExternalReadSettings
        """
        if node_database_writer.database_write_settings.connection_mode == "inline":
            database_connection = node_database_writer.database_settings.database_connection.model_dump()
        else:
            database_connection = {k: v for k, v in database_reference_settings.model_dump().items() if k != "password"}

        ext_database_connection = ExtDatabaseConnection(**database_connection,
                                                        password=password)
        return cls(connection=ext_database_connection,
                   table_name=table_name,
                   if_exists=node_database_writer.database_write_settings.if_exists,
                   flowfile_flow_id=node_database_writer.flow_id,
                   flowfile_node_id=node_database_writer.node_id,
                   operation=base64.b64encode(lf.serialize()).decode())


class DatabaseExternalReadSettings(BaseModel):
    """Settings for SQL source."""
    connection: ExtDatabaseConnection
    query: str
    flowfile_flow_id: int = 1
    flowfile_node_id: int | str = -1

    @classmethod
    def create_from_from_node_database_reader(cls, node_database_reader: NodeDatabaseReader,
                                              password: str,
                                              query: str,
                                              database_reference_settings: FullDatabaseConnection = None) -> 'DatabaseExternalReadSettings':
        """
        Create DatabaseExternalReadSettings from NodeDatabaseReader.
        Args:
            node_database_reader (NodeDatabaseReader): an instance of NodeDatabaseReader
            password (str): the password for the database connection
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
                                                        password=password)
        return cls(connection=ext_database_connection,
                   query=query,
                   flowfile_flow_id=node_database_reader.flow_id,
                   flowfile_node_id=node_database_reader.node_id)
