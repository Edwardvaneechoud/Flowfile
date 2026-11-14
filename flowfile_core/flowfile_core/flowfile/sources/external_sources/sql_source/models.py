import base64
from typing import Annotated, Any, Literal

import polars as pl
from pydantic import BaseModel, BeforeValidator, Field, PlainSerializer

from flowfile_core.flowfile.sources.external_sources.sql_source.utils import (
    dialect_uri_options,
    refuse_inline_snowflake,
)
from flowfile_core.schemas.input_schema import (
    DatabaseConnection,
    FullDatabaseConnection,
    NodeDatabaseReader,
    NodeDatabaseWriter,
    SecretRef,
)
from shared.db_dialects import get_dialect_or_generic


def _connection_fields(connection_mode: str | None, inline: DatabaseConnection | None, reference) -> dict[str, Any]:
    """The worker connection's fields, its password left out (the caller adds the encrypted one).

    A saved Snowflake connection travels as its URL without the secret, in ``url``; the worker merges the decrypted
    password into it (``DbDialect.with_credentials``), so the wire model carries no Snowflake field of its own.
    """
    if connection_mode == "inline":
        refuse_inline_snowflake(inline)
        return inline.model_dump()
    fields = {k: v for k, v in reference.model_dump().items() if k in ExtDatabaseConnection.model_fields}
    fields.pop("password", None)
    options = dialect_uri_options(reference)
    if options:
        fields["url"] = get_dialect_or_generic(reference.database_type).build_uri(
            host=reference.host,
            port=reference.port,
            username=reference.username,
            password=None,
            database=reference.database,
            **options,
        )
    return fields


# Custom type for bytes that serializes to/from base64 string in JSON
def _decode_bytes(v: Any) -> bytes:
    if isinstance(v, bytes):
        return v
    if isinstance(v, str):
        return base64.b64decode(v)
    raise ValueError(f"Expected bytes or base64 string, got {type(v)}")


Base64Bytes = Annotated[
    bytes,
    BeforeValidator(_decode_bytes),
    PlainSerializer(lambda x: base64.b64encode(x).decode("ascii"), return_type=str),
]


class ExtDatabaseConnection(DatabaseConnection):
    """Database connection configuration with password handling.

    ``password_ref`` names the secret core resolved into ``password``; it is excluded from a dump, so a worker, whose
    connection refuses an unknown field, is never sent it.
    """

    password_ref: SecretRef | None = Field(default=None, exclude=True)
    password: str | None = None
    ssl_enabled: bool | None = False


class DatabaseExternalWriteSettings(BaseModel):
    """Settings for SQL sink."""

    connection: ExtDatabaseConnection
    table_name: str
    if_exists: Literal["append", "replace", "fail"] | None = "append"
    flowfile_flow_id: int = 1
    flowfile_node_id: int | str = -1
    operation: Base64Bytes  # Accepts bytes or base64 string, serializes to base64

    @classmethod
    def create_from_from_node_database_writer(
        cls,
        node_database_writer: NodeDatabaseWriter,
        password: str,
        table_name: str,
        lf: pl.LazyFrame,
        database_reference_settings: FullDatabaseConnection = None,
    ) -> "DatabaseExternalWriteSettings":
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
        write_settings = node_database_writer.database_write_settings
        database_connection = _connection_fields(
            write_settings.connection_mode, write_settings.database_connection, database_reference_settings
        )

        ext_database_connection = ExtDatabaseConnection(**database_connection, password=password)
        return cls(
            connection=ext_database_connection,
            table_name=table_name,
            if_exists=node_database_writer.database_write_settings.if_exists,
            flowfile_flow_id=node_database_writer.flow_id,
            flowfile_node_id=node_database_writer.node_id,
            operation=lf.serialize(),  # Pass raw bytes, Base64Bytes handles encoding
        )


class DatabaseExternalReadSettings(BaseModel):
    """Settings for SQL source."""

    connection: ExtDatabaseConnection
    query: str
    flowfile_flow_id: int = 1
    flowfile_node_id: int | str = -1

    @classmethod
    def create_from_from_node_database_reader(
        cls,
        node_database_reader: NodeDatabaseReader,
        password: str,
        query: str,
        database_reference_settings: FullDatabaseConnection = None,
    ) -> "DatabaseExternalReadSettings":
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
        read_settings = node_database_reader.database_settings
        database_connection = _connection_fields(
            read_settings.connection_mode, read_settings.database_connection, database_reference_settings
        )

        ext_database_connection = ExtDatabaseConnection(**database_connection, password=password)
        return cls(
            connection=ext_database_connection,
            query=query,
            flowfile_flow_id=node_database_reader.flow_id,
            flowfile_node_id=node_database_reader.node_id,
        )
