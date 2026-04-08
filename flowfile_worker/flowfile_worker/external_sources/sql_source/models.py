from typing import Literal

from pydantic import BaseModel, SecretStr

from flowfile_worker.secrets import decrypt_secret
from shared.sql_utils import construct_sql_uri, get_sqlalchemy_uri


class DataBaseConnection(BaseModel):
    """Database connection configuration with secure password handling."""

    username: str | None = None
    password: SecretStr | None = None  # Encrypted password
    host: str | None = None
    port: int | None = None
    database: str | None = None  # The database name
    database_type: str = "postgresql"  # Database type (postgresql, mysql, etc.)
    url: str | None = None

    def get_decrypted_secret(self) -> SecretStr:
        return decrypt_secret(self.password.get_secret_value())

    def create_uri(self) -> str:
        """
        Creates a database URI based on the connection details.
        If url is provided, it returns that directly.
        Otherwise, it constructs a URI from the individual components.

        Returns:
            str: The database URI (base scheme, suitable for connectorx)
        """
        password_str = None
        if self.password:
            password_str = decrypt_secret(self.password.get_secret_value()).get_secret_value()
        return construct_sql_uri(
            database_type=self.database_type,
            host=self.host,
            port=self.port,
            username=self.username,
            password=password_str,
            database=self.database,
            url=self.url,
        )

    def create_sqlalchemy_uri(self) -> str:
        """
        Creates a SQLAlchemy-compatible database URI with driver suffix.

        connectorx uses base URI schemes (e.g. mysql://) while SQLAlchemy
        requires driver-specific schemes (e.g. mysql+pymysql://).

        Returns:
            str: The database URI with appropriate driver suffix for SQLAlchemy.
        """
        return get_sqlalchemy_uri(self.create_uri())


class DatabaseReadSettings(BaseModel):
    """Settings for SQL source."""

    connection: DataBaseConnection
    query: str
    flowfile_flow_id: int = 1
    flowfile_node_id: int | str = -1


class DatabaseWriteSettings(BaseModel):
    """Settings for SQL sink."""

    connection: DataBaseConnection
    table_name: str
    if_exists: Literal["append", "replace", "fail"] = "append"
    flowfile_flow_id: int = 1
    flowfile_node_id: int | str = -1
