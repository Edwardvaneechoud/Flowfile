from typing import Literal

from pydantic import BaseModel, SecretStr

from flowfile_worker.secrets import decrypt_secret
from shared.sql_utils import construct_sql_uri, get_sqlalchemy_uri

_RESERVED_EXTRA_PARAMS = ("auth_method", "private_key", "private_key_passphrase")


class DataBaseConnection(BaseModel):
    """Database connection configuration with secure credential handling."""

    username: str | None = None
    password: SecretStr | None = None  # Encrypted password
    host: str | None = None
    port: int | None = None
    database: str | None = None
    database_type: str = "postgresql"  # Database type (postgresql, mysql, etc.)
    ssl_enabled: bool | None = False
    url: str | None = None
    extra_params: dict[str, str] | None = None  # Dialect-specific params (e.g. snowflake account/warehouse)
    auth_method: str | None = None  # None == password auth
    private_key: SecretStr | None = None  # Encrypted private key PEM (key-pair auth)
    private_key_passphrase: SecretStr | None = None  # Encrypted private-key passphrase

    def get_decrypted_secret(self) -> SecretStr:
        return decrypt_secret(self.password.get_secret_value())

    @staticmethod
    def _decrypt(value: SecretStr | None) -> str | None:
        if not value or not value.get_secret_value():
            return None
        return decrypt_secret(value.get_secret_value()).get_secret_value()

    def create_uri(self) -> str:
        """
        Creates a database URI based on the connection details.
        If url is provided, it returns that directly.
        Otherwise, it constructs a URI from the individual components.

        Returns:
            str: The database URI (base scheme, suitable for connectorx)
        """
        # Belt and braces: the named auth params must never collide with a splatted
        # extra_params key (core rejects these at its API boundary already).
        extra_params = {k: v for k, v in (self.extra_params or {}).items() if k not in _RESERVED_EXTRA_PARAMS}
        return construct_sql_uri(
            database_type=self.database_type,
            host=self.host,
            port=self.port,
            username=self.username,
            password=self._decrypt(self.password),
            database=self.database,
            url=self.url,
            ssl_enabled=bool(self.ssl_enabled),
            connect_timeout=10,
            auth_method=self.auth_method,
            private_key=self._decrypt(self.private_key),
            private_key_passphrase=self._decrypt(self.private_key_passphrase),
            **extra_params,
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
