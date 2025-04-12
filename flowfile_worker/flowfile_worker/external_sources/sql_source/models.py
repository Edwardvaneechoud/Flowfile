from typing import Optional, Literal
from pydantic import BaseModel, SecretStr
from flowfile_worker.secrets import decrypt_secret


class DataBaseConnection(BaseModel):
    """Database connection configuration with secure password handling."""
    username: Optional[str] = None
    password: Optional[SecretStr] = None  # Encrypted password
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None  # The database name
    database_type: str = "postgresql"  # Database type (postgresql, mysql, etc.)
    url: Optional[str] = None

    def get_decrypted_secret(self) -> SecretStr:
        return decrypt_secret(self.password.get_secret_value())

    def create_uri(self) -> str:
        """
        Creates a database URI based on the connection details.
        If url is provided, it returns that directly.
        Otherwise, it constructs a URI from the individual components.

        Returns:
            str: The database URI
        """
        # If URL is already provided, use it
        if self.url:
            return self.url

        # Validate that required fields are present
        if not all([self.host, self.database_type]):
            raise ValueError("Host and database type are required to create a URI")

        # Create credential part if username is provided
        credentials = ""
        if self.username:
            credentials = self.username
            if self.password:
                # Get the raw password string from SecretStr
                password_value = decrypt_secret(self.password.get_secret_value()).get_secret_value()
                credentials += f":{password_value}"
            credentials += "@"

        # Create port part if port is provided
        port_section = ""
        if self.port:
            port_section = f":{self.port}"
        if self.database:

            base_uri = f"{self.database_type}://{credentials}{self.host}{port_section}/{self.database}"
        else:
            base_uri = f"{self.database_type}://{credentials}{self.host}{port_section}"
        return base_uri


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
    if_exists: Literal['append', 'replace', 'fail'] = 'append'
    flowfile_flow_id: int = 1
    flowfile_node_id: int | str = -1
