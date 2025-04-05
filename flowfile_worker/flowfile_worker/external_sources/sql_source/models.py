from typing import Optional, Dict, Any
from pydantic import BaseModel, SecretStr


class DataBaseConnection(BaseModel):
    """Database connection configuration with secure password handling."""
    username: Optional[str] = None
    password: Optional[SecretStr] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None  # The database name
    db_type: str = "postgresql"  # Database type (postgresql, mysql, etc.)
    url: Optional[str] = None

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
        if not all([self.host, self.db_type]):
            raise ValueError("Host and database type are required to create a URI")

        # Create credential part if username is provided
        credentials = ""
        if self.username:
            credentials = self.username
            if self.password:
                # Get the raw password string from SecretStr
                password_value = self.password.get_secret_value()
                credentials += f":{password_value}"
            credentials += "@"

        # Create port part if port is provided
        port_section = ""
        if self.port:
            port_section = f":{self.port}"
        if self.database:

            base_uri = f"{self.db_type}://{credentials}{self.host}{port_section}/{self.database}"
        else:
            base_uri = f"{self.db_type}://{credentials}{self.host}{port_section}"
        return base_uri


class SQLSourceSettings(BaseModel):
    """Settings for SQL source."""
    connection: DataBaseConnection
    query: str
    flowfile_flow_id: int = 1
    flowfile_node_id: int | str = -1
