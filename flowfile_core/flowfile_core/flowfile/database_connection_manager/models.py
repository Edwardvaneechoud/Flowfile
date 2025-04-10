from pydantic import BaseModel


class DatabaseConnectionOutput(BaseModel):
    id: int
    name: str
    type: str
    host: str
    port: int
    database: str
    username: str
    password: str | None = None  # Password can be None if not stored in the database
    ssl_mode: str | None = None  # SSL mode can be None if not applicable
    created_at: str
    updated_at: str