# This file was auto-generated to provide type information for flowfile_frame.database.connection_manager
# DO NOT MODIFY THIS FILE MANUALLY
# Run `python flowfile_frame/submodule_stub_generator.py` to regenerate
from __future__ import annotations

from typing import Any, Callable, Iterable, Optional, Union

from typing import Literal
from pydantic import SecretStr
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import get_database_connection, get_database_connection_schema, store_database_connection
from flowfile_core.schemas.input_schema import FullDatabaseConnection, FullDatabaseConnectionInterface
from flowfile_core.database.models import DatabaseConnection as DBConnectionModel
from flowfile_core.database.models import Secret

def create_database_connection(connection_name: str, database_type: Literal['postgresql', 'mysql', 'sqlite', 'mssql', 'oracle']='postgresql', host: str | None=None, port: int | None=None, database: str | None=None, username: str | None=None, password: str | pydantic.types.SecretStr | None=None, ssl_enabled: bool=False, url: str | None=None) -> FullDatabaseConnection: ...

def create_database_connection_if_not_exists(connection_name: str, database_type: Literal['postgresql', 'mysql', 'sqlite', 'mssql', 'oracle']='postgresql', host: str | None=None, port: int | None=None, database: str | None=None, username: str | None=None, password: str | pydantic.types.SecretStr | None=None, ssl_enabled: bool=False, url: str | None=None) -> FullDatabaseConnection: ...

def del_database_connection(connection_name: str) -> bool: ...

def get_all_available_database_connections() -> list[flowfile_core.schemas.input_schema.FullDatabaseConnectionInterface]: ...

def get_current_user_id() -> int: ...

def get_database_connection_by_name(connection_name: str) -> flowfile_core.schemas.input_schema.FullDatabaseConnection | None: ...

