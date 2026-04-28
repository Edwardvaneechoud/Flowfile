# This file was auto-generated to provide type information for flowfile_frame.cloud_storage.secret_manager
# DO NOT MODIFY THIS FILE MANUALLY
# Run `python flowfile_frame/submodule_stub_generator.py` to regenerate
from __future__ import annotations

from typing import Any, Callable, Iterable, Optional, Union

from flowfile_core.auth.jwt import create_access_token, get_current_user_sync
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import delete_cloud_connection, get_all_cloud_connections_interface, store_cloud_connection
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection, FullCloudStorageConnectionInterface

def create_cloud_storage_connection(connection: FullCloudStorageConnection) -> None: ...

def create_cloud_storage_connection_if_not_exists(connection: FullCloudStorageConnection) -> None: ...

def del_cloud_storage_connection(connection_name: str) -> None: ...

def get_all_available_cloud_storage_connections() -> list[flowfile_core.schemas.cloud_storage_schemas.FullCloudStorageConnectionInterface]: ...

def get_current_user_id() -> int | None: ...

