"""API connection management for flowfile_frame."""

from __future__ import annotations

from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.api_connection_manager.api_connections import (
    delete_api_connection as _delete,
)
from flowfile_core.flowfile.api_connection_manager.api_connections import (
    get_all_api_connections_interface,
    get_api_connection_schema,
    store_api_connection,
)
from flowfile_core.schemas.api_schemas import (
    ApiAuth,
    FullApiConnection,
    FullApiConnectionInterface,
)


def get_current_user_id() -> int:
    """Get the current user ID (defaults to 1 for single-user/local mode)."""
    return 1


def create_api_connection(
    connection_name: str,
    auth: ApiAuth,
    *,
    base_url: str | None = None,
    default_headers: dict[str, str] | None = None,
    verify_ssl: bool = True,
) -> None:
    """Create and store a new API connection.

    Args:
        connection_name: Unique name for this connection.
        auth: Authentication configuration (BearerAuth, ApiKeyAuth, etc.).
        base_url: Optional base URL to prepend to relative paths.
        default_headers: Optional default headers sent with every request.
        verify_ssl: Whether to verify SSL certificates.

    Raises:
        ValueError: If a connection with this name already exists.
    """
    user_id = get_current_user_id()
    connection = FullApiConnection(
        connection_name=connection_name,
        base_url=base_url,
        auth=auth,
        default_headers=default_headers,
        verify_ssl=verify_ssl,
    )
    with get_db_context() as db:
        store_api_connection(db, connection, user_id)


def create_api_connection_if_not_exists(
    connection_name: str,
    auth: ApiAuth,
    *,
    base_url: str | None = None,
    default_headers: dict[str, str] | None = None,
    verify_ssl: bool = True,
) -> None:
    """Create an API connection only if it doesn't already exist."""
    try:
        create_api_connection(
            connection_name,
            auth,
            base_url=base_url,
            default_headers=default_headers,
            verify_ssl=verify_ssl,
        )
    except ValueError:
        pass


def get_api_connection_by_name(connection_name: str) -> FullApiConnection | None:
    """Retrieve a stored API connection by name."""
    user_id = get_current_user_id()
    with get_db_context() as db:
        return get_api_connection_schema(db, connection_name, user_id)


def get_all_available_api_connections() -> list[FullApiConnectionInterface]:
    """List all stored API connections (without exposing secrets)."""
    user_id = get_current_user_id()
    with get_db_context() as db:
        return get_all_api_connections_interface(db, user_id)


def del_api_connection(connection_name: str) -> None:
    """Delete a stored API connection and its secrets."""
    user_id = get_current_user_id()
    with get_db_context() as db:
        _delete(db, connection_name, user_id)
