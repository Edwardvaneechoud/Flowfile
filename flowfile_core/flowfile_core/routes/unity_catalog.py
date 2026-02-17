"""API routes for Unity Catalog connection management and table browsing.

Provides endpoints for:
- CRUD operations on UC connections
- Testing UC server connectivity
- Browsing catalogs, schemas, and tables
- Resolving table metadata for use in flow nodes
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException

from flowfile_core.auth.dependencies import get_current_active_user
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_uc_connection,
    get_all_uc_connections_interface,
    get_local_uc_connection,
    store_uc_connection,
)
from flowfile_core.unity_catalog.client import UnityCatalogClient, UnityCatalogError
from flowfile_core.unity_catalog.schemas import (
    CatalogInfo,
    SchemaInfo,
    TableInfo,
    UnityCatalogConnectionInput,
    UnityCatalogConnectionInterface,
)

logger = logging.getLogger(__name__)

router = APIRouter()


def _get_client_for_connection(connection_name: str, user_id: int) -> UnityCatalogClient:
    """Helper to create a UC client from a stored connection."""
    result = get_local_uc_connection(connection_name, user_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"UC connection '{connection_name}' not found")
    db_conn, auth_token = result
    return UnityCatalogClient(
        server_url=db_conn.server_url,
        auth_token=auth_token,
    )


# ---------------------------------------------------------------------------
# Connection CRUD
# ---------------------------------------------------------------------------


@router.post("/uc_connection")
def create_uc_connection(
    connection: UnityCatalogConnectionInput,
    current_user=Depends(get_current_active_user),
):
    """Create a new Unity Catalog connection."""
    with get_db_context() as db:
        try:
            store_uc_connection(db, connection, current_user.id)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
    return {"message": "Unity Catalog connection created successfully"}


@router.get("/uc_connections", response_model=list[UnityCatalogConnectionInterface])
def list_uc_connections(current_user=Depends(get_current_active_user)):
    """List all Unity Catalog connections for the current user."""
    with get_db_context() as db:
        return get_all_uc_connections_interface(db, current_user.id)


@router.delete("/uc_connection")
def remove_uc_connection(
    connection_name: str,
    current_user=Depends(get_current_active_user),
):
    """Delete a Unity Catalog connection."""
    with get_db_context() as db:
        delete_uc_connection(db, connection_name, current_user.id)
    return {"message": "Unity Catalog connection deleted successfully"}


# ---------------------------------------------------------------------------
# Connection Test
# ---------------------------------------------------------------------------


@router.post("/test")
def test_uc_connection(
    connection: UnityCatalogConnectionInput,
    current_user=Depends(get_current_active_user),
):
    """Test connectivity to a Unity Catalog server without saving the connection."""
    auth_token = connection.auth_token.get_secret_value() if connection.auth_token else None
    try:
        client = UnityCatalogClient(
            server_url=connection.server_url,
            auth_token=auth_token,
        )
        success = client.test_connection()
        client.close()
        if success:
            return {"success": True, "message": "Connection successful"}
        return {"success": False, "message": "Could not reach Unity Catalog server"}
    except Exception as exc:
        return {"success": False, "message": str(exc)}


# ---------------------------------------------------------------------------
# Browse: Catalogs / Schemas / Tables
# ---------------------------------------------------------------------------


@router.get("/browse/catalogs", response_model=list[CatalogInfo])
def browse_catalogs(
    connection_name: str,
    current_user=Depends(get_current_active_user),
):
    """List all catalogs in a UC server."""
    client = _get_client_for_connection(connection_name, current_user.id)
    try:
        return client.list_catalogs()
    except UnityCatalogError as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    finally:
        client.close()


@router.get("/browse/schemas", response_model=list[SchemaInfo])
def browse_schemas(
    connection_name: str,
    catalog_name: str,
    current_user=Depends(get_current_active_user),
):
    """List all schemas in a catalog."""
    client = _get_client_for_connection(connection_name, current_user.id)
    try:
        return client.list_schemas(catalog_name)
    except UnityCatalogError as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    finally:
        client.close()


@router.get("/browse/tables", response_model=list[TableInfo])
def browse_tables(
    connection_name: str,
    catalog_name: str,
    schema_name: str,
    current_user=Depends(get_current_active_user),
):
    """List all tables in a schema."""
    client = _get_client_for_connection(connection_name, current_user.id)
    try:
        return client.list_tables(catalog_name, schema_name)
    except UnityCatalogError as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    finally:
        client.close()


@router.get("/browse/table_info", response_model=TableInfo)
def get_table_info(
    connection_name: str,
    full_table_name: str,
    current_user=Depends(get_current_active_user),
):
    """Get detailed metadata for a specific table."""
    client = _get_client_for_connection(connection_name, current_user.id)
    try:
        return client.get_table(full_table_name)
    except UnityCatalogError as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    finally:
        client.close()
