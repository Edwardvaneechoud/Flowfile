"""FastAPI routes for managing Google Analytics 4 connections.

Mirrors ``cloud_connections.py``. The ``/test`` endpoint validates the
service-account JSON by parsing it and attempting a token refresh — no GA
API call is made (keeps the test fast and free of property-level IAM needs).
"""

import json

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, SecretStr
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs import logger
from flowfile_core.database.connection import get_db
from flowfile_core.flowfile.database_connection_manager.ga_connections import (
    delete_ga_connection,
    get_all_ga_connections_interface,
    get_ga_connection,
    store_ga_connection,
    update_ga_connection,
)
from flowfile_core.schemas.google_analytics_schemas import (
    FullGoogleAnalyticsConnection,
    FullGoogleAnalyticsConnectionInterface,
)

router = APIRouter()


class GoogleAnalyticsConnectionTestRequest(BaseModel):
    service_account_json: SecretStr


class GoogleAnalyticsConnectionTestResponse(BaseModel):
    success: bool
    message: str


@router.post("/ga_connection", tags=["ga_connections"])
def create_ga_connection_endpoint(
    input_connection: FullGoogleAnalyticsConnection,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Create a new Google Analytics connection. The service-account JSON is
    encrypted with the user's derived key before being written to the DB."""
    logger.info(f"Create GA connection {input_connection.connection_name}")
    try:
        store_ga_connection(db, input_connection, current_user.id)
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    except Exception as e:
        logger.error(e)
        raise HTTPException(422, str(e)) from e
    return {"message": "Google Analytics connection created successfully"}


@router.put("/ga_connection", tags=["ga_connections"])
def update_ga_connection_endpoint(
    input_connection: FullGoogleAnalyticsConnection,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Update a GA connection. Sending an empty ``service_account_json`` keeps
    the existing key (matches the cloud-connection UX)."""
    logger.info(f"Update GA connection {input_connection.connection_name}")
    try:
        update_ga_connection(db, input_connection, current_user.id)
    except ValueError as e:
        raise HTTPException(404, str(e)) from e
    except Exception as e:
        logger.error(e)
        raise HTTPException(422, str(e)) from e
    return {"message": "Google Analytics connection updated successfully"}


@router.delete("/ga_connection", tags=["ga_connections"])
def delete_ga_connection_endpoint(
    connection_name: str,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Delete a GA connection and its encrypted service-account Secret."""
    logger.info(f"Delete GA connection {connection_name}")
    db_conn = get_ga_connection(db, connection_name, current_user.id)
    if db_conn is None:
        raise HTTPException(404, "Google Analytics connection not found")
    delete_ga_connection(db, connection_name, current_user.id)
    return {"message": "Google Analytics connection deleted successfully"}


@router.get(
    "/ga_connections",
    tags=["ga_connections"],
    response_model=list[FullGoogleAnalyticsConnectionInterface],
)
def list_ga_connections(
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> list[FullGoogleAnalyticsConnectionInterface]:
    """List all GA connections for the current user (no secrets)."""
    return get_all_ga_connections_interface(db, current_user.id)


@router.post(
    "/test",
    tags=["ga_connections"],
    response_model=GoogleAnalyticsConnectionTestResponse,
)
def test_ga_connection(
    request: GoogleAnalyticsConnectionTestRequest,
) -> GoogleAnalyticsConnectionTestResponse:
    """Validate a service-account JSON by parsing it and refreshing an OAuth token.

    The Google GA Data API is NOT called — that would require a valid property
    ID and the corresponding IAM grant. A token refresh proves the key is
    well-formed and the service account is active.
    """
    try:
        key_info = json.loads(request.service_account_json.get_secret_value())
    except json.JSONDecodeError as e:
        return GoogleAnalyticsConnectionTestResponse(success=False, message=f"Invalid JSON: {e.msg}")

    try:
        # Imported lazily so environments without the worker extras can still start.
        from google.auth.transport.requests import Request
        from google.oauth2 import service_account
    except ImportError as e:
        raise HTTPException(
            500,
            "google-auth is not installed. Install the 'google_analytics' extras to use this feature.",
        ) from e

    try:
        creds = service_account.Credentials.from_service_account_info(
            key_info, scopes=["https://www.googleapis.com/auth/analytics.readonly"]
        )
        creds.refresh(Request())
    except Exception as e:
        return GoogleAnalyticsConnectionTestResponse(success=False, message=f"Could not authenticate: {e}")

    return GoogleAnalyticsConnectionTestResponse(
        success=True,
        message=f"Authenticated as {key_info.get('client_email', 'unknown service account')}",
    )
