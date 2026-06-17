from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

# Core modules
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs import logger
from flowfile_core.database.connection import get_db
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_cloud_connection,
    get_all_cloud_connections_interface,
    get_cloud_connection,
    store_cloud_connection,
    update_cloud_connection,
)
from flowfile_core.routes._connection_sharing import (
    authorize_connection_mutation,
    changed_target_fields,
    require_credentials_on_target_change,
)

# Schema and models
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection, FullCloudStorageConnectionInterface

# External dependencies
# File handling
router = APIRouter()


@router.post("/cloud_connection", tags=["cloud_connections"])
def create_cloud_storage_connection(
    input_connection: FullCloudStorageConnection,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """
    Create a new cloud storage connection.
    Parameters
        input_connection: FullCloudStorageConnection schema containing connection details
        current_user: User obtained from Depends(get_current_active_user)
        db: Session obtained from Depends(get_db)
    Returns
        Dict with a success message
    """
    logger.info(f"Create cloud connection {input_connection.connection_name}")
    try:
        store_cloud_connection(db, input_connection, current_user.id)
    except ValueError:
        raise HTTPException(422, "Connection name already exists") from None
    except Exception as e:
        logger.error(e)
        raise HTTPException(422, str(e)) from e
    return {"message": "Cloud connection created successfully"}


@router.put("/cloud_connection", tags=["cloud_connections"])
def update_cloud_storage_connection(
    input_connection: FullCloudStorageConnection,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Update an existing cloud storage connection (own, or group-shared with manage access)."""
    logger.info(f"Update cloud connection {input_connection.connection_name}")
    db_connection = get_cloud_connection(db, input_connection.connection_name, current_user.id)
    if db_connection is None:
        raise HTTPException(404, "Cloud connection not found")
    if authorize_connection_mutation(db, current_user, "cloud_connection", db_connection):
        changed = changed_target_fields(
            db_connection, input_connection, ("storage_type", "auth_method", "endpoint_url", "verify_ssl")
        )
        has_new_credentials = any(
            field is not None and field.get_secret_value()
            for field in (
                input_connection.aws_secret_access_key,
                input_connection.azure_account_key,
                input_connection.azure_client_secret,
                input_connection.azure_sas_token,
                input_connection.gcs_service_account_key,
            )
        )
        has_bundled_secrets = any(
            getattr(db_connection, column) is not None
            for column in (
                "aws_secret_access_key_id",
                "aws_session_token_id",
                "azure_account_key_id",
                "azure_client_secret_id",
                "azure_sas_token_id",
                "gcs_service_account_key_id",
            )
        )
        require_credentials_on_target_change(changed, has_new_credentials, has_bundled_secrets)
    try:
        # Owner's user_id keeps rotated secrets encrypted under the OWNER's key.
        update_cloud_connection(db, input_connection, db_connection.user_id)
    except ValueError:
        raise HTTPException(404, "Cloud connection not found") from None
    except Exception as e:
        logger.error(e)
        raise HTTPException(422, str(e)) from e
    return {"message": "Cloud connection updated successfully"}


@router.delete("/cloud_connection", tags=["cloud_connections"])
def delete_cloud_connection_with_connection_name(
    connection_name: str, current_user=Depends(get_current_active_user), db: Session = Depends(get_db)
):
    """
    Delete a cloud connection (own, or group-shared with manage access).
    """
    logger.info(f"Deleting cloud connection {connection_name}")
    db_connection = get_cloud_connection(db, connection_name, current_user.id)
    if db_connection is None:
        raise HTTPException(404, "Cloud connection connection not found")
    authorize_connection_mutation(db, current_user, "cloud_connection", db_connection)
    delete_cloud_connection(db, connection_name, db_connection.user_id)
    return {"message": "Cloud connection deleted successfully"}


@router.get("/cloud_connections", tags=["cloud_connection"], response_model=list[FullCloudStorageConnectionInterface])
def get_cloud_connections(
    db: Session = Depends(get_db), current_user=Depends(get_current_active_user)
) -> list[FullCloudStorageConnectionInterface]:
    """
    Get all cloud storage connections for the current user.
    Parameters
        db: Session obtained from Depends(get_db)
        current_user: User obtained from Depends(get_current_active_user)

    Returns
        List[FullCloudStorageConnectionInterface]
    """
    return get_all_cloud_connections_interface(db, current_user.id)
