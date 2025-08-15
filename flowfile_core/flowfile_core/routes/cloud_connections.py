from typing import List

from fastapi import HTTPException, Depends, APIRouter
from sqlalchemy.orm import Session

# Core modules
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs import logger
from flowfile_core.database.connection import get_db
from flowfile_core.flowfile.database_connection_manager.db_connections import (store_cloud_connection,
                                                                               get_cloud_connection_schema,
                                                                               get_all_cloud_connections_interface,
                                                                               delete_cloud_connection)
# Schema and models
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection, FullCloudStorageConnectionInterface

# External dependencies
# File handling
router = APIRouter()


@router.post("/cloud_connection", tags=['cloud_connections'])
def create_cloud_storage_connection(input_connection: FullCloudStorageConnection,
                                    current_user=Depends(get_current_active_user),
                                    db: Session = Depends(get_db)
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
    logger.info(f'Create cloud connection {input_connection.connection_name}')
    try:
        store_cloud_connection(db, input_connection, current_user.id)
    except ValueError:
        raise HTTPException(422, 'Connection name already exists')
    except Exception as e:
        logger.error(e)
        raise HTTPException(422, str(e))
    return {"message": "Cloud connection created successfully"}


@router.delete('/cloud_connection', tags=['cloud_connections'])
def delete_cloud_connection_with_connection_name(connection_name: str,
                                                 current_user=Depends(get_current_active_user),
                                                 db: Session = Depends(get_db)
                                                 ):
    """
    Delete a cloud connection.
    """
    logger.info(f'Deleting cloud connection {connection_name}')
    cloud_storage_connection = get_cloud_connection_schema(db, connection_name, current_user.id)
    if cloud_storage_connection is None:
        raise HTTPException(404, 'Cloud connection connection not found')
    delete_cloud_connection(db, connection_name, current_user.id)
    return {"message": "Cloud connection deleted successfully"}


@router.get('/cloud_connections', tags=['cloud_connection'],
            response_model=List[FullCloudStorageConnectionInterface])
def get_cloud_connections(
        db: Session = Depends(get_db),
        current_user=Depends(get_current_active_user)) -> List[FullCloudStorageConnectionInterface]:
    """
    Get all cloud storage connections for the current user.
    Parameters
        db: Session obtained from Depends(get_db)
        current_user: User obtained from Depends(get_current_active_user)

    Returns
        List[FullCloudStorageConnectionInterface]
    """
    return get_all_cloud_connections_interface(db, current_user.id)
