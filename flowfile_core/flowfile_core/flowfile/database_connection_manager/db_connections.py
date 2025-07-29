from flowfile_core.schemas.input_schema import FullDatabaseConnection, FullDatabaseConnectionInterface
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection, FullCloudStorageConnectionInterface
from sqlalchemy.orm import Session
from flowfile_core.database.models import (DatabaseConnection as DBConnectionModel, Secret,
                                           CloudStorageConnection as DBCloudStorageConnection)
from flowfile_core.secret_manager.secret_manager import store_secret, SecretInput, decrypt_secret
from flowfile_core.database.connection import get_db_context


def store_database_connection(db: Session, connection: FullDatabaseConnection, user_id: int) -> DBConnectionModel:
    """
    Store a database connection in the database.
    """
    # Encrypt the password

    existing_database_connection = get_database_connection(db, connection.connection_name, user_id)
    if existing_database_connection:
        raise ValueError(
            f"Database connection with name '{connection.connection_name}' already exists for user {user_id}."
            f" Please use a unique connection name or delete the existing connection first."
        )

    password_id = store_secret(db, SecretInput(name=connection.connection_name, value=connection.password), user_id).id

    # Create a new database connection object
    db_connection = DBConnectionModel(
        connection_name=connection.connection_name,
        host=connection.host,
        port=connection.port,
        database=connection.database,
        database_type=connection.database_type,
        username=connection.username,
        password_id=password_id,
        ssl_enabled=connection.ssl_enabled,
        user_id=user_id
    )

    # Add and commit the new connection to the database
    db.add(db_connection)
    db.commit()
    db.refresh(db_connection)

    return db_connection


def get_database_connection(db: Session, connection_name: str, user_id: int) -> DBConnectionModel | None:
    """
    Get a database connection by its name and user ID.
    """
    db_connection = db.query(DBConnectionModel).filter(
        DBConnectionModel.connection_name == connection_name,
        DBConnectionModel.user_id == user_id
    ).first()

    return db_connection


def get_cloud_connection(db: Session, connection_name: str, user_id: int) -> DBCloudStorageConnection | None:
    """
    Get a cloud storage connection by its name and user ID.
    """
    db_connection = db.query(DBCloudStorageConnection).filter(
        DBCloudStorageConnection.connection_name == connection_name,
        DBCloudStorageConnection.user_id == user_id
    ).first()

    return db_connection


def get_database_connection_schema(db: Session, connection_name: str, user_id: int) -> FullDatabaseConnection | None:
    """
    Get a database connection schema by its name and user ID.
    """
    db_connection = get_database_connection(db, connection_name, user_id)

    if db_connection:
        # Decrypt the password
        password_secret = db.query(Secret).filter(Secret.id == db_connection.password_id).first()
        if not password_secret:
            raise Exception("Password secret not found")

        return FullDatabaseConnection(
            connection_name=db_connection.connection_name,
            host=db_connection.host,
            port=db_connection.port,
            database=db_connection.database,
            database_type=db_connection.database_type,
            username=db_connection.username,
            password=password_secret.encrypted_value,
            ssl_enabled=db_connection.ssl_enabled
        )

    return None


def get_local_database_connection(connection_name: str, user_id: int) -> FullDatabaseConnection | None:
    with get_db_context() as db:
        return get_database_connection_schema(db, connection_name, user_id)


def get_local_cloud_connection(connection_name: str, user_id: int) -> FullCloudStorageConnection | None:
    """
    Get a cloud storage connection schema by its name and user ID.
    Args:
        connection_name (str): The name of the cloud storage connection.
        user_id (int): The ID of the user who owns the connection.

    Returns:
        FullCloudStorageConnection | None: The cloud storage connection schema if found, otherwise None.
    """
    with get_db_context() as db:
        return get_cloud_connection_schema(db, connection_name, user_id)


def delete_database_connection(db: Session, connection_name: str, user_id: int) -> None:
    """
    Delete a database connection by its name and user ID.
    """
    db_connection = db.query(DBConnectionModel).filter(
        DBConnectionModel.connection_name == connection_name,
        DBConnectionModel.user_id == user_id
    ).first()

    if db_connection:
        db.delete(db_connection)

        password_secret = db.query(Secret).filter(Secret.id == db_connection.password_id).first()
        if password_secret:
            db.delete(password_secret)
        db.commit()


def database_connection_interface_from_db_connection(
        db_connection: DBConnectionModel) -> FullDatabaseConnectionInterface:
    """
    Convert a database connection from the database model to the interface model.
    """
    return FullDatabaseConnectionInterface(
        connection_name=db_connection.connection_name,
        database_type=db_connection.database_type,
        username=db_connection.username,
        host=db_connection.host,
        port=db_connection.port,
        database=db_connection.database,
        ssl_enabled=db_connection.ssl_enabled
    )


def get_all_database_connections_interface(db: Session, user_id: int) -> list[FullDatabaseConnectionInterface]:
    """
    Get all database connections for a user.
    """
    # Get the raw query results
    query_results = db.query(DBConnectionModel).filter(
        DBConnectionModel.user_id == user_id
    ).all()

    # Convert with explicit type assertion
    result = []
    for db_connection in query_results:
        # Verify that we have an instance, not a type
        if isinstance(db_connection, DBConnectionModel):
            result.append(database_connection_interface_from_db_connection(db_connection))
        else:
            # Raise an error if we somehow get a type instead of an instance
            raise TypeError(f"Expected a DBConnectionModel instance, got {type(db_connection)}")

    return result


def store_cloud_connection(db: Session, connection: FullCloudStorageConnection, user_id: int) -> DBCloudStorageConnection:
    """
    Placeholder function to store a cloud database connection.
    This function should be implemented based on specific cloud provider requirements.
    """
    existing_database_connection = get_cloud_connection(db, connection.connection_name, user_id)
    if existing_database_connection:
        raise ValueError(
            f"Database connection with name '{connection.connection_name}' already exists for user {user_id}."
            f" Please use a unique connection name or delete the existing connection first."
        )
    if connection.aws_secret_access_key is not None:
        aws_secret_access_key_ref_id = store_secret(db,
                                                    SecretInput(name=connection.connection_name + "_aws_secret_access_key",
                                                                value=connection.aws_secret_access_key), user_id).id
    else:
        aws_secret_access_key_ref_id = None
    if connection.azure_client_secret is not None:
        azure_client_secret_ref_id = store_secret(db,
                                                  SecretInput(name=connection.connection_name + "azure_client_secret",
                                                              value=connection.azure_client_secret), user_id).id
    else:
        azure_client_secret_ref_id = None
    if connection.azure_account_key is not None:
        azure_account_key_ref_id = store_secret(db, SecretInput(name=connection.connection_name + "azure_account_key",
                                                                value=connection.azure_account_key), user_id).id
    else:
        azure_account_key_ref_id = None

    db_cloud_connection = DBCloudStorageConnection(
        connection_name=connection.connection_name,
        storage_type=connection.storage_type,
        auth_method=connection.auth_method,
        user_id=user_id,

            # AWS S3 fields
        aws_region=connection.aws_region,
        aws_access_key_id=connection.aws_access_key_id,
        aws_role_arn=connection.aws_role_arn,
        aws_secret_access_key_id=aws_secret_access_key_ref_id,
        aws_allow_unsafe_html=connection.aws_allow_unsafe_html,

        # Azure ADLS fields
        azure_account_name=connection.azure_account_name,
        azure_tenant_id=connection.azure_tenant_id,
        azure_client_id=connection.azure_client_id,
        azure_account_key_id=azure_account_key_ref_id,
        azure_client_secret_id=azure_client_secret_ref_id,

        # Common fields
        endpoint_url=connection.endpoint_url,
        verify_ssl=connection.verify_ssl
    )
    db.add(db_cloud_connection)
    db.commit()
    db.refresh(db_cloud_connection)
    return db_cloud_connection


def get_full_cloud_storage_interface_from_db(
        db_cloud_connection: DBCloudStorageConnection) -> FullCloudStorageConnectionInterface:
    """
    Convert a cloud storage connection from the database model to the interface model.
    """
    return FullCloudStorageConnectionInterface(
        connection_name=db_cloud_connection.connection_name,
        storage_type=db_cloud_connection.storage_type,
        auth_method=db_cloud_connection.auth_method,
        aws_allow_unsafe_html=db_cloud_connection.aws_allow_unsafe_html,
        aws_region=db_cloud_connection.aws_region,
        aws_access_key_id=db_cloud_connection.aws_access_key_id,
        aws_role_arn=db_cloud_connection.aws_role_arn,
        azure_account_name=db_cloud_connection.azure_account_name,
        azure_tenant_id=db_cloud_connection.azure_tenant_id,
        azure_client_id=db_cloud_connection.azure_client_id,
        endpoint_url=db_cloud_connection.endpoint_url,
        verify_ssl=db_cloud_connection.verify_ssl
    )


def get_cloud_connection_schema(db: Session, connection_name: str, user_id: int) -> FullCloudStorageConnection | None:
    """
    Retrieves a full cloud storage connection schema, including decrypted secrets, by its name and user ID.
    """
    db_connection = get_cloud_connection(db, connection_name, user_id)
    if not db_connection:
        return None

    # Decrypt secrets associated with the connection
    aws_secret_key = None
    if db_connection.aws_secret_access_key_id:
        secret_record = db.query(Secret).filter(Secret.id == db_connection.aws_secret_access_key_id).first()
        if secret_record:
            aws_secret_key = decrypt_secret(secret_record.encrypted_value)

    azure_account_key = None
    if db_connection.azure_account_key_id:
        secret_record = db.query(Secret).filter(Secret.id == db_connection.azure_account_key_id).first()
        if secret_record:
            azure_account_key = decrypt_secret(secret_record.encrypted_value)

    azure_client_secret = None
    if db_connection.azure_client_secret_id:
        secret_record = db.query(Secret).filter(Secret.id == db_connection.azure_client_secret_id).first()
        if secret_record:
            azure_client_secret = decrypt_secret(secret_record.encrypted_value)

    # Construct the full Pydantic model
    return FullCloudStorageConnection(
        connection_name=db_connection.connection_name,
        storage_type=db_connection.storage_type,
        auth_method=db_connection.auth_method,
        aws_allow_unsafe_html=db_connection.aws_allow_unsafe_html,
        aws_region=db_connection.aws_region,
        aws_access_key_id=db_connection.aws_access_key_id,
        aws_secret_access_key=aws_secret_key,
        aws_role_arn=db_connection.aws_role_arn,
        azure_account_name=db_connection.azure_account_name,
        azure_account_key=azure_account_key,
        azure_tenant_id=db_connection.azure_tenant_id,
        azure_client_id=db_connection.azure_client_id,
        azure_client_secret=azure_client_secret,
        endpoint_url=db_connection.endpoint_url,
        verify_ssl=db_connection.verify_ssl
    )


def cloud_connection_interface_from_db_connection(
        db_connection: DBCloudStorageConnection) -> FullCloudStorageConnectionInterface:
    """
    Converts a DBCloudStorageConnection model to a FullCloudStorageConnectionInterface model,
    which safely exposes non-sensitive data.
    """
    return FullCloudStorageConnectionInterface(
        connection_name=db_connection.connection_name,
        storage_type=db_connection.storage_type,
        auth_method=db_connection.auth_method,
        aws_allow_unsafe_html=db_connection.aws_allow_unsafe_html,
        aws_region=db_connection.aws_region,
        aws_access_key_id=db_connection.aws_access_key_id,
        aws_role_arn=db_connection.aws_role_arn,
        azure_account_name=db_connection.azure_account_name,
        azure_tenant_id=db_connection.azure_tenant_id,
        azure_client_id=db_connection.azure_client_id,
        endpoint_url=db_connection.endpoint_url,
        verify_ssl=db_connection.verify_ssl
    )


def get_all_cloud_connections_interface(db: Session, user_id: int) -> list[FullCloudStorageConnectionInterface]:
    """
    Retrieves a list of all cloud storage connections for a user in a safe interface format (no secrets).
    """
    db_connections = db.query(DBCloudStorageConnection).filter(DBCloudStorageConnection.user_id == user_id).all()

    return [cloud_connection_interface_from_db_connection(conn) for conn in db_connections]


def delete_cloud_connection(db: Session, connection_name: str, user_id: int) -> None:
    """
    Deletes a cloud storage connection and all of its associated secrets from the database.
    """
    db_connection = get_cloud_connection(db, connection_name, user_id)

    if db_connection:
        # Collect all secret IDs associated with this connection
        secret_ids_to_delete = [
            db_connection.aws_secret_access_key_id,
            db_connection.aws_session_token_id,
            db_connection.azure_account_key_id,
            db_connection.azure_client_secret_id,
            db_connection.azure_sas_token_id
        ]
        # Filter out None values
        secret_ids_to_delete = [id for id in secret_ids_to_delete if id is not None]

        # Delete associated secrets if they exist
        if secret_ids_to_delete:
            db.query(Secret).filter(Secret.id.in_(secret_ids_to_delete)).delete(synchronize_session=False)

        # Delete the connection record itself
        db.delete(db_connection)
        db.commit()
