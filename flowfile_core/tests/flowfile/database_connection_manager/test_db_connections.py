import pytest

from flowfile_core.schemas.input_schema import FullDatabaseConnection, FullDatabaseConnectionInterface
from flowfile_core.schemas.cloud_storage_schemas import (FullCloudStorageConnection,
                                                         FullCloudStorageConnectionInterface)
from flowfile_core.flowfile.database_connection_manager.db_connections import (store_database_connection,
                                                                               get_database_connection,
                                                                               get_cloud_connection,
                                                                               delete_database_connection,
                                                                               get_database_connection_schema,
                                                                               get_all_database_connections_interface,
                                                                               store_cloud_connection,
                                                                               delete_cloud_connection,
                                                                               get_all_cloud_connections_interface,
                                                                               get_cloud_connection_schema)
from flowfile_core.database.connection import get_db_context, SessionLocal
from flowfile_core.secret_manager.secret_manager import get_encrypted_secret
from pydantic import SecretStr


def del_all_cloud_connections(user_id: int = 1):
    """
    Deletes all cloud connections from the database.
    This is useful for cleaning up test data.
    """
    with get_db_context() as db:
        all_cloud_connections = get_all_cloud_connections_interface(db, user_id)
        for cloud_connection in all_cloud_connections:
            delete_cloud_connection(db, cloud_connection.connection_name, user_id)


@pytest.fixture()
def cloud_connection():
    """Reusable AWS CLI connection configuration."""
    minio_connection = FullCloudStorageConnection(
        connection_name="minio-test",
        storage_type="s3",  # Use s3, not a separate minio type
        auth_method="access_key",
        aws_access_key_id="minioadmin",
        aws_secret_access_key=SecretStr("minioadmin"),
        aws_region="us-east-1",
        endpoint_url="http://localhost:9000",
    )
    return minio_connection


def test_database_connection():
    user_id = 1
    connection = FullDatabaseConnection(username='testuser', password='testpass',
                                        connection_name='test_connection_v2', host='localhost',
                                        port=5433, database='testdb', database_type='postgresql', ssl_enabled=False)
    with get_db_context() as db:
        db_connection = store_database_connection(db, connection, user_id)
        assert db_connection is not None, "Database connection should not be None"
        assert db_connection.id is not None, "ID should not be None"

    encrypted_secret = get_encrypted_secret(user_id, connection.connection_name)
    assert encrypted_secret is not None, "Encrypted secret should not be None"

    with get_db_context() as db:
        # Clean up the database connection
        database_connection = get_database_connection(db, connection.connection_name, user_id)
        assert database_connection is not None, "Database connection should not be None"

    with get_db_context() as db:
        delete_database_connection(db, connection.connection_name, user_id)
        # Verify that the database connection has been deleted
        database_connection = get_database_connection(db, connection.connection_name, user_id)
        assert database_connection is None, "Database connection should be None after deletion"
    # verify that the secret has been deleted
    encrypted_secret = get_encrypted_secret(user_id, connection.connection_name)
    assert encrypted_secret is None, "Encrypted secret should be None after deletion"


def test_get_database_connection_schema():
    # ensure that the database is connection created
    user_id = 1
    connection = FullDatabaseConnection(username='testuser', password='testpass',
                                        connection_name='test_connection_v2', host='localhost',
                                        port=5433, database='testdb', database_type='postgresql', ssl_enabled=False)

    with get_db_context() as db:
        db_connection = store_database_connection(db, connection, user_id)
        assert db_connection is not None, "Database connection should not be None"
        assert db_connection.id is not None, "ID should not be None"
    with get_db_context() as db:
        # Get the database connection schema
        db_connection_schema = get_database_connection_schema(db, connection.connection_name, user_id).model_dump()
        db_connection_schema.pop('password')
        assert db_connection_schema == {k: v for k, v in connection.model_dump().items() if k!='password'}, "Database connection schema should match the original connection"
    # Clean up the database connection
    with get_db_context() as db:
        delete_database_connection(db, connection.connection_name, user_id)
        # Verify that the database connection has been deleted
        database_connection = get_database_connection(db, connection.connection_name, user_id)
        assert database_connection is None, "Database connection should be None after deletion"


def test_get_all_database_connections_interface():
    # ensure that the database is connection created
    user_id = 1
    connection = FullDatabaseConnection(username='testuser', password='testpass',
                                        connection_name='test_connection_v2', host='localhost',
                                        port=5433, database='testdb', database_type='postgresql', ssl_enabled=False)

    with get_db_context() as db:
        db_connection = store_database_connection(db, connection, user_id)
        assert db_connection is not None, "Database connection should not be None"
        assert db_connection.id is not None, "ID should not be None"

    with get_db_context() as db:
        # Get all database connections
        all_connections = get_all_database_connections_interface(db, user_id)
        assert isinstance(all_connections, list), "All connections should be a list"
        assert len(all_connections) > 0, "All connections should not be empty"
        assert isinstance(all_connections[0], FullDatabaseConnectionInterface), "All connections should be of type FullDatabaseConnectionInterface"
        assert not any(hasattr(acs, 'password') for acs in all_connections), "All connections should not have password attribute"

    # Clean up the database connection
    with get_db_context() as db:
        delete_database_connection(db, connection.connection_name, user_id)
        # Verify that the database connection has been deleted
        database_connection = get_database_connection(db, connection.connection_name, user_id)
        assert database_connection is None, "Database connection should be None after deletion"


def test_store_and_delete_cloud_connection(cloud_connection):
    """
    Tests the creation and subsequent deletion of a cloud storage connection,
    ensuring that the connection and its associated secrets are properly handled.
    """
    user_id = 1
    del_all_cloud_connections(user_id)
    secret_name = f"{cloud_connection.connection_name}_aws_secret_access_key"
    # 1. Store the cloud connection
    with get_db_context() as db:
        db_conn = store_cloud_connection(db, cloud_connection, user_id)
        assert db_conn is not None
        assert db_conn.aws_secret_access_key_id is not None

    # 2. Verify the connection and its secret are in the database
    with get_db_context() as db:
        retrieved_conn = get_cloud_connection(db, cloud_connection.connection_name, user_id)
        assert retrieved_conn is not None
        assert retrieved_conn.connection_name == cloud_connection.connection_name

        secret = get_encrypted_secret(user_id, secret_name)
        assert secret is not None

    # 3. Delete the cloud connection
    with get_db_context() as db:
        delete_cloud_connection(db, cloud_connection.connection_name, user_id)

    # 4. Verify the connection and its secret have been removed
    with get_db_context() as db:
        deleted_conn = get_cloud_connection(db, cloud_connection.connection_name, user_id)
        assert deleted_conn is None

        deleted_secret = get_encrypted_secret(user_id, secret_name)
        assert deleted_secret is None


def test_get_cloud_connection_schema(cloud_connection):
    """
    Tests retrieving a full cloud connection object, ensuring that secret
    values are correctly decrypted and all data matches the original input.
    """
    user_id = 1
    del_all_cloud_connections(user_id)

    # Setup: Store the connection
    with get_db_context() as db:
        store_cloud_connection(db, cloud_connection, user_id)

    # Retrieve the full schema
    with get_db_context() as db:

        schema = get_cloud_connection_schema(db, cloud_connection.connection_name, user_id)
        # Assertions
        assert isinstance(schema, FullCloudStorageConnection)
        assert schema.connection_name == cloud_connection.connection_name
        assert schema.storage_type == cloud_connection.storage_type
        # Ensure the secret is correctly retrieved and decrypted
        assert schema.aws_secret_access_key.get_secret_value() == cloud_connection.aws_secret_access_key.get_secret_value()

    # Teardown: Clean up the connection
    with get_db_context() as db:
        delete_cloud_connection(db, cloud_connection.connection_name, user_id)


def test_get_all_cloud_connections_interface(cloud_connection):
    """
    Tests retrieving all cloud connections for a user as a list of safe-to-display
    interface objects that do not contain any secrets.
    """
    user_id = 1
    del_all_cloud_connections(user_id)
    # Setup: Store a connection to ensure the list is not empty
    with get_db_context() as db:
        store_cloud_connection(db, cloud_connection, user_id)

    with get_db_context() as db:
        interfaces = get_all_cloud_connections_interface(db, user_id)

        assert isinstance(interfaces, list)
        assert len(interfaces) > 0

        interface = next((i for i in interfaces if i.connection_name == cloud_connection.connection_name), None)
        assert interface is not None
        assert isinstance(interface, FullCloudStorageConnectionInterface)

        # IMPORTANT: Verify that no secret attributes are present
        assert not hasattr(interface, 'aws_secret_access_key')
        assert not hasattr(interface, 'azure_account_key')
        assert not hasattr(interface, 'azure_client_secret')

        assert interface.aws_access_key_id == cloud_connection.aws_access_key_id

    # Teardown
    with get_db_context() as db:
        delete_cloud_connection(db, cloud_connection.connection_name, user_id)
