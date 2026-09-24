import pytest
from pydantic import SecretStr

from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_cloud_connection,
    delete_database_connection,
    get_all_cloud_connections_interface,
    get_all_database_connections_interface,
    get_cloud_connection,
    get_cloud_connection_schema,
    get_database_connection,
    get_database_connection_schema,
    store_cloud_connection,
    store_database_connection,
    update_cloud_connection,
)
from flowfile_core.database.models import Secret
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection, FullCloudStorageConnectionInterface
from flowfile_core.schemas.input_schema import FullDatabaseConnection, FullDatabaseConnectionInterface
from flowfile_core.secret_manager.secret_manager import decrypt_secret, get_encrypted_secret


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


@pytest.fixture()
def cli_cloud_connection():
    """Reusable AWS CLI connection configuration."""
    minio_connection = FullCloudStorageConnection(
        connection_name="minio-test",
        storage_type="s3",
        auth_method="aws-cli",
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
        database_connection = get_database_connection(db, connection.connection_name, user_id)
        assert database_connection is not None, "Database connection should not be None"

    with get_db_context() as db:
        delete_database_connection(db, connection.connection_name, user_id)
        database_connection = get_database_connection(db, connection.connection_name, user_id)
        assert database_connection is None, "Database connection should be None after deletion"
    encrypted_secret = get_encrypted_secret(user_id, connection.connection_name)
    assert encrypted_secret is None, "Encrypted secret should be None after deletion"


def test_get_database_connection_schema():
    user_id = 1
    connection = FullDatabaseConnection(username='testuser', password='testpass',
                                        connection_name='test_connection_v2', host='localhost',
                                        port=5433, database='testdb', database_type='postgresql', ssl_enabled=False)

    with get_db_context() as db:
        db_connection = store_database_connection(db, connection, user_id)
        assert db_connection is not None, "Database connection should not be None"
        assert db_connection.id is not None, "ID should not be None"
    with get_db_context() as db:
        db_connection_schema = get_database_connection_schema(db, connection.connection_name, user_id).model_dump()
        db_connection_schema.pop('password')
        assert db_connection_schema == {k: v for k, v in connection.model_dump().items() if k!='password'}, "Database connection schema should match the original connection"
    with get_db_context() as db:
        delete_database_connection(db, connection.connection_name, user_id)
        database_connection = get_database_connection(db, connection.connection_name, user_id)
        assert database_connection is None, "Database connection should be None after deletion"


def test_get_all_database_connections_interface():
    user_id = 1
    connection = FullDatabaseConnection(username='testuser', password='testpass',
                                        connection_name='test_connection_v2', host='localhost',
                                        port=5433, database='testdb', database_type='postgresql', ssl_enabled=False)

    with get_db_context() as db:
        db_connection = store_database_connection(db, connection, user_id)
        assert db_connection is not None, "Database connection should not be None"
        assert db_connection.id is not None, "ID should not be None"

    with get_db_context() as db:
        all_connections = get_all_database_connections_interface(db, user_id)
        assert isinstance(all_connections, list), "All connections should be a list"
        assert len(all_connections) > 0, "All connections should not be empty"
        assert isinstance(all_connections[0], FullDatabaseConnectionInterface), "All connections should be of type FullDatabaseConnectionInterface"
        assert not any(hasattr(acs, 'password') for acs in all_connections), "All connections should not have password attribute"

    with get_db_context() as db:
        delete_database_connection(db, connection.connection_name, user_id)
        database_connection = get_database_connection(db, connection.connection_name, user_id)
        assert database_connection is None, "Database connection should be None after deletion"


def test_store_and_get_cloud_connection_unsafe_html(cloud_connection):
    """
    Tests the storage of a cloud connection with unsafe HTML in the connection name,
    ensuring that it is sanitized before being stored in the database.
    """
    user_id = 1
    del_all_cloud_connections(user_id)
    cloud_connection.aws_allow_unsafe_html = True
    with get_db_context() as db:
        db_conn = store_cloud_connection(db, cloud_connection, user_id)
        assert db_conn is not None
        assert db_conn.aws_secret_access_key_id is not None
    with get_db_context() as db:
        retrieved_db_obj = get_cloud_connection(db, cloud_connection.connection_name, user_id)
        assert retrieved_db_obj.aws_allow_unsafe_html, "Connection should allow unsafe HTML"
        retrieved_interface_obj = get_cloud_connection_schema(db, cloud_connection.connection_name, user_id)
        assert retrieved_interface_obj.aws_allow_unsafe_html, "Interface should allow unsafe HTML"


def test_store_and_delete_cloud_connection(cloud_connection):
    """
    Tests the creation and subsequent deletion of a cloud storage connection,
    ensuring that the connection and its associated secrets are properly handled.
    """
    user_id = 1
    del_all_cloud_connections(user_id)
    secret_name = f"{cloud_connection.connection_name}_aws_secret_access_key"
    with get_db_context() as db:
        db_conn = store_cloud_connection(db, cloud_connection, user_id)
        assert db_conn is not None
        assert db_conn.aws_secret_access_key_id is not None

    with get_db_context() as db:
        retrieved_conn = get_cloud_connection(db, cloud_connection.connection_name, user_id)
        assert retrieved_conn is not None
        assert retrieved_conn.connection_name == cloud_connection.connection_name

        secret = get_encrypted_secret(user_id, secret_name)
        assert secret is not None

    with get_db_context() as db:
        delete_cloud_connection(db, cloud_connection.connection_name, user_id)

    with get_db_context() as db:
        deleted_conn = get_cloud_connection(db, cloud_connection.connection_name, user_id)
        assert deleted_conn is None

        deleted_secret = get_encrypted_secret(user_id, secret_name)
        assert deleted_secret is None


def test_store_aws_cli_cloud_connection(cli_cloud_connection):
    user_id = 1
    del_all_cloud_connections(user_id)
    with get_db_context() as db:
        store_cloud_connection(db, cli_cloud_connection, user_id)
    with get_db_context() as db:
        schema = get_cloud_connection_schema(db, cli_cloud_connection.connection_name, user_id)


def test_get_cloud_connection_schema(cloud_connection):
    """
    Tests retrieving a full cloud connection object, ensuring that secret
    values are correctly decrypted and all data matches the original input.
    """
    user_id = 1
    del_all_cloud_connections(user_id)

    with get_db_context() as db:
        store_cloud_connection(db, cloud_connection, user_id)

    with get_db_context() as db:

        schema = get_cloud_connection_schema(db, cloud_connection.connection_name, user_id)
        assert isinstance(schema, FullCloudStorageConnection)
        assert schema.connection_name == cloud_connection.connection_name
        assert schema.storage_type == cloud_connection.storage_type
        assert schema.aws_secret_access_key.get_secret_value() == cloud_connection.aws_secret_access_key.get_secret_value()

    with get_db_context() as db:
        delete_cloud_connection(db, cloud_connection.connection_name, user_id)


def test_get_all_cloud_connections_interface(cloud_connection):
    """
    Tests retrieving all cloud connections for a user as a list of safe-to-display
    interface objects that do not contain any secrets.
    """
    user_id = 1
    del_all_cloud_connections(user_id)
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

    with get_db_context() as db:
        delete_cloud_connection(db, cloud_connection.connection_name, user_id)


class TestAwsProfileAndSessionTokenPersistence:
    """``aws_profile`` and ``aws_session_token`` survive store, load, update and delete."""

    user_id = 1

    @staticmethod
    def _load(name: str) -> FullCloudStorageConnection:
        with get_db_context() as db:
            return get_cloud_connection_schema(db, name, 1)

    def test_aws_cli_profile_round_trips_trimmed(self):
        del_all_cloud_connections(self.user_id)
        connection = FullCloudStorageConnection(
            connection_name="minio connection",
            storage_type="s3",
            auth_method="aws-cli",
            aws_profile="  analytics  ",
            endpoint_url="http://localhost:9000",
        )
        with get_db_context() as db:
            store_cloud_connection(db, connection, self.user_id)
        assert self._load("minio connection").aws_profile == "analytics"
        with get_db_context() as db:
            interface = next(
                i for i in get_all_cloud_connections_interface(db, self.user_id)
                if i.connection_name == "minio connection"
            )
        assert interface.aws_profile == "analytics"
        assert "aws_session_token" not in interface.model_dump()

        with get_db_context() as db:
            update_cloud_connection(db, connection.model_copy(update={"aws_profile": "   "}), self.user_id)
        assert self._load("minio connection").aws_profile is None
        del_all_cloud_connections(self.user_id)

    def test_profile_is_only_kept_for_aws_cli(self):
        del_all_cloud_connections(self.user_id)
        connection = FullCloudStorageConnection(
            connection_name="profiled",
            storage_type="s3",
            auth_method="aws-cli",
            aws_profile="analytics",
        )
        with get_db_context() as db:
            store_cloud_connection(db, connection, self.user_id)
        switched = connection.model_copy(
            update={"auth_method": "access_key", "aws_access_key_id": "AKID", "aws_secret_access_key": SecretStr("s")}
        )
        with get_db_context() as db:
            update_cloud_connection(db, switched, self.user_id)
        assert self._load("profiled").aws_profile is None
        del_all_cloud_connections(self.user_id)

    def test_session_token_is_stored_encrypted_kept_rotated_and_deleted(self):
        del_all_cloud_connections(self.user_id)
        connection = FullCloudStorageConnection(
            connection_name="temporary-keys",
            storage_type="s3",
            auth_method="access_key",
            aws_access_key_id="ASIATEMP",
            aws_secret_access_key=SecretStr("temp-secret"),
            aws_session_token=SecretStr("token-1"),
        )
        with get_db_context() as db:
            db_row = store_cloud_connection(db, connection, self.user_id)
            token_secret_id = db_row.aws_session_token_id
            assert token_secret_id is not None
            stored = db.query(Secret).filter(Secret.id == token_secret_id).one()
            assert "token-1" not in stored.encrypted_value
        assert self._load("temporary-keys").aws_session_token.get_secret_value() == "token-1"

        blank = connection.model_copy(update={"aws_secret_access_key": None, "aws_session_token": SecretStr("")})
        with get_db_context() as db:
            update_cloud_connection(db, blank, self.user_id)
        assert self._load("temporary-keys").aws_session_token.get_secret_value() == "token-1"

        rotated = connection.model_copy(update={"aws_session_token": SecretStr("token-2")})
        with get_db_context() as db:
            update_cloud_connection(db, rotated, self.user_id)
        assert self._load("temporary-keys").aws_session_token.get_secret_value() == "token-2"

        with get_db_context() as db:
            delete_cloud_connection(db, "temporary-keys", self.user_id)
            assert db.query(Secret).filter(Secret.id == token_secret_id).first() is None

    def test_blank_session_token_is_not_stored(self):
        del_all_cloud_connections(self.user_id)
        connection = FullCloudStorageConnection(
            connection_name="static-keys",
            storage_type="s3",
            auth_method="access_key",
            aws_access_key_id="AKID",
            aws_secret_access_key=SecretStr("secret"),
            aws_session_token=SecretStr(""),
        )
        with get_db_context() as db:
            assert store_cloud_connection(db, connection, self.user_id).aws_session_token_id is None
        assert self._load("static-keys").aws_session_token is None
        del_all_cloud_connections(self.user_id)

    def test_worker_interface_carries_the_profile_and_the_encrypted_token(self):
        connection = FullCloudStorageConnection(
            connection_name="temporary-keys",
            storage_type="s3",
            auth_method="access_key",
            aws_access_key_id="ASIATEMP",
            aws_secret_access_key=SecretStr("temp-secret"),
            aws_session_token=SecretStr("token-1"),
            aws_profile="analytics",
        )
        worker_interface = connection.get_worker_interface(self.user_id)
        assert worker_interface.aws_profile == "analytics"
        assert worker_interface.aws_session_token.startswith(f"$ffsec$1${self.user_id}$")
        assert decrypt_secret(worker_interface.aws_session_token).get_secret_value() == "token-1"
