
from flowfile_core.schemas.input_schema import FullDatabaseConnection, FullDatabaseConnectionInterface
from flowfile_core.flowfile.database_connection_manager.db_connections import (store_database_connection,
                                                                               get_database_connection,
                                                                               delete_database_connection,
                                                                               get_database_connection_schema,
                                                                               get_all_database_connections_interface)
from flowfile_core.database.connection import get_db_context, SessionLocal
from flowfile_core.secrets.secrets import get_encrypted_secret


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
