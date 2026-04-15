"""Tests for GA connection CRUD + secret encryption round-trip."""

from __future__ import annotations

from pydantic import SecretStr

from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.ga_connections import (
    delete_ga_connection,
    get_all_ga_connections_interface,
    get_ga_connection,
    get_ga_connection_schema,
    store_ga_connection,
    update_ga_connection,
)
from flowfile_core.schemas.google_analytics_schemas import (
    FullGoogleAnalyticsConnection,
    FullGoogleAnalyticsConnectionInterface,
)
from flowfile_core.secret_manager.secret_manager import get_encrypted_secret

_TEST_KEY = '{"type": "service_account", "project_id": "proj-x", "client_email": "sa@proj-x.iam"}'


def _cleanup(user_id: int = 1) -> None:
    with get_db_context() as db:
        for conn in get_all_ga_connections_interface(db, user_id):
            delete_ga_connection(db, conn.connection_name, user_id)


def test_store_and_retrieve_ga_connection() -> None:
    """Round-trip: store -> fetch schema -> verify JSON key is decrypted correctly."""
    user_id = 1
    _cleanup(user_id)
    connection = FullGoogleAnalyticsConnection(
        connection_name="ga-test",
        description="Test GA connection",
        default_property_id="123456789",
        service_account_json=SecretStr(_TEST_KEY),
    )

    with get_db_context() as db:
        stored = store_ga_connection(db, connection, user_id)
        assert stored is not None
        assert stored.id is not None
        assert stored.service_account_key_id is not None

    with get_db_context() as db:
        schema = get_ga_connection_schema(db, connection.connection_name, user_id)
        assert isinstance(schema, FullGoogleAnalyticsConnection)
        assert schema.connection_name == connection.connection_name
        assert schema.default_property_id == "123456789"
        assert schema.description == "Test GA connection"
        assert schema.service_account_json.get_secret_value() == _TEST_KEY

    _cleanup(user_id)


def test_duplicate_name_is_rejected() -> None:
    user_id = 1
    _cleanup(user_id)
    connection = FullGoogleAnalyticsConnection(
        connection_name="ga-dup",
        service_account_json=SecretStr(_TEST_KEY),
    )

    with get_db_context() as db:
        store_ga_connection(db, connection, user_id)

    with get_db_context() as db:
        try:
            store_ga_connection(db, connection, user_id)
        except ValueError as e:
            assert "already exists" in str(e)
        else:
            raise AssertionError("Expected ValueError for duplicate connection name")

    _cleanup(user_id)


def test_update_preserves_key_when_blank() -> None:
    """Updating with ``service_account_json=None`` keeps the stored key."""
    user_id = 1
    _cleanup(user_id)
    connection = FullGoogleAnalyticsConnection(
        connection_name="ga-update",
        description="initial",
        default_property_id="1",
        service_account_json=SecretStr(_TEST_KEY),
    )

    with get_db_context() as db:
        store_ga_connection(db, connection, user_id)

    # Update without providing a new key.
    with get_db_context() as db:
        update_ga_connection(
            db,
            FullGoogleAnalyticsConnection(
                connection_name="ga-update",
                description="updated",
                default_property_id="2",
                service_account_json=None,
            ),
            user_id,
        )

    with get_db_context() as db:
        schema = get_ga_connection_schema(db, "ga-update", user_id)
        assert schema.description == "updated"
        assert schema.default_property_id == "2"
        # The key was preserved.
        assert schema.service_account_json.get_secret_value() == _TEST_KEY

    _cleanup(user_id)


def test_update_replaces_key_when_provided() -> None:
    user_id = 1
    _cleanup(user_id)
    new_key = '{"type": "service_account", "project_id": "replaced", "client_email": "sa@new.iam"}'

    with get_db_context() as db:
        store_ga_connection(
            db,
            FullGoogleAnalyticsConnection(
                connection_name="ga-replace",
                service_account_json=SecretStr(_TEST_KEY),
            ),
            user_id,
        )

    with get_db_context() as db:
        update_ga_connection(
            db,
            FullGoogleAnalyticsConnection(
                connection_name="ga-replace",
                service_account_json=SecretStr(new_key),
            ),
            user_id,
        )

    with get_db_context() as db:
        schema = get_ga_connection_schema(db, "ga-replace", user_id)
        assert schema.service_account_json.get_secret_value() == new_key

    _cleanup(user_id)


def test_delete_removes_row_and_secret() -> None:
    user_id = 1
    _cleanup(user_id)
    secret_name = "ga-delete_ga_service_account_key"
    connection = FullGoogleAnalyticsConnection(
        connection_name="ga-delete",
        service_account_json=SecretStr(_TEST_KEY),
    )

    with get_db_context() as db:
        store_ga_connection(db, connection, user_id)

    assert get_encrypted_secret(user_id, secret_name) is not None

    with get_db_context() as db:
        delete_ga_connection(db, "ga-delete", user_id)

    with get_db_context() as db:
        assert get_ga_connection(db, "ga-delete", user_id) is None
    assert get_encrypted_secret(user_id, secret_name) is None


def test_interface_list_hides_secret() -> None:
    user_id = 1
    _cleanup(user_id)
    with get_db_context() as db:
        store_ga_connection(
            db,
            FullGoogleAnalyticsConnection(
                connection_name="ga-interface",
                description="visible",
                default_property_id="999",
                service_account_json=SecretStr(_TEST_KEY),
            ),
            user_id,
        )

    with get_db_context() as db:
        interfaces = get_all_ga_connections_interface(db, user_id)

    assert interfaces, "Expected at least one connection"
    match = next(i for i in interfaces if i.connection_name == "ga-interface")
    assert isinstance(match, FullGoogleAnalyticsConnectionInterface)
    # The public interface must not expose the secret.
    assert not hasattr(match, "service_account_json")
    assert match.description == "visible"
    assert match.default_property_id == "999"

    _cleanup(user_id)


def test_worker_interface_encrypts_secret() -> None:
    """``get_worker_interface`` emits an encrypted token, not plaintext."""
    user_id = 7
    connection = FullGoogleAnalyticsConnection(
        connection_name="ga-worker-iface",
        service_account_json=SecretStr(_TEST_KEY),
    )
    worker_iface = connection.get_worker_interface(user_id)

    assert worker_iface.connection_name == "ga-worker-iface"
    token = worker_iface.service_account_json_encrypted
    # Fernet token format used by ``encrypt_secret``: "$ffsec$1$<user_id>$<fernet_token>".
    assert token.startswith("$ffsec$1$")
    assert _TEST_KEY not in token  # Plaintext must not appear anywhere.
