"""Tests for GA connection CRUD + OAuth refresh-token / service-account round-trips."""

from __future__ import annotations

import json

import pytest

from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.ga_connections import (
    delete_ga_connection,
    get_all_ga_connections_interface,
    get_encrypted_credential,
    get_encrypted_refresh_token,
    get_ga_connection,
    update_ga_connection_metadata,
    upsert_ga_connection_with_refresh_token,
    upsert_ga_connection_with_service_account,
)
from flowfile_core.schemas.google_analytics_schemas import FullGoogleAnalyticsConnectionInterface
from flowfile_core.secret_manager.secret_manager import decrypt_secret, get_encrypted_secret

_TEST_REFRESH_TOKEN = "1//0g-some-fake-refresh-token-value-for-tests"
_TEST_SA_KEY = json.dumps(
    {
        "type": "service_account",
        "client_email": "etl-bot@my-project.iam.gserviceaccount.com",
        "private_key": "-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----\n",
        "token_uri": "https://oauth2.googleapis.com/token",
        "project_id": "my-project",
    }
)


def _cleanup(user_id: int = 1) -> None:
    with get_db_context() as db:
        for conn in get_all_ga_connections_interface(db, user_id):
            delete_ga_connection(db, conn.connection_name, user_id)


def test_upsert_creates_connection_and_encrypts_token() -> None:
    user_id = 1
    _cleanup(user_id)

    with get_db_context() as db:
        stored = upsert_ga_connection_with_refresh_token(
            db,
            connection_name="ga-test",
            user_id=user_id,
            refresh_token=_TEST_REFRESH_TOKEN,
            oauth_user_email="user@example.com",
            description="Test GA connection",
            default_property_id="123456789",
        )
        assert stored.id is not None
        assert stored.credential_secret_id is not None
        assert stored.oauth_user_email == "user@example.com"

    with get_db_context() as db:
        encrypted = get_encrypted_refresh_token(db, "ga-test", user_id)
        assert encrypted is not None
        assert encrypted.startswith("$ffsec$1$")
        assert _TEST_REFRESH_TOKEN not in encrypted
        assert decrypt_secret(encrypted).get_secret_value() == _TEST_REFRESH_TOKEN

    _cleanup(user_id)


def test_upsert_replaces_token_on_reconnect() -> None:
    """Calling upsert a second time for the same (name, user) rotates the token."""
    user_id = 1
    _cleanup(user_id)
    new_token = "1//0g-rotated-refresh-token"

    with get_db_context() as db:
        upsert_ga_connection_with_refresh_token(
            db,
            connection_name="ga-rotate",
            user_id=user_id,
            refresh_token=_TEST_REFRESH_TOKEN,
            oauth_user_email="a@example.com",
        )

    with get_db_context() as db:
        upsert_ga_connection_with_refresh_token(
            db,
            connection_name="ga-rotate",
            user_id=user_id,
            refresh_token=new_token,
            oauth_user_email="b@example.com",
        )

    with get_db_context() as db:
        encrypted = get_encrypted_refresh_token(db, "ga-rotate", user_id)
        assert decrypt_secret(encrypted).get_secret_value() == new_token
        conn = get_ga_connection(db, "ga-rotate", user_id)
        assert conn.oauth_user_email == "b@example.com"

    _cleanup(user_id)


def test_update_metadata_leaves_token_intact() -> None:
    user_id = 1
    _cleanup(user_id)

    with get_db_context() as db:
        upsert_ga_connection_with_refresh_token(
            db,
            connection_name="ga-meta",
            user_id=user_id,
            refresh_token=_TEST_REFRESH_TOKEN,
            oauth_user_email="user@example.com",
            description="initial",
            default_property_id="1",
        )

    with get_db_context() as db:
        update_ga_connection_metadata(
            db,
            connection_name="ga-meta",
            user_id=user_id,
            description="updated",
            default_property_id="2",
        )

    with get_db_context() as db:
        conn = get_ga_connection(db, "ga-meta", user_id)
        assert conn.description == "updated"
        assert conn.default_property_id == "2"
        encrypted = get_encrypted_refresh_token(db, "ga-meta", user_id)
        assert decrypt_secret(encrypted).get_secret_value() == _TEST_REFRESH_TOKEN

    _cleanup(user_id)


def test_delete_removes_row_and_secret() -> None:
    user_id = 1
    _cleanup(user_id)
    secret_name = "ga-delete_ga_oauth_refresh_token"

    with get_db_context() as db:
        upsert_ga_connection_with_refresh_token(
            db,
            connection_name="ga-delete",
            user_id=user_id,
            refresh_token=_TEST_REFRESH_TOKEN,
            oauth_user_email=None,
        )

    assert get_encrypted_secret(user_id, secret_name) is not None

    with get_db_context() as db:
        delete_ga_connection(db, "ga-delete", user_id)

    with get_db_context() as db:
        assert get_ga_connection(db, "ga-delete", user_id) is None
    assert get_encrypted_secret(user_id, secret_name) is None


def test_interface_list_exposes_email_but_not_secret() -> None:
    user_id = 1
    _cleanup(user_id)

    with get_db_context() as db:
        upsert_ga_connection_with_refresh_token(
            db,
            connection_name="ga-iface",
            user_id=user_id,
            refresh_token=_TEST_REFRESH_TOKEN,
            oauth_user_email="vis@example.com",
            description="visible",
            default_property_id="999",
        )

    with get_db_context() as db:
        interfaces = get_all_ga_connections_interface(db, user_id)

    match = next(i for i in interfaces if i.connection_name == "ga-iface")
    assert isinstance(match, FullGoogleAnalyticsConnectionInterface)
    assert match.description == "visible"
    assert match.default_property_id == "999"
    assert match.oauth_user_email == "vis@example.com"
    assert match.auth_method == "oauth"
    # No credential field on the public interface.
    assert not hasattr(match, "refresh_token")
    assert not hasattr(match, "service_account_json")

    _cleanup(user_id)


def test_get_encrypted_refresh_token_returns_none_for_unknown() -> None:
    user_id = 1
    _cleanup(user_id)
    with get_db_context() as db:
        assert get_encrypted_refresh_token(db, "does-not-exist", user_id) is None


def test_upsert_service_account_encrypts_key_and_sets_email() -> None:
    user_id = 1
    _cleanup(user_id)

    with get_db_context() as db:
        stored = upsert_ga_connection_with_service_account(
            db,
            connection_name="ga-sa",
            user_id=user_id,
            service_account_key=_TEST_SA_KEY,
            description="Service account connection",
            default_property_id="123456789",
        )
        assert stored.credential_secret_id is not None
        assert stored.auth_method == "service_account"
        # client_email is surfaced as the principal identity.
        assert stored.oauth_user_email == "etl-bot@my-project.iam.gserviceaccount.com"

    with get_db_context() as db:
        encrypted = get_encrypted_credential(db, "ga-sa", user_id)
        assert encrypted is not None
        assert encrypted.startswith("$ffsec$1$")
        assert "private_key" not in encrypted  # the plaintext JSON is not stored
        # Round-trips back to the original JSON key.
        assert json.loads(decrypt_secret(encrypted).get_secret_value()) == json.loads(_TEST_SA_KEY)

        interfaces = get_all_ga_connections_interface(db, user_id)
        match = next(i for i in interfaces if i.connection_name == "ga-sa")
        assert match.auth_method == "service_account"
        assert match.oauth_user_email == "etl-bot@my-project.iam.gserviceaccount.com"

    _cleanup(user_id)


def test_upsert_service_account_rejects_invalid_json() -> None:
    user_id = 1
    _cleanup(user_id)
    with get_db_context() as db:
        with pytest.raises(ValueError, match="not valid JSON"):
            upsert_ga_connection_with_service_account(
                db,
                connection_name="ga-bad-json",
                user_id=user_id,
                service_account_key="{not json",
            )
    with get_db_context() as db:
        assert get_ga_connection(db, "ga-bad-json", user_id) is None


def test_upsert_service_account_rejects_wrong_type_or_missing_fields() -> None:
    user_id = 1
    _cleanup(user_id)
    # Wrong "type".
    with get_db_context() as db:
        with pytest.raises(ValueError, match="service_account"):
            upsert_ga_connection_with_service_account(
                db,
                connection_name="ga-wrong-type",
                user_id=user_id,
                service_account_key=json.dumps(
                    {"type": "authorized_user", "client_email": "x@y.z", "private_key": "k"}
                ),
            )
    # Missing client_email.
    with get_db_context() as db:
        with pytest.raises(ValueError, match="client_email"):
            upsert_ga_connection_with_service_account(
                db,
                connection_name="ga-missing-email",
                user_id=user_id,
                service_account_key=json.dumps({"type": "service_account", "private_key": "k"}),
            )


def test_switching_auth_method_reuses_credential_slot() -> None:
    """Reconnecting an existing SA connection via OAuth flips auth_method back to
    oauth and rotates the single credential slot."""
    user_id = 1
    _cleanup(user_id)

    with get_db_context() as db:
        sa_conn = upsert_ga_connection_with_service_account(
            db,
            connection_name="ga-switch",
            user_id=user_id,
            service_account_key=_TEST_SA_KEY,
        )
        sa_secret_id = sa_conn.credential_secret_id

    with get_db_context() as db:
        oauth_conn = upsert_ga_connection_with_refresh_token(
            db,
            connection_name="ga-switch",
            user_id=user_id,
            refresh_token=_TEST_REFRESH_TOKEN,
            oauth_user_email="human@example.com",
        )
        assert oauth_conn.auth_method == "oauth"
        assert oauth_conn.oauth_user_email == "human@example.com"
        # Same credential row, rotated to the refresh token.
        assert oauth_conn.credential_secret_id == sa_secret_id
        encrypted = get_encrypted_credential(db, "ga-switch", user_id)
        assert decrypt_secret(encrypted).get_secret_value() == _TEST_REFRESH_TOKEN

    _cleanup(user_id)
