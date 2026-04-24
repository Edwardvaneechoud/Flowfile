"""Tests for the per-user OAuth client config stored in the Secret table."""

from __future__ import annotations

from flowfile_core.configs.app_settings import (
    GOOGLE_OAUTH_CLIENT_ID_KEY,
    GOOGLE_OAUTH_CLIENT_SECRET_KEY,
    GOOGLE_OAUTH_REDIRECT_URI_KEY,
    clear_google_oauth_config,
    get_google_oauth_config,
    get_user_secret,
    set_google_oauth_config,
    set_user_secret,
)
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import Secret


def _cleanup(user_id: int = 1) -> None:
    with get_db_context() as db:
        db.query(Secret).filter(
            Secret.user_id == user_id,
            Secret.name.in_(
                [
                    GOOGLE_OAUTH_CLIENT_ID_KEY,
                    GOOGLE_OAUTH_CLIENT_SECRET_KEY,
                    GOOGLE_OAUTH_REDIRECT_URI_KEY,
                ]
            ),
        ).delete(synchronize_session=False)
        db.commit()


def test_set_and_get_roundtrip() -> None:
    user_id = 1
    _cleanup(user_id)
    with get_db_context() as db:
        set_user_secret(db, GOOGLE_OAUTH_CLIENT_ID_KEY, "a-value", user_id)
    with get_db_context() as db:
        assert get_user_secret(db, GOOGLE_OAUTH_CLIENT_ID_KEY, user_id) == "a-value"
    _cleanup(user_id)


def test_set_updates_existing_row() -> None:
    user_id = 1
    _cleanup(user_id)
    with get_db_context() as db:
        set_user_secret(db, GOOGLE_OAUTH_CLIENT_ID_KEY, "first", user_id)
        set_user_secret(db, GOOGLE_OAUTH_CLIENT_ID_KEY, "second", user_id)
    with get_db_context() as db:
        assert get_user_secret(db, GOOGLE_OAUTH_CLIENT_ID_KEY, user_id) == "second"
        rows = (
            db.query(Secret)
            .filter(Secret.name == GOOGLE_OAUTH_CLIENT_ID_KEY, Secret.user_id == user_id)
            .count()
        )
        # Update, not append — still a single row.
        assert rows == 1
    _cleanup(user_id)


def test_missing_setting_returns_none() -> None:
    _cleanup()
    with get_db_context() as db:
        assert get_user_secret(db, "ga_oauth_never_set", 1) is None


def test_encryption_at_rest_not_plaintext() -> None:
    user_id = 1
    _cleanup(user_id)
    with get_db_context() as db:
        set_user_secret(db, GOOGLE_OAUTH_CLIENT_SECRET_KEY, "the-secret-value", user_id)
    with get_db_context() as db:
        row = (
            db.query(Secret)
            .filter(
                Secret.name == GOOGLE_OAUTH_CLIENT_SECRET_KEY,
                Secret.user_id == user_id,
            )
            .first()
        )
        assert row is not None
        assert "the-secret-value" not in row.encrypted_value
    _cleanup(user_id)


def test_google_oauth_config_per_user_isolation() -> None:
    """User 1's config never leaks into user 2's."""
    _cleanup(1)
    _cleanup(2)
    with get_db_context() as db:
        set_google_oauth_config(
            db,
            user_id=1,
            client_id="one-id",
            client_secret="one-secret",
            redirect_uri="http://localhost/cb1",
        )
        set_google_oauth_config(
            db,
            user_id=2,
            client_id="two-id",
            client_secret="two-secret",
            redirect_uri="http://localhost/cb2",
        )

    with get_db_context() as db:
        cfg_one = get_google_oauth_config(db, 1)
        cfg_two = get_google_oauth_config(db, 2)
        assert cfg_one["client_id"] == "one-id"
        assert cfg_one["client_secret"] == "one-secret"
        assert cfg_two["client_id"] == "two-id"
        assert cfg_two["client_secret"] == "two-secret"

    _cleanup(1)
    _cleanup(2)


def test_google_oauth_config_env_fallback(monkeypatch) -> None:
    """Env vars fill in any key the user hasn't stored in their Secret table."""
    _cleanup()
    monkeypatch.setattr(
        "flowfile_core.configs.app_settings.GOOGLE_OAUTH_CLIENT_ID", "env-client-id"
    )
    monkeypatch.setattr(
        "flowfile_core.configs.app_settings.GOOGLE_OAUTH_CLIENT_SECRET", "env-client-secret"
    )

    with get_db_context() as db:
        cfg = get_google_oauth_config(db, 1)
        assert cfg["client_id"] == "env-client-id"
        assert cfg["client_secret"] == "env-client-secret"

    with get_db_context() as db:
        set_user_secret(db, GOOGLE_OAUTH_CLIENT_ID_KEY, "db-client-id", 1)

    with get_db_context() as db:
        cfg = get_google_oauth_config(db, 1)
        # DB row wins for client_id, env fills in the rest.
        assert cfg["client_id"] == "db-client-id"
        assert cfg["client_secret"] == "env-client-secret"

    _cleanup(1)


def test_clear_google_oauth_config() -> None:
    _cleanup()
    with get_db_context() as db:
        set_google_oauth_config(
            db,
            user_id=1,
            client_id="cid",
            client_secret="csecret",
            redirect_uri="http://localhost/cb",
        )
    with get_db_context() as db:
        clear_google_oauth_config(db, 1)
    with get_db_context() as db:
        assert get_user_secret(db, GOOGLE_OAUTH_CLIENT_ID_KEY, 1) is None
        assert get_user_secret(db, GOOGLE_OAUTH_CLIENT_SECRET_KEY, 1) is None
        assert get_user_secret(db, GOOGLE_OAUTH_REDIRECT_URI_KEY, 1) is None
