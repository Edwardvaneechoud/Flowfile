"""The /system/exchange_store admin routes and the settings behind them: a saved store, the environment winning."""

from __future__ import annotations

import uuid
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from flowfile_core import main
from flowfile_core.auth.jwt import get_current_active_user, get_current_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs import exchange_config, settings
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import store_cloud_connection
from flowfile_core.flowfile.execution import exchange
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection

ENV_VARS = ("FLOWFILE_EXCHANGE_URI", "FLOWFILE_EXCHANGE_CONNECTION", "FLOWFILE_EXCHANGE_MAX_ROWS")


def _client(user: PydanticUser) -> Iterator[TestClient]:
    main.app.dependency_overrides[get_current_active_user] = lambda: user
    main.app.dependency_overrides[get_current_user] = lambda: user
    try:
        yield TestClient(main.app)
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)
        main.app.dependency_overrides.pop(get_current_user, None)


@pytest.fixture
def admin_client() -> Iterator[TestClient]:
    yield from _client(
        PydanticUser(username="local_user", id=1, disabled=False, is_admin=True, must_change_password=False)
    )


@pytest.fixture
def non_admin_client() -> Iterator[TestClient]:
    yield from _client(
        PydanticUser(username="not_admin", id=2, disabled=False, is_admin=False, must_change_password=False)
    )


@pytest.fixture
def no_env(monkeypatch):
    for name in ENV_VARS:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def s3_connection() -> str:
    """An S3 connection of the local user, under a fresh name."""
    name = f"exchange-{uuid.uuid4().hex[:8]}"
    connection = FullCloudStorageConnection(
        connection_name=name,
        storage_type="s3",
        auth_method="access_key",
        aws_access_key_id="key",
        aws_secret_access_key=SecretStr("secret"),
        aws_region="nbg1",
        endpoint_url="https://objects.test",
    )
    with get_db_context() as db:
        store_cloud_connection(db, connection, user_id=1)
    return name


def test_the_routes_are_an_administrators(non_admin_client):
    assert non_admin_client.get("/system/exchange_store").status_code == 403
    assert non_admin_client.post("/system/exchange_store", json={"uri": "s3://b/x"}).status_code == 403


def test_nothing_saved_and_nothing_in_the_environment_is_no_store(admin_client, no_env, s3_connection):
    answer = admin_client.get("/system/exchange_store")

    assert answer.status_code == 200
    state = answer.json()
    assert state["source"] == "none" and state["uri"] is None and state["env_override"] is False
    assert state["saved"] == {"uri": None, "connection": None, "max_rows": None}
    assert s3_connection in state["connections"]
    assert settings.get_exchange_uri() is None
    assert exchange.exchange_store() is None


def test_a_saved_store_is_what_the_settings_and_the_run_read(admin_client, no_env, s3_connection):
    body = {"uri": "s3://exchange-bucket/flowfile/", "connection": s3_connection, "max_rows": 1000}

    answer = admin_client.post("/system/exchange_store", json=body)

    assert answer.status_code == 200, answer.text
    state = answer.json()
    assert state["source"] == "saved" and state["env_override"] is False
    assert state["uri"] == "s3://exchange-bucket/flowfile" and state["connection"] == s3_connection
    assert state["max_rows"] == 1000 and state["saved"]["uri"] == "s3://exchange-bucket/flowfile"
    assert settings.get_exchange_uri() == "s3://exchange-bucket/flowfile"
    assert settings.get_exchange_connection() == s3_connection
    assert settings.get_exchange_max_rows() == 1000
    store = exchange.exchange_store()
    assert store is not None and store.uri == "s3://exchange-bucket/flowfile"
    with get_db_context() as db:
        from flowfile_core.database.models import CloudStorageConnection

        row = db.query(CloudStorageConnection).filter_by(connection_name=s3_connection).one()
        assert store.credential.connection_id == row.id and store.credential.user_id == row.user_id
    assert admin_client.get("/system/exchange_store").json()["uri"] == "s3://exchange-bucket/flowfile"


def test_a_save_is_a_file_the_next_call_reads(admin_client, no_env, s3_connection):
    admin_client.post("/system/exchange_store", json={"uri": "s3://b/x", "connection": s3_connection})

    assert exchange_config._settings_file().is_file()
    assert exchange_config.saved() == exchange_config.ExchangeConfig(uri="s3://b/x", connection=s3_connection)


@pytest.mark.parametrize(
    ("body", "fragment"),
    [
        ({"uri": "gs://bucket/x", "connection": "any"}, "must be an s3:// URI"),
        ({"uri": "s3://", "connection": "any"}, "must be an s3:// URI"),
        ({"uri": "s3://bucket/x"}, "Name the stored S3 connection"),
        ({"uri": "s3://bucket/x", "connection": "no-such-connection"}, "No S3 connection named 'no-such-connection'"),
    ],
)
def test_a_store_that_cannot_open_is_refused_and_nothing_is_saved(admin_client, no_env, s3_connection, body, fragment):
    answer = admin_client.post("/system/exchange_store", json=body)

    assert answer.status_code == 422, answer.text
    assert fragment in answer.json()["detail"]
    assert exchange_config.saved() == exchange_config.ExchangeConfig()


def test_a_negative_row_limit_is_refused(admin_client, no_env, s3_connection):
    body = {"uri": "s3://bucket/x", "connection": s3_connection, "max_rows": -1}

    assert admin_client.post("/system/exchange_store", json=body).status_code == 422


def test_no_uri_clears_the_store(admin_client, no_env, s3_connection):
    admin_client.post("/system/exchange_store", json={"uri": "s3://b/x", "connection": s3_connection})

    answer = admin_client.post("/system/exchange_store", json={"uri": ""})

    assert answer.json()["source"] == "none"
    assert settings.get_exchange_uri() is None
    assert exchange_config.saved() == exchange_config.ExchangeConfig()


def test_the_environment_wins_and_the_page_is_told(admin_client, no_env, s3_connection, monkeypatch):
    admin_client.post(
        "/system/exchange_store", json={"uri": "s3://saved/x", "connection": s3_connection, "max_rows": 7}
    )
    monkeypatch.setenv("FLOWFILE_EXCHANGE_URI", "s3://pinned/y")
    monkeypatch.setenv("FLOWFILE_EXCHANGE_CONNECTION", "pinned-connection")

    state = admin_client.get("/system/exchange_store").json()

    assert state["source"] == "env" and state["env_override"] is True
    assert state["uri"] == "s3://pinned/y" and state["connection"] == "pinned-connection"
    assert state["max_rows"] == 7, "the row limit is the saved one until its own variable is set"
    assert state["saved"] == {"uri": "s3://saved/x", "connection": s3_connection, "max_rows": 7}
    assert settings.get_exchange_uri() == "s3://pinned/y"
    monkeypatch.setenv("FLOWFILE_EXCHANGE_MAX_ROWS", "5")
    assert settings.get_exchange_max_rows() == 5
    monkeypatch.setenv("FLOWFILE_EXCHANGE_MAX_ROWS", "five")
    with pytest.raises(ValueError, match="whole number"):
        settings.get_exchange_max_rows()


def test_an_unreadable_file_is_no_store(no_env):
    exchange_config._settings_file().write_text("- not: [a mapping", encoding="utf-8")

    assert exchange_config.saved() == exchange_config.ExchangeConfig()

    exchange_config._settings_file().write_text("uri: 3\nconnection: ''\nmax_rows: true\n", encoding="utf-8")

    assert exchange_config.saved() == exchange_config.ExchangeConfig()
