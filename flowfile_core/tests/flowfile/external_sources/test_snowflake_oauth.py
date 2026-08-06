"""Snowflake SSO / OAuth tests: connection CRUD, refresh-token custody, core-side
token minting (with rotation persistence), the reconnect-required surface, the
signed-state OAuth routes, and the projection's re-auth marking.

The token-endpoint HTTP behavior itself is covered in
shared/tests/test_snowflake_oauth.py against a mock server; here the shared
helpers are monkeypatched so these tests exercise the core plumbing only. The
live leg is gated on FLOWFILE_TEST_SNOWFLAKE_OAUTH_* (a real security
integration) — fakesnow is irrelevant to auth.
"""

import os

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr, ValidationError

from flowfile_core import main
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import Secret
from flowfile_core.flowfile.database_connection_manager import db_oauth
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_database_connection,
    get_all_database_connections_interface,
    get_database_connection,
    get_database_connection_schema,
    store_database_connection,
    update_database_connection,
)
from flowfile_core.flowfile.database_connection_manager.db_oauth import (
    ReconnectRequiredError,
    resolve_oauth_access_token,
    resolve_oauth_endpoints,
    store_refresh_token,
)
from flowfile_core.schemas.input_schema import DatabaseConnection, FullDatabaseConnection
from flowfile_core.secret_manager.secret_manager import decrypt_secret
from shared.snowflake_oauth import SnowflakeOAuthError, TokenResponse

USER_ID = 1
EXTRA_PARAMS = {"account": "myorg-myaccount", "warehouse": "COMPUTE_WH"}


def _oauth_connection(
    name: str,
    client_id: str | None = "client-abc",
    client_secret: str | None = "sekrit",
    token_endpoint: str | None = None,
    authorize_endpoint: str | None = None,
) -> FullDatabaseConnection:
    return FullDatabaseConnection(
        connection_name=name,
        database_type="snowflake",
        username="user",
        password=SecretStr(""),
        database="ANALYTICS",
        extra_params=EXTRA_PARAMS,
        auth_method="oauth",
        oauth_client_id=client_id,
        oauth_client_secret=SecretStr(client_secret) if client_secret is not None else None,
        oauth_authorize_endpoint=authorize_endpoint,
        oauth_token_endpoint=token_endpoint,
    )


def _cleanup(name: str) -> None:
    with get_db_context() as db:
        if get_database_connection(db, name, USER_ID) is not None:
            delete_database_connection(db, name, USER_ID)


def _store(connection: FullDatabaseConnection) -> None:
    with get_db_context() as db:
        store_database_connection(db, connection, USER_ID)


def _sign_in(name: str, refresh_token: str = "rt-initial") -> None:
    with get_db_context() as db:
        row = get_database_connection(db, name, USER_ID)
        store_refresh_token(db, row, refresh_token)


class TestOauthCrud:
    def test_store_and_reload_round_trips_client_config(self):
        name = "snowflake_oauth_store"
        _cleanup(name)
        try:
            _store(_oauth_connection(name))
            with get_db_context() as db:
                reloaded = get_database_connection_schema(db, name, USER_ID)
            assert reloaded.auth_method == "oauth"
            assert reloaded.oauth_client_id == "client-abc"
            secret_ciphertext = reloaded.oauth_client_secret.get_secret_value()
            assert secret_ciphertext.startswith("$ffsec$"), "schema must return ciphertext, never the secret"
            assert decrypt_secret(secret_ciphertext).get_secret_value() == "sekrit"
            assert reloaded.oauth_refresh_token is None, "no sign-in yet"
        finally:
            _cleanup(name)

    def test_store_requires_client_config(self):
        _cleanup("snowflake_oauth_missing")
        with get_db_context() as db:
            with pytest.raises(ValueError, match="client id and client secret"):
                store_database_connection(db, _oauth_connection("snowflake_oauth_missing", client_secret=None), USER_ID)

    def test_interface_reports_connected_state_without_secrets(self):
        name = "snowflake_oauth_iface"
        _cleanup(name)
        try:
            _store(_oauth_connection(name))
            with get_db_context() as db:
                iface = next(
                    i for i in get_all_database_connections_interface(db, USER_ID) if i.connection_name == name
                )
            assert iface.auth_method == "oauth"
            assert iface.oauth_client_id == "client-abc"
            assert iface.oauth_connected is False
            assert not hasattr(iface, "oauth_client_secret")
            assert not hasattr(iface, "oauth_refresh_token")

            _sign_in(name)
            with get_db_context() as db:
                iface = next(
                    i for i in get_all_database_connections_interface(db, USER_ID) if i.connection_name == name
                )
            assert iface.oauth_connected is True
        finally:
            _cleanup(name)

    def test_update_rotates_client_secret_and_empty_keeps_existing(self):
        name = "snowflake_oauth_rotate"
        _cleanup(name)
        try:
            _store(_oauth_connection(name))
            with get_db_context() as db:
                update_database_connection(db, _oauth_connection(name, client_secret=""), USER_ID)
            with get_db_context() as db:
                kept = get_database_connection_schema(db, name, USER_ID).oauth_client_secret.get_secret_value()
            assert decrypt_secret(kept).get_secret_value() == "sekrit"

            with get_db_context() as db:
                update_database_connection(db, _oauth_connection(name, client_secret="rotated"), USER_ID)
            with get_db_context() as db:
                rotated = get_database_connection_schema(db, name, USER_ID).oauth_client_secret.get_secret_value()
            assert decrypt_secret(rotated).get_secret_value() == "rotated"
        finally:
            _cleanup(name)

    def test_client_change_drops_the_refresh_token(self):
        name = "snowflake_oauth_repoint"
        _cleanup(name)
        try:
            _store(_oauth_connection(name))
            _sign_in(name)
            with get_db_context() as db:
                token_secret_id = get_database_connection(db, name, USER_ID).oauth_refresh_token_id
            assert token_secret_id is not None

            with get_db_context() as db:
                update_database_connection(db, _oauth_connection(name, client_id="other-client"), USER_ID)
            with get_db_context() as db:
                row = get_database_connection(db, name, USER_ID)
                assert row.oauth_refresh_token_id is None, "a changed client must force re-authentication"
                assert db.query(Secret).filter(Secret.id == token_secret_id).count() == 0
        finally:
            _cleanup(name)

    def test_unchanged_client_keeps_the_refresh_token(self):
        name = "snowflake_oauth_keep"
        _cleanup(name)
        try:
            _store(_oauth_connection(name))
            _sign_in(name)
            with get_db_context() as db:
                update_database_connection(db, _oauth_connection(name, client_secret=""), USER_ID)
            with get_db_context() as db:
                assert get_database_connection(db, name, USER_ID).oauth_refresh_token_id is not None
        finally:
            _cleanup(name)

    def test_switching_away_from_oauth_deletes_oauth_secrets(self):
        name = "snowflake_oauth_switch"
        _cleanup(name)
        try:
            _store(_oauth_connection(name))
            _sign_in(name)
            with get_db_context() as db:
                row = get_database_connection(db, name, USER_ID)
                oauth_secret_ids = [row.oauth_client_secret_id, row.oauth_refresh_token_id]
            assert all(secret_id is not None for secret_id in oauth_secret_ids)

            password_connection = FullDatabaseConnection(
                connection_name=name,
                database_type="snowflake",
                username="user",
                password=SecretStr("new-pass"),
                database="ANALYTICS",
                extra_params=EXTRA_PARAMS,
            )
            with get_db_context() as db:
                update_database_connection(db, password_connection, USER_ID)
            with get_db_context() as db:
                row = get_database_connection(db, name, USER_ID)
                assert row.oauth_client_id is None
                assert row.oauth_client_secret_id is None
                assert row.oauth_refresh_token_id is None
                assert db.query(Secret).filter(Secret.id.in_(oauth_secret_ids)).count() == 0
        finally:
            _cleanup(name)

    def test_delete_removes_oauth_secrets(self):
        name = "snowflake_oauth_delete"
        _cleanup(name)
        _store(_oauth_connection(name))
        _sign_in(name)
        with get_db_context() as db:
            row = get_database_connection(db, name, USER_ID)
            secret_ids = [row.password_id, row.oauth_client_secret_id, row.oauth_refresh_token_id]
        assert all(secret_id is not None for secret_id in secret_ids)
        with get_db_context() as db:
            delete_database_connection(db, name, USER_ID)
        with get_db_context() as db:
            assert db.query(Secret).filter(Secret.id.in_(secret_ids)).count() == 0


class TestEndpointResolution:
    def test_endpoints_derive_from_account(self):
        name = "snowflake_oauth_derive"
        _cleanup(name)
        try:
            _store(_oauth_connection(name))
            with get_db_context() as db:
                endpoints = resolve_oauth_endpoints(get_database_connection(db, name, USER_ID))
            assert endpoints.authorize_endpoint == "https://myorg-myaccount.snowflakecomputing.com/oauth/authorize"
            assert endpoints.token_endpoint == "https://myorg-myaccount.snowflakecomputing.com/oauth/token-request"
        finally:
            _cleanup(name)

    def test_explicit_endpoints_win(self):
        name = "snowflake_oauth_external"
        _cleanup(name)
        try:
            _store(
                _oauth_connection(
                    name,
                    authorize_endpoint="https://idp.example.com/authorize",
                    token_endpoint="https://idp.example.com/token",
                )
            )
            with get_db_context() as db:
                endpoints = resolve_oauth_endpoints(get_database_connection(db, name, USER_ID))
            assert endpoints.authorize_endpoint == "https://idp.example.com/authorize"
            assert endpoints.token_endpoint == "https://idp.example.com/token"
        finally:
            _cleanup(name)


class TestAccessTokenResolution:
    def test_resolve_mints_encrypted_access_token(self, monkeypatch):
        name = "snowflake_oauth_resolve"
        _cleanup(name)
        try:
            _store(_oauth_connection(name))
            _sign_in(name, "rt-stored")
            calls = {}

            def fake_refresh(token_endpoint, client_id, client_secret, refresh_token):
                calls.update(
                    endpoint=token_endpoint,
                    client_id=client_id,
                    client_secret=client_secret,
                    refresh_token=refresh_token,
                )
                return TokenResponse(access_token="at-fresh", expires_in=600, refresh_token=None)

            monkeypatch.setattr(db_oauth, "refresh_access_token", fake_refresh)
            ciphertext = resolve_oauth_access_token(name, USER_ID)
            assert ciphertext.startswith("$ffsec$")
            assert decrypt_secret(ciphertext).get_secret_value() == "at-fresh"
            assert calls["endpoint"] == "https://myorg-myaccount.snowflakecomputing.com/oauth/token-request"
            assert calls["client_id"] == "client-abc"
            assert calls["client_secret"] == "sekrit"
            assert calls["refresh_token"] == "rt-stored"
        finally:
            _cleanup(name)

    def test_rotated_refresh_token_is_persisted(self, monkeypatch):
        name = "snowflake_oauth_rotation"
        _cleanup(name)
        try:
            _store(_oauth_connection(name))
            _sign_in(name, "rt-old")
            monkeypatch.setattr(
                db_oauth,
                "refresh_access_token",
                lambda *a, **k: TokenResponse(access_token="at", expires_in=600, refresh_token="rt-rotated"),
            )
            resolve_oauth_access_token(name, USER_ID)
            with get_db_context() as db:
                row = get_database_connection(db, name, USER_ID)
                stored = db.query(Secret).filter(Secret.id == row.oauth_refresh_token_id).first()
            assert decrypt_secret(stored.encrypted_value).get_secret_value() == "rt-rotated"
        finally:
            _cleanup(name)

    def test_never_signed_in_raises_reconnect_required(self):
        name = "snowflake_oauth_nosignin"
        _cleanup(name)
        try:
            _store(_oauth_connection(name))
            with pytest.raises(ReconnectRequiredError, match="Reconnect"):
                resolve_oauth_access_token(name, USER_ID)
        finally:
            _cleanup(name)

    def test_invalid_grant_raises_reconnect_required(self, monkeypatch):
        name = "snowflake_oauth_expired"
        _cleanup(name)
        try:
            _store(_oauth_connection(name))
            _sign_in(name)

            def fake_refresh(*args, **kwargs):
                raise SnowflakeOAuthError("expired", error="invalid_grant", status_code=400)

            monkeypatch.setattr(db_oauth, "refresh_access_token", fake_refresh)
            with pytest.raises(ReconnectRequiredError, match="sign-in has expired"):
                resolve_oauth_access_token(name, USER_ID)
        finally:
            _cleanup(name)

    def test_transient_idp_error_is_not_reconnect(self, monkeypatch):
        name = "snowflake_oauth_transient"
        _cleanup(name)
        try:
            _store(_oauth_connection(name))
            _sign_in(name)

            def fake_refresh(*args, **kwargs):
                raise SnowflakeOAuthError("upstream down", status_code=503)

            monkeypatch.setattr(db_oauth, "refresh_access_token", fake_refresh)
            with pytest.raises(SnowflakeOAuthError):
                resolve_oauth_access_token(name, USER_ID)
        finally:
            _cleanup(name)


class TestModelValidation:
    def test_inline_oauth_without_token_is_rejected(self):
        with pytest.raises(ValidationError, match="stored connection"):
            DatabaseConnection(database_type="snowflake", username="u", auth_method="oauth")

    def test_oauth_rejected_for_non_supporting_dialect(self):
        with pytest.raises(ValidationError, match="not supported"):
            FullDatabaseConnection(
                connection_name="x",
                database_type="postgresql",
                username="u",
                password=SecretStr("p"),
                auth_method="oauth",
            )

    def test_wire_model_with_token_ciphertext_passes(self):
        from flowfile_core.flowfile.sources.external_sources.sql_source.models import ExtDatabaseConnection

        connection = ExtDatabaseConnection(
            database_type="snowflake",
            username="u",
            auth_method="oauth",
            extra_params=EXTRA_PARAMS,
            oauth_token="$ffsec$1$1$abc",
        )
        assert connection.oauth_token == "$ffsec$1$1$abc"


def _get_test_client() -> TestClient:
    with TestClient(main.app) as auth_client:
        token = auth_client.post("/auth/token").json()["access_token"]
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


class TestOauthRoutes:
    def test_state_round_trip_and_type_check(self):
        from fastapi import HTTPException

        from flowfile_core.routes import db_oauth as routes

        state = routes._sign_oauth_state(user_id=7, connection_name="conn-a")
        payload = routes._verify_oauth_state(state)
        assert payload["user_id"] == 7
        assert payload["connection_name"] == "conn-a"
        with pytest.raises(HTTPException):
            routes._verify_oauth_state(state + "tampered")

    def test_start_builds_authorize_url(self):
        name = "snowflake_oauth_start"
        _cleanup(name)
        try:
            _store(_oauth_connection(name))
            client = _get_test_client()
            response = client.get("/db_connection_lib/oauth/start", params={"connection_name": name})
            assert response.status_code == 200, response.text
            auth_url = response.json()["auth_url"]
            assert auth_url.startswith("https://myorg-myaccount.snowflakecomputing.com/oauth/authorize?")
            assert "client_id=client-abc" in auth_url
            assert "scope=refresh_token" in auth_url, "Snowflake OAuth uses its proprietary refresh scope"
            assert "state=" in auth_url
        finally:
            _cleanup(name)

    def test_start_uses_offline_access_for_external_idp(self):
        name = "snowflake_oauth_start_ext"
        _cleanup(name)
        try:
            _store(
                _oauth_connection(
                    name,
                    authorize_endpoint="https://idp.example.com/authorize",
                    token_endpoint="https://idp.example.com/token",
                )
            )
            client = _get_test_client()
            response = client.get("/db_connection_lib/oauth/start", params={"connection_name": name})
            assert response.status_code == 200, response.text
            auth_url = response.json()["auth_url"]
            assert auth_url.startswith("https://idp.example.com/authorize?")
            assert "scope=offline_access" in auth_url
        finally:
            _cleanup(name)

    def test_start_rejects_non_oauth_connection(self):
        name = "snowflake_oauth_start_pw"
        _cleanup(name)
        try:
            _store(
                FullDatabaseConnection(
                    connection_name=name,
                    database_type="snowflake",
                    username="u",
                    password=SecretStr("p"),
                    extra_params=EXTRA_PARAMS,
                )
            )
            client = _get_test_client()
            response = client.get("/db_connection_lib/oauth/start", params={"connection_name": name})
            assert response.status_code == 422
        finally:
            _cleanup(name)

    def test_callback_stores_refresh_token(self, monkeypatch):
        from flowfile_core.routes import db_oauth as routes

        name = "snowflake_oauth_callback"
        _cleanup(name)
        try:
            _store(_oauth_connection(name))
            calls = {}

            def fake_exchange(token_endpoint, client_id, client_secret, code, redirect_uri):
                calls.update(code=code, redirect_uri=redirect_uri)
                return TokenResponse(access_token="at", expires_in=600, refresh_token="rt-from-code")

            monkeypatch.setattr(routes, "exchange_authorization_code", fake_exchange)
            state = routes._sign_oauth_state(user_id=USER_ID, connection_name=name)
            client = TestClient(main.app)
            response = client.get(
                "/db_connection_lib/oauth/callback", params={"code": "the-code", "state": state}
            )
            assert response.status_code == 200, response.text
            assert "signed in" in response.text
            assert calls["code"] == "the-code"
            assert calls["redirect_uri"] == "http://localhost:63578/db_connection_lib/oauth/callback"
            with get_db_context() as db:
                row = get_database_connection(db, name, USER_ID)
                stored = db.query(Secret).filter(Secret.id == row.oauth_refresh_token_id).first()
            assert decrypt_secret(stored.encrypted_value).get_secret_value() == "rt-from-code"
        finally:
            _cleanup(name)

    def test_callback_rejects_invalid_state(self):
        client = TestClient(main.app)
        response = client.get("/db_connection_lib/oauth/callback", params={"code": "c", "state": "garbage"})
        assert response.status_code == 400
        assert "Invalid OAuth state" in response.text

    def test_callback_without_refresh_token_errors(self, monkeypatch):
        from flowfile_core.routes import db_oauth as routes

        name = "snowflake_oauth_no_rt"
        _cleanup(name)
        try:
            _store(_oauth_connection(name))
            monkeypatch.setattr(
                routes,
                "exchange_authorization_code",
                lambda *a, **k: TokenResponse(access_token="at", expires_in=600, refresh_token=None),
            )
            state = routes._sign_oauth_state(user_id=USER_ID, connection_name=name)
            client = TestClient(main.app)
            response = client.get("/db_connection_lib/oauth/callback", params={"code": "c", "state": state})
            assert response.status_code == 400
            assert "did not return a refresh token" in response.text
            with get_db_context() as db:
                assert get_database_connection(db, name, USER_ID).oauth_refresh_token_id is None
        finally:
            _cleanup(name)

    def test_reconnect_required_surfaces_as_422_error_code(self, monkeypatch):
        name = "snowflake_oauth_422"
        _cleanup(name)
        try:
            _store(_oauth_connection(name))
            client = _get_test_client()
            # Never signed in -> resolving credentials for a browse call must 422.
            response = client.post(
                "/db_schemas",
                json={
                    "connection_mode": "reference",
                    "database_connection_name": name,
                    "query_mode": "table",
                },
            )
            if response.status_code == 404:
                pytest.skip("no /db_schemas route in this build")
            assert response.status_code == 422, response.text
            detail = response.json()["detail"]
            assert detail["error_code"] == "RECONNECT_REQUIRED"
            assert "Reconnect" in detail["message"]
        finally:
            _cleanup(name)


class TestOauthRepointGuard:
    @staticmethod
    def _guard(changed, has_new_credentials, has_bundled_secrets=True):
        from fastapi import HTTPException

        from flowfile_core.routes._connection_sharing import require_credentials_on_target_change

        try:
            require_credentials_on_target_change(
                changed, has_new_credentials=has_new_credentials, has_bundled_secrets=has_bundled_secrets
            )
        except HTTPException:
            return False
        return True

    def test_endpoint_change_without_credentials_blocked(self):
        assert self._guard(["oauth_token_endpoint"], has_new_credentials=False) is False

    def test_endpoint_change_with_new_client_secret_allowed(self):
        assert self._guard(["oauth_token_endpoint"], has_new_credentials=True) is True


class TestProjectionMarksReauth:
    def test_import_of_oauth_connection_warns_and_skips_token(self):
        from flowfile_core.project.importer import _import_db_connection
        from flowfile_core.project.models import SetupResult

        name = "snowflake_oauth_projected"
        _cleanup(name)
        try:
            result = SetupResult()
            data = {
                "kind": "database_connection",
                "connection_name": name,
                "database_type": "snowflake",
                "username": "user",
                "auth_method": "oauth",
                "extra_params": EXTRA_PARAMS,
                "password": f"${{secret:{name}}}",
                "oauth_client_id": "client-abc",
                "oauth_client_secret": f"${{secret:{name}_oauth_client_secret}}",
            }
            imported = _import_db_connection(data, USER_ID, {}, result)
            assert imported == name
            assert any("re-authentication" in w for w in result.warnings)
            with get_db_context() as db:
                row = get_database_connection(db, name, USER_ID)
                assert row.auth_method == "oauth"
                assert row.oauth_client_id == "client-abc"
                assert row.oauth_refresh_token_id is None, "token material must never come from a projection"
        finally:
            _cleanup(name)

    def test_projection_never_writes_refresh_token(self):
        from flowfile_core.project.projection import _db_connection_dict

        name = "snowflake_oauth_project_out"
        _cleanup(name)
        try:
            _store(_oauth_connection(name))
            _sign_in(name, "rt-secret-material")
            with get_db_context() as db:
                row = get_database_connection(db, name, USER_ID)
                projected = _db_connection_dict(db, row)
            flat = str(projected)
            assert "rt-secret-material" not in flat
            assert "refresh_token" not in flat
            assert projected["oauth_client_id"] == "client-abc"
            assert projected["oauth_client_secret"] == f"${{secret:{name}_oauth_client_secret}}"
        finally:
            _cleanup(name)


_LIVE_OAUTH_ENV = ("ACCOUNT", "USER", "DATABASE", "WAREHOUSE", "CLIENT_ID", "CLIENT_SECRET", "REFRESH_TOKEN")

snowflake_oauth_available = pytest.mark.skipif(
    not all(os.environ.get(f"FLOWFILE_TEST_SNOWFLAKE_OAUTH_{name}") for name in _LIVE_OAUTH_ENV),
    reason="FLOWFILE_TEST_SNOWFLAKE_OAUTH_* credentials are not configured",
)


@snowflake_oauth_available
class TestSnowflakeOauthLive:
    def test_refresh_and_read_round_trip(self):
        """Real security integration: refresh the token, connect, run SELECT 1."""
        from shared.db_dialects import get_dialect
        from shared.snowflake_oauth import derive_snowflake_endpoints, refresh_access_token

        env = {name: os.environ[f"FLOWFILE_TEST_SNOWFLAKE_OAUTH_{name}"] for name in _LIVE_OAUTH_ENV}
        _, token_endpoint = derive_snowflake_endpoints(env["ACCOUNT"])
        token = refresh_access_token(
            token_endpoint, env["CLIENT_ID"], env["CLIENT_SECRET"], env["REFRESH_TOKEN"]
        )
        dialect = get_dialect("snowflake")
        uri = dialect.build_uri(
            username=env["USER"],
            database=env["DATABASE"],
            account=env["ACCOUNT"],
            warehouse=env["WAREHOUSE"],
            auth_method="oauth",
            oauth_token=token.access_token,
        )
        import logging

        df = dialect.read("SELECT 1 AS one", uri, logging.getLogger(__name__))
        assert df["one"].to_list() == [1]
