"""HTTP-surface tests for the dialect catalog, the vocabulary gates, and the browse routes."""

import base64
import sqlite3

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
from pydantic import SecretStr

from flowfile_core import main
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import Secret
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_database_connection,
    get_database_connection,
    store_database_connection,
)
from flowfile_core.schemas import input_schema
from flowfile_core.secret_manager.secret_manager import decrypt_secret
from shared.db_dialects import KNOWN_DIALECT_NAMES, SnowflakeDialect


def get_test_client() -> TestClient:
    with TestClient(main.app) as auth_client:
        token = auth_client.post("/auth/token").json()["access_token"]
    _client = TestClient(main.app)
    _client.headers = {"Authorization": f"Bearer {token}"}
    return _client


client = get_test_client()


def _cleanup_connection(connection_name: str) -> None:
    with get_db_context() as db:
        if get_database_connection(db, connection_name, 1) is not None:
            delete_database_connection(db, connection_name, 1)


def test_get_db_dialects():
    response = client.get("/db_dialects")
    assert response.status_code == 200, response.text
    dialects = response.json()
    assert [d["name"] for d in dialects] == list(KNOWN_DIALECT_NAMES)
    by_name = {d["name"]: d for d in dialects}
    assert by_name["sqlite"]["file_based"] is True
    assert by_name["sqlite"]["default_port"] is None
    assert by_name["postgresql"]["default_port"] == 5432
    assert by_name["postgresql"]["supports_ssl"] is True
    # Snowflake's driver is an optional extra; every other driver ships with Flowfile.
    assert all(d["available"] is True for d in dialects if d["name"] != "snowflake")
    snowflake = by_name["snowflake"]
    assert snowflake["display_name"] == "Snowflake"
    assert snowflake["saved_connection_only"] is True
    assert snowflake["auth_methods"] == ["oauth", "access_token", "key_pair"]
    assert snowflake["install_hint"] == 'pip install "flowfile[snowflake]"'
    assert isinstance(snowflake["available"], bool)
    assert all(d["auth_methods"] == [] and d["saved_connection_only"] is False for d in dialects if d is not snowflake)


def test_create_db_connection_rejects_unknown_dialect():
    payload = {
        "connection_name": "unknown_dialect_conn",
        "database_type": "mariadb",
        "username": "u",
        "password": "pw",
        "host": "h",
        "port": 3306,
        "database": "d",
    }
    response = client.post("/db_connection_lib", json=payload)
    assert response.status_code == 422, response.text
    assert "Unsupported database type" in response.text
    with get_db_context() as db:
        assert get_database_connection(db, "unknown_dialect_conn", 1) is None


def test_create_db_connection_accepts_mixed_case_dialect():
    _cleanup_connection("mixed_case_conn")
    payload = {
        "connection_name": "mixed_case_conn",
        "database_type": "PostgreSQL",
        "username": "u",
        "password": "pw",
        "host": "h",
        "port": 5432,
        "database": "d",
    }
    response = client.post("/db_connection_lib", json=payload)
    assert response.status_code == 200, response.text
    with get_db_context() as db:
        stored = get_database_connection(db, "mixed_case_conn", 1)
        assert stored.database_type == "postgresql", "stored type must be canonical lowercase"
    _cleanup_connection("mixed_case_conn")


def test_update_db_connection_keeps_legacy_type_but_blocks_switch_to_unknown():
    """Stored pre-registry types (free-string era) must stay updatable in place."""
    _cleanup_connection("legacy_redshift_conn")
    with get_db_context() as db:
        store_database_connection(
            db,
            input_schema.FullDatabaseConnection(
                connection_name="legacy_redshift_conn",
                database_type="redshift",
                username="u",
                password=SecretStr("old_pw"),
                host="h",
                port=5439,
                database="d",
            ),
            1,
        )

    payload = {
        "connection_name": "legacy_redshift_conn",
        "database_type": "redshift",
        "username": "u",
        "password": "rotated_pw",
        "host": "h",
        "port": 5439,
        "database": "d",
    }
    response = client.put("/db_connection_lib", json=payload)
    assert response.status_code == 200, f"password rotation on a legacy type must keep working: {response.text}"

    payload["database_type"] = "mariadb"
    response = client.put("/db_connection_lib", json=payload)
    assert response.status_code == 422, response.text
    assert "Unsupported database type" in response.text

    _cleanup_connection("legacy_redshift_conn")


def test_update_same_dialect_different_case_is_not_a_type_change(tmp_path):
    """Resubmitting 'SQLite' over a stored 'sqlite' must not trip the changed-type
    gate or the re-enter-credentials guard."""
    _cleanup_connection("case_insensitive_conn")
    db_path = str(tmp_path / "case.db")
    with get_db_context() as db:
        store_database_connection(
            db,
            input_schema.FullDatabaseConnection(
                connection_name="case_insensitive_conn",
                database_type="sqlite",
                username="",
                password=SecretStr(""),
                database=db_path,
            ),
            1,
        )

    payload = {
        "connection_name": "case_insensitive_conn",
        "database_type": "SQLite",
        "username": "",
        "password": "",
        "database": db_path,
    }
    response = client.put("/db_connection_lib", json=payload)
    assert response.status_code == 200, response.text

    _cleanup_connection("case_insensitive_conn")


def _browse_settings(database_type: str, db_path: str) -> dict:
    return {
        "connection_mode": "inline",
        "database_connection": {"database_type": database_type, "database": db_path},
        "query_mode": "table",
    }


def test_db_schemas_and_db_tables_endpoints(tmp_path):
    db_path = str(tmp_path / "browse.db")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE movies (id INTEGER, title TEXT)")
    conn.execute("CREATE TABLE actors (id INTEGER, name TEXT)")
    conn.commit()
    conn.close()

    response = client.post("/db_schemas", json=_browse_settings("sqlite", db_path))
    assert response.status_code == 200, response.text
    assert "main" in response.json()

    response = client.post("/db_tables", json=_browse_settings("sqlite", db_path))
    assert response.status_code == 200, response.text
    assert {"main.movies", "main.actors"}.issubset(set(response.json()))

    settings = _browse_settings("sqlite", db_path)
    settings["schema_name"] = "main"
    response = client.post("/db_tables", json=settings)
    assert response.status_code == 200, response.text
    assert {"movies", "actors"}.issubset(set(response.json()))


def _make_duckdb_file(tmp_path) -> str:
    import duckdb

    db_path = str(tmp_path / "browse.duckdb")
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE movies (id INTEGER, title VARCHAR, rating DOUBLE)")
    con.execute("INSERT INTO movies VALUES (1, 'The Matrix', 8.7)")
    con.close()
    return db_path


def test_duckdb_in_dialect_catalog():
    response = client.get("/db_dialects")
    assert response.status_code == 200, response.text
    duckdb_entry = next(d for d in response.json() if d["name"] == "duckdb")
    assert duckdb_entry["file_based"] is True
    assert duckdb_entry["default_port"] is None
    assert duckdb_entry["available"] is True


def test_mssql_in_dialect_catalog():
    response = client.get("/db_dialects")
    assert response.status_code == 200, response.text
    mssql_entry = next(d for d in response.json() if d["name"] == "mssql")
    assert mssql_entry["display_name"] == "SQL Server"
    assert mssql_entry["file_based"] is False
    assert mssql_entry["default_port"] == 1433
    assert mssql_entry["available"] is True


def test_denodo_in_dialect_catalog():
    response = client.get("/db_dialects")
    assert response.status_code == 200, response.text
    denodo_entry = next(d for d in response.json() if d["name"] == "denodo")
    assert denodo_entry["display_name"] == "Denodo"
    assert denodo_entry["file_based"] is False
    assert denodo_entry["default_port"] == 9996
    assert denodo_entry["supports_ssl"] is True
    assert denodo_entry["available"] is True


def test_create_duckdb_connection_without_credentials():
    _cleanup_connection("duckdb_conn")
    payload = {
        "connection_name": "duckdb_conn",
        "database_type": "duckdb",
        "username": "",
        "password": "",
        "database": "/tmp/some.duckdb",
    }
    response = client.post("/db_connection_lib", json=payload)
    assert response.status_code == 200, response.text
    listed = client.get("/db_connection_lib").json()
    assert any(c["connection_name"] == "duckdb_conn" for c in listed)
    _cleanup_connection("duckdb_conn")


def test_duckdb_browse_and_validate_endpoints(tmp_path):
    db_path = _make_duckdb_file(tmp_path)

    response = client.post("/db_schemas", json=_browse_settings("duckdb", db_path))
    assert response.status_code == 200, response.text
    assert "main" in response.json()

    response = client.post("/db_tables", json=_browse_settings("duckdb", db_path))
    assert response.status_code == 200, response.text
    assert "main.movies" in response.json()

    settings = _browse_settings("duckdb", db_path)
    settings["table_name"] = "movies"
    response = client.post("/validate_db_settings", json=settings)
    assert response.status_code == 200, response.text

    settings["table_name"] = "does_not_exist"
    response = client.post("/validate_db_settings", json=settings)
    assert response.status_code == 422, response.text


# ---------- Snowflake connections ----------

SF_NAME = "sf_conn"


@pytest.fixture(scope="module")
def rsa_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _pem(key, passphrase: str | None = None) -> str:
    encryption = (
        serialization.BestAvailableEncryption(passphrase.encode()) if passphrase else serialization.NoEncryption()
    )
    return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, encryption).decode()


def _stored_der(key) -> str:
    der = key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    return base64.b64encode(der).decode()


@pytest.fixture
def snowflake_available(monkeypatch):
    monkeypatch.setattr(SnowflakeDialect, "is_available", lambda self: True)
    _cleanup_connection(SF_NAME)
    yield
    _cleanup_connection(SF_NAME)


def _sf_body(**overrides) -> dict:
    body = {
        "connection_name": SF_NAME,
        "database_type": "snowflake",
        "username": "ME@EXAMPLE.COM",
        "password": "pat-token-123",
        "host": "https://app.snowflake.com/myorg/myaccount/#/homepage",
        "port": 443,
        "database": "ANALYTICS",
        "ssl_enabled": True,
        "auth_method": "access_token",
        "role": "ANALYST",
        "warehouse": "COMPUTE_WH",
    }
    return body | overrides


def _stored_row_and_secret(name: str = SF_NAME):
    with get_db_context() as db:
        row = get_database_connection(db, name, 1)
        assert row is not None
        secret = db.query(Secret).filter(Secret.id == row.password_id).one()
        plain = decrypt_secret(secret.encrypted_value).get_secret_value()
        return (row.host, row.port, row.username, row.auth_method, row.role, row.warehouse, row.database), plain


def test_snowflake_create_needs_the_driver(monkeypatch):
    _cleanup_connection(SF_NAME)
    monkeypatch.setattr(SnowflakeDialect, "is_available", lambda self: False)
    response = client.post("/db_connection_lib", json=_sf_body())
    assert response.status_code == 422, response.text
    assert response.json()["detail"] == (
        'The Snowflake driver is not installed. Install it with: pip install "flowfile[snowflake]", '
        "then restart Flowfile."
    )
    with get_db_context() as db:
        assert get_database_connection(db, SF_NAME, 1) is None


def test_snowflake_access_token_connection_round_trip(snowflake_available):
    response = client.post("/db_connection_lib", json=_sf_body(password="  pat-token-123  "))
    assert response.status_code == 200, response.text
    fields, secret = _stored_row_and_secret()
    assert fields == (
        "myorg-myaccount.snowflakecomputing.com",
        None,
        "ME@EXAMPLE.COM",
        "access_token",
        "ANALYST",
        "COMPUTE_WH",
        "ANALYTICS",
    )
    assert secret == "pat-token-123"

    listed = next(c for c in client.get("/db_connection_lib").json() if c["connection_name"] == SF_NAME)
    assert listed["auth_method"] == "access_token"
    assert listed["role"] == "ANALYST"
    assert listed["warehouse"] == "COMPUTE_WH"
    assert listed["host"] == "myorg-myaccount.snowflakecomputing.com"
    assert "password" not in listed

    # A blank secret with the method unchanged keeps the stored token; the role may change.
    response = client.put("/db_connection_lib", json=_sf_body(password="", role="", warehouse="BIG_WH"))
    assert response.status_code == 200, response.text
    fields, secret = _stored_row_and_secret()
    assert fields[4:6] == (None, "BIG_WH")
    assert secret == "pat-token-123"

    response = client.put("/db_connection_lib", json=_sf_body(password="pat-token-456"))
    assert response.status_code == 200, response.text
    assert _stored_row_and_secret()[1] == "pat-token-456"

    assert client.delete("/db_connection_lib", params={"connection_name": SF_NAME}).status_code == 200
    with get_db_context() as db:
        assert get_database_connection(db, SF_NAME, 1) is None


@pytest.mark.parametrize("passphrase", [None, "open sesame"])
def test_snowflake_key_pair_stores_the_normalised_key(snowflake_available, rsa_key, passphrase):
    body = _sf_body(auth_method="key_pair", password=_pem(rsa_key, passphrase), private_key_passphrase=passphrase)
    response = client.post("/db_connection_lib", json=body)
    assert response.status_code == 200, response.text
    fields, secret = _stored_row_and_secret()
    assert fields[3] == "key_pair"
    assert secret == _stored_der(rsa_key), "the key is stored unencrypted as PKCS8 DER in base64"
    assert passphrase is None or passphrase not in secret


def test_snowflake_key_pair_refusals(snowflake_available, rsa_key):
    encrypted = _pem(rsa_key, "right")
    wrong = client.post(
        "/db_connection_lib", json=_sf_body(auth_method="key_pair", password=encrypted, private_key_passphrase="wrong")
    )
    assert wrong.status_code == 422
    assert wrong.json()["detail"] == "The passphrase does not open this private key."
    missing = client.post("/db_connection_lib", json=_sf_body(auth_method="key_pair", password=encrypted))
    assert missing.json()["detail"] == "This private key is encrypted. Enter its passphrase."
    public = (
        rsa_key.public_key()
        .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        .decode()
    )
    assert (
        "public key"
        in client.post("/db_connection_lib", json=_sf_body(auth_method="key_pair", password=public)).json()["detail"]
    )
    with get_db_context() as db:
        assert get_database_connection(db, SF_NAME, 1) is None


@pytest.mark.parametrize(
    ("overrides", "detail"),
    [
        ({"host": "https://evil.example.com/x"}, "is not a Snowflake account"),
        ({"host": ""}, "Enter your Snowflake account"),
        ({"username": ""}, "Enter your Snowflake username."),
        ({"auth_method": None}, "Choose how to sign in to Snowflake"),
        ({"auth_method": "password"}, "Choose how to sign in to Snowflake"),
        ({"password": ""}, "Paste a Snowflake access token."),
        ({"auth_method": "key_pair", "password": ""}, "Choose your private key file (.p8)."),
        ({"auth_method": "oauth", "password": "", "username": ""}, "Sign in with Snowflake to save this connection."),
        ({"auth_method": "oauth", "password": "", "oauth_flow_id": "nope"}, "expired. Sign in again."),
    ],
)
def test_snowflake_create_refusals(snowflake_available, overrides, detail):
    response = client.post("/db_connection_lib", json=_sf_body(**overrides))
    assert response.status_code == 422, response.text
    assert detail in response.json()["detail"]
    with get_db_context() as db:
        assert get_database_connection(db, SF_NAME, 1) is None


def test_snowflake_switching_method_needs_the_new_secret(snowflake_available, rsa_key):
    assert client.post("/db_connection_lib", json=_sf_body()).status_code == 200
    switched = client.put("/db_connection_lib", json=_sf_body(auth_method="key_pair", password=""))
    assert switched.status_code == 422
    assert switched.json()["detail"] == (
        "You changed how this connection signs in. Choose your private key file (.p8)."
    )
    assert _stored_row_and_secret()[1] == "pat-token-123", "a refused update changes nothing"

    switched = client.put("/db_connection_lib", json=_sf_body(auth_method="key_pair", password=_pem(rsa_key)))
    assert switched.status_code == 200, switched.text
    fields, secret = _stored_row_and_secret()
    assert fields[3] == "key_pair"
    assert secret == _stored_der(rsa_key)


def test_switching_another_dialect_to_snowflake_needs_a_secret(snowflake_available):
    assert (
        client.post("/db_connection_lib", json=_sf_body(database_type="postgresql", host="h", port=5432)).status_code
        == 200
    )
    response = client.put("/db_connection_lib", json=_sf_body(password=""))
    assert response.status_code == 422
    assert "Paste a Snowflake access token." in response.json()["detail"]


def test_snowflake_update_needs_the_driver(snowflake_available, monkeypatch):
    assert client.post("/db_connection_lib", json=_sf_body()).status_code == 200
    monkeypatch.setattr(SnowflakeDialect, "is_available", lambda self: False)
    response = client.put("/db_connection_lib", json=_sf_body(password=""))
    assert response.status_code == 422
    assert "flowfile[snowflake]" in response.json()["detail"]


def test_other_dialects_store_no_snowflake_fields():
    _cleanup_connection("plain_pg")
    body = {
        "connection_name": "plain_pg",
        "database_type": "postgresql",
        "username": "u",
        "password": "pw",
        "host": "db.internal",
        "port": 5432,
        "database": "d",
        "auth_method": "access_token",
        "role": "R",
        "warehouse": "W",
    }
    assert client.post("/db_connection_lib", json=body).status_code == 200
    with get_db_context() as db:
        row = get_database_connection(db, "plain_pg", 1)
        assert (row.host, row.port, row.auth_method, row.role, row.warehouse) == ("db.internal", 5432, None, None, None)
    listed = next(c for c in client.get("/db_connection_lib").json() if c["connection_name"] == "plain_pg")
    assert (listed["auth_method"], listed["role"], listed["warehouse"]) == (None, None, None)

    assert client.put("/db_connection_lib", json=body | {"password": ""}).status_code == 200
    with get_db_context() as db:
        row = get_database_connection(db, "plain_pg", 1)
        assert (row.auth_method, row.role, row.warehouse) == (None, None, None)
        secret = db.query(Secret).filter(Secret.id == row.password_id).one()
        assert decrypt_secret(secret.encrypted_value).get_secret_value() == "pw"
    _cleanup_connection("plain_pg")


def test_db_dialects_hides_browser_sign_in_on_a_multi_user_server(monkeypatch):
    from flowfile_core.routes.routes import get_db_dialects

    def snowflake_methods():
        return next(d for d in get_db_dialects() if d.name == "snowflake").auth_methods

    assert next(d for d in client.get("/db_dialects").json() if d["name"] == "snowflake")["auth_methods"] == [
        "oauth",
        "access_token",
        "key_pair",
    ]
    for mode in ("docker", "server"):
        monkeypatch.setenv("FLOWFILE_MODE", mode)
        assert snowflake_methods() == ["access_token", "key_pair"]
    monkeypatch.setenv("FLOWFILE_MODE", "package")
    assert snowflake_methods() == ["oauth", "access_token", "key_pair"]


# ---------- POST /db_connection_lib/test ----------


def test_connection_test_unknown_name_is_404():
    response = client.post("/db_connection_lib/test", json={"connection_name": "no_such_connection_xyz"})
    assert response.status_code == 404


def test_connection_test_sqlite_for_real(tmp_path):
    _cleanup_connection("test_sqlite_conn")
    db_path = tmp_path / "probe.db"
    sqlite3.connect(db_path).close()
    body = {"connection_name": "test_sqlite_conn", "database_type": "sqlite", "username": "", "password": ""}
    assert client.post("/db_connection_lib", json=body | {"database": str(db_path)}).status_code == 200
    response = client.post("/db_connection_lib/test", json={"connection_name": "test_sqlite_conn"})
    assert response.status_code == 200
    assert response.json() == {"success": True, "message": "Connection successful.", "details": None}

    db_path.unlink()
    response = client.post("/db_connection_lib/test", json={"connection_name": "test_sqlite_conn"})
    assert response.status_code == 200
    assert response.json()["success"] is False
    assert "File not found" in response.json()["message"]
    assert not db_path.exists(), "testing a missing file must not create it"
    _cleanup_connection("test_sqlite_conn")


def test_connection_test_duckdb_for_real(tmp_path):
    _cleanup_connection("test_duckdb_conn")
    db_path = _make_duckdb_file(tmp_path)
    body = {"connection_name": "test_duckdb_conn", "database_type": "duckdb", "username": "", "password": ""}
    assert client.post("/db_connection_lib", json=body | {"database": db_path}).status_code == 200
    response = client.post("/db_connection_lib/test", json={"connection_name": "test_duckdb_conn"})
    assert response.json() == {"success": True, "message": "Connection successful.", "details": None}
    _cleanup_connection("test_duckdb_conn")


def test_connection_test_unreachable_server_fails_with_the_password_masked():
    _cleanup_connection("test_dead_pg")
    body = {
        "connection_name": "test_dead_pg",
        "database_type": "postgresql",
        "username": "u",
        "password": "very-secret-pw",
        "host": "127.0.0.1",
        "port": 1,
        "database": "d",
    }
    assert client.post("/db_connection_lib", json=body).status_code == 200
    response = client.post("/db_connection_lib/test", json={"connection_name": "test_dead_pg"})
    assert response.status_code == 200
    result = response.json()
    assert result["success"] is False and result["message"]
    assert "very-secret-pw" not in result["message"]
    _cleanup_connection("test_dead_pg")


def test_connection_test_snowflake_describes_the_session(snowflake_available, monkeypatch):
    seen = []

    def session_info(self, uri):
        seen.append(uri)
        return {"user": "ME@EXAMPLE.COM", "role": "ANALYST", "warehouse": "COMPUTE_WH", "database": ""}

    monkeypatch.setattr(SnowflakeDialect, "session_info", session_info)
    assert client.post("/db_connection_lib", json=_sf_body()).status_code == 200
    response = client.post("/db_connection_lib/test", json={"connection_name": SF_NAME})
    assert response.status_code == 200
    assert response.json() == {
        "success": True,
        "message": "Connected as ME@EXAMPLE.COM, role ANALYST, warehouse COMPUTE_WH.",
        "details": {"user": "ME@EXAMPLE.COM", "role": "ANALYST", "warehouse": "COMPUTE_WH"},
    }
    (uri,) = seen
    assert uri == (
        "snowflake://ME%40EXAMPLE.COM:pat-token-123@myorg-myaccount.snowflakecomputing.com:443/ANALYTICS"
        "?auth=access_token&role=ANALYST&warehouse=COMPUTE_WH"
    )


def test_connection_test_snowflake_failure_is_a_200(snowflake_available, monkeypatch):
    from shared.db_dialects.snowflake import SnowflakeError

    def session_info(self, uri):
        raise SnowflakeError("No warehouse is selected. Snowflake said: pat-token-123 rejected")

    monkeypatch.setattr(SnowflakeDialect, "session_info", session_info)
    assert client.post("/db_connection_lib", json=_sf_body()).status_code == 200
    result = client.post("/db_connection_lib/test", json={"connection_name": SF_NAME}).json()
    assert result["success"] is False
    assert result["message"] == "No warehouse is selected. Snowflake said: *** rejected"

    monkeypatch.setattr(SnowflakeDialect, "is_available", lambda self: False)
    result = client.post("/db_connection_lib/test", json={"connection_name": SF_NAME}).json()
    assert result["success"] is False and "flowfile[snowflake]" in result["message"]


# ---------- Picking a database in a node ----------


def _sf_reference_settings(**overrides) -> dict:
    return {"connection_mode": "reference", "database_connection_name": SF_NAME, "query_mode": "table"} | overrides


def test_db_dialects_says_which_dialects_browse_databases():
    by_name = {d["name"]: d for d in client.get("/db_dialects").json()}
    assert by_name["snowflake"]["browses_databases"] is True
    assert all(d["browses_databases"] is False for name, d in by_name.items() if name != "snowflake")


def test_db_databases_and_database_name_for_a_single_database_dialect(tmp_path):
    db_path = str(tmp_path / "one.db")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE movies (id INTEGER)")
    conn.commit()
    conn.close()

    response = client.post("/db_databases", json=_browse_settings("sqlite", db_path))
    assert response.status_code == 200, response.text
    assert response.json() == []

    plain = client.post("/db_schemas", json=_browse_settings("sqlite", db_path))
    with_database = client.post("/db_schemas", json=_browse_settings("sqlite", db_path) | {"database_name": "OTHER"})
    assert plain.status_code == with_database.status_code == 200, with_database.text
    assert with_database.json() == plain.json()

    # SQLite reads the database name as a schema path: database "main" + schema "main" is not one, so it fails
    # at the database rather than reading the default database's table.
    settings = _browse_settings("sqlite", db_path) | {"database_name": "main", "schema_name": "main"}
    assert client.post("/db_tables", json=settings).status_code == 422


def test_db_databases_schemas_and_tables_honour_database_name(snowflake_available, monkeypatch):
    seen: list[tuple] = []

    def list_databases(self, uri):
        seen.append(("databases", uri))
        return ["ANALYTICS", "SALES"]

    def list_schemas(self, uri):
        seen.append(("schemas", uri))
        return ["RAW", "PUBLIC"]

    def list_tables(self, uri, schema_name):
        seen.append(("tables", uri, schema_name))
        return ["ORDERS"]

    monkeypatch.setattr(SnowflakeDialect, "list_databases", list_databases)
    monkeypatch.setattr(SnowflakeDialect, "list_schemas", list_schemas)
    monkeypatch.setattr(SnowflakeDialect, "list_tables", list_tables)
    assert client.post("/db_connection_lib", json=_sf_body()).status_code == 200

    response = client.post("/db_databases", json=_sf_reference_settings())
    assert response.status_code == 200, response.text
    assert response.json() == ["ANALYTICS", "SALES"]
    assert client.post("/db_schemas", json=_sf_reference_settings()).status_code == 200
    response = client.post("/db_schemas", json=_sf_reference_settings(database_name="SALES"))
    assert response.status_code == 200, response.text
    assert response.json() == ["PUBLIC", "RAW"]
    response = client.post("/db_tables", json=_sf_reference_settings(database_name="SALES", schema_name="PUBLIC"))
    assert response.status_code == 200, response.text
    assert response.json() == ["ORDERS"]
    assert client.post("/db_tables", json=_sf_reference_settings(schema_name="PUBLIC")).status_code == 200

    def uri(database: str) -> str:
        return (
            f"snowflake://ME%40EXAMPLE.COM:pat-token-123@myorg-myaccount.snowflakecomputing.com:443/{database}"
            "?auth=access_token&role=ANALYST&warehouse=COMPUTE_WH"
        )

    assert seen == [
        ("databases", uri("ANALYTICS")),
        ("schemas", uri("ANALYTICS")),
        ("schemas", uri("SALES")),
        ("tables", uri("SALES"), "SALES.PUBLIC"),
        ("tables", uri("ANALYTICS"), "PUBLIC"),
    ]


@pytest.mark.parametrize("database_name", ["SALES.PUBLIC", "a.b"])
def test_a_dotted_database_name_never_reaches_the_snowflake_uri(snowflake_available, monkeypatch, database_name):
    monkeypatch.setattr(SnowflakeDialect, "list_schemas", lambda self, uri: pytest.fail(f"listed {uri}"))
    assert client.post("/db_connection_lib", json=_sf_body()).status_code == 200
    response = client.post("/db_schemas", json=_sf_reference_settings(database_name=database_name))
    assert response.status_code == 422, response.text
    assert "Invalid SQL identifier" in str(response.json()["detail"])


@pytest.mark.parametrize("database_name", ["a/b", "x?y=1", "has space", '"open', "1ST", 'a"b'])
def test_a_database_name_that_is_no_identifier_is_refused_by_the_settings(snowflake_available, database_name):
    response = client.post("/db_schemas", json=_sf_reference_settings(database_name=database_name))
    assert response.status_code == 422, response.text


def test_db_dialects_says_which_dialects_take_quoted_names():
    by_name = {d["name"]: d for d in client.get("/db_dialects").json()}
    assert by_name["snowflake"]["quoted_identifiers"] is True
    assert all(d["quoted_identifiers"] is False for name, d in by_name.items() if name != "snowflake")


def test_quoted_snowflake_names_reach_the_uri_and_the_schema_path_as_given(snowflake_available, monkeypatch):
    seen: list[tuple] = []
    monkeypatch.setattr(
        SnowflakeDialect, "list_schemas", lambda self, uri: seen.append(("schemas", uri)) or ['"My Schema"']
    )
    monkeypatch.setattr(
        SnowflakeDialect, "list_tables", lambda self, uri, schema: seen.append(("tables", uri, schema)) or ["T"]
    )
    assert client.post("/db_connection_lib", json=_sf_body()).status_code == 200
    database = '"USER$evaneechoud"'
    response = client.post("/db_schemas", json=_sf_reference_settings(database_name=database))
    assert response.status_code == 200, response.text
    settings = _sf_reference_settings(database_name=database, schema_name='"My Schema"')
    response = client.post("/db_tables", json=settings)
    assert response.status_code == 200, response.text
    path = "/%22USER%24evaneechoud%22?"
    assert seen[0][0] == "schemas" and path in seen[0][1]
    assert seen[1][0] == "tables" and path in seen[1][1]
    assert seen[1][2] == '"USER$evaneechoud"."My Schema"'


@pytest.mark.parametrize("schema_name", ['"main"', "ma$in"])
def test_a_quoted_schema_is_refused_for_a_dialect_that_does_not_take_one(tmp_path, schema_name):
    db_path = str(tmp_path / "plain.db")
    sqlite3.connect(db_path).close()
    response = client.post("/db_tables", json=_browse_settings("sqlite", db_path) | {"schema_name": schema_name})
    assert response.status_code == 422, response.text
    assert response.json() == {
        "detail": f"Invalid SQL identifier: '{schema_name}'. Only letters, numbers, and underscores are allowed."
    }


def test_db_databases_failure_is_a_422_with_a_string_detail(snowflake_available, monkeypatch):
    from shared.db_dialects.snowflake import SnowflakeError

    def list_databases(self, uri):
        raise SnowflakeError("No warehouse is selected.")

    monkeypatch.setattr(SnowflakeDialect, "list_databases", list_databases)
    assert client.post("/db_connection_lib", json=_sf_body()).status_code == 200
    response = client.post("/db_databases", json=_sf_reference_settings())
    assert response.status_code == 422
    assert response.json() == {"detail": "No warehouse is selected."}
