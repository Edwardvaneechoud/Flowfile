"""HTTP-surface tests for the dialect catalog, the vocabulary gates, and the browse routes."""

import sqlite3

from fastapi.testclient import TestClient
from pydantic import SecretStr

from flowfile_core import main
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_database_connection,
    get_database_connection,
    store_database_connection,
)
from flowfile_core.schemas import input_schema
from shared.db_dialects import KNOWN_DIALECT_NAMES


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
    assert all(d["available"] is True for d in dialects)


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
