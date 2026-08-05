"""Pins verify_database_reachable's dialect-driven behavior: registry default ports,
legacy alias fallback via _DEFAULT_DB_PORTS, and the file-based skip."""

import socket

import pytest

from flowfile_worker.external_sources.sql_source.main import verify_database_reachable
from flowfile_worker.external_sources.sql_source.models import DataBaseConnection


@pytest.fixture
def captured_connections(monkeypatch):
    calls = []

    class _Sock:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def fake_create_connection(address, timeout=None):
        calls.append(address)
        return _Sock()

    monkeypatch.setattr(socket, "create_connection", fake_create_connection)
    return calls


def test_preflight_uses_registry_default_port(captured_connections):
    verify_database_reachable(DataBaseConnection(database_type="postgresql", host="db.example"))
    assert captured_connections == [("db.example", 5432)]


def test_preflight_mssql_uses_registry_default_port(captured_connections):
    verify_database_reachable(DataBaseConnection(database_type="mssql", host="db.example"))
    assert captured_connections == [("db.example", 1433)]


def test_preflight_explicit_port_wins(captured_connections):
    verify_database_reachable(DataBaseConnection(database_type="mysql", host="db.example", port=13306))
    assert captured_connections == [("db.example", 13306)]


def test_preflight_alias_types_fall_back_to_port_map(captured_connections):
    verify_database_reachable(DataBaseConnection(database_type="mariadb", host="db.example"))
    assert captured_connections == [("db.example", 3306)]


def test_preflight_skips_file_based_dialects(captured_connections):
    verify_database_reachable(DataBaseConnection(database_type="sqlite", database="/tmp/x.db"))
    verify_database_reachable(DataBaseConnection(database_type="duckdb", database="/tmp/x.duckdb"))
    assert captured_connections == []


def test_preflight_skips_url_connections(captured_connections):
    verify_database_reachable(DataBaseConnection(database_type="postgresql", url="postgresql://u@h/d"))
    assert captured_connections == []


def test_preflight_skips_hostless_snowflake_connections(captured_connections):
    """Snowflake's locator is the account in extra_params, so host is None and the
    TCP pre-flight has nothing meaningful to probe."""
    verify_database_reachable(
        DataBaseConnection(database_type="snowflake", extra_params={"account": "myorg-myaccount"})
    )
    assert captured_connections == []


def test_snowflake_create_uri_carries_extra_params():
    connection = DataBaseConnection(
        database_type="snowflake",
        username="u",
        database="ANALYTICS",
        extra_params={"account": "myorg-myaccount", "warehouse": "COMPUTE_WH"},
    )
    assert connection.create_uri() == "snowflake://u@myorg-myaccount/ANALYTICS?warehouse=COMPUTE_WH"
