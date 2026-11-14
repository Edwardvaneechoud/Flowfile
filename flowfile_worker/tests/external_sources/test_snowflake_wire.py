"""A Snowflake connection arrives as its URL without the secret plus the encrypted secret; the worker merges them."""

import socket

from flowfile_worker.external_sources.sql_source.main import verify_database_reachable
from flowfile_worker.external_sources.sql_source.models import DataBaseConnection, DatabaseReadSettings
from flowfile_worker.secrets import encrypt_secret

HOST = "myorg-myaccount.snowflakecomputing.com"
URL = f"snowflake://ME%40EXAMPLE.COM@{HOST}:443/ANALYTICS?auth=key_pair&role=ANALYST&warehouse=WH"


def _connection(secret: str) -> DataBaseConnection:
    return DataBaseConnection(
        database_type="snowflake",
        username="ME@EXAMPLE.COM",
        host=HOST,
        database="ANALYTICS",
        url=URL,
        password=encrypt_secret(secret, 1),
    )


def test_create_uri_merges_the_decrypted_secret_into_the_url():
    uri = _connection("MIIEv/key+material==").create_uri()
    assert uri == (
        f"snowflake://ME%40EXAMPLE.COM:MIIEv%2Fkey%2Bmaterial%3D%3D@{HOST}:443/ANALYTICS"
        "?auth=key_pair&role=ANALYST&warehouse=WH"
    )


def test_core_payload_shape_is_accepted_by_the_worker_contract():
    body = {
        "connection": {
            "database_type": "snowflake",
            "username": "ME@EXAMPLE.COM",
            "host": HOST,
            "port": None,
            "database": "ANALYTICS",
            "url": URL,
            "password": encrypt_secret("tok", 1),
            "ssl_enabled": False,
        },
        "query": "SELECT * FROM T",
    }
    settings = DatabaseReadSettings.model_validate(body)
    assert settings.connection.create_uri().startswith(f"snowflake://ME%40EXAMPLE.COM:tok@{HOST}:443/")


def test_no_tcp_precheck_for_a_snowflake_url(monkeypatch):
    calls = []
    monkeypatch.setattr(socket, "create_connection", lambda *a, **k: calls.append(a))
    verify_database_reachable(_connection("tok"))
    assert calls == []
