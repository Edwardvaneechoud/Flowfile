"""Snowflake connections at run time: the worker payload, the core URI and the inline refusal."""

import pytest
from pydantic import SecretStr

from flowfile_core.flowfile.flow_graph.builders.connectors import _resolve_database_credentials
from flowfile_core.flowfile.sources.external_sources.sql_source.models import (
    DatabaseExternalReadSettings,
    DatabaseExternalWriteSettings,
)
from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import _resolve_connection, connection_uri
from flowfile_core.flowfile.sources.external_sources.sql_source.utils import (
    SNOWFLAKE_INLINE_REFUSAL,
    dialect_uri_options,
)
from flowfile_core.schemas import input_schema
from flowfile_core.secret_manager.secret_manager import encrypt_secret

HOST = "myorg-myaccount.snowflakecomputing.com"
SECRET_FREE_URL = f"snowflake://ME%40EXAMPLE.COM@{HOST}:443/ANALYTICS?auth=key_pair&role=ANALYST&warehouse=WH"


def _snowflake_reference(password: str) -> input_schema.FullDatabaseConnection:
    return input_schema.FullDatabaseConnection(
        connection_name="sf",
        database_type="snowflake",
        username="ME@EXAMPLE.COM",
        password=SecretStr(password),
        host=HOST,
        database="ANALYTICS",
        auth_method="key_pair",
        role="ANALYST",
        warehouse="WH",
    )


def _postgres_reference(password: str) -> input_schema.FullDatabaseConnection:
    return input_schema.FullDatabaseConnection(
        connection_name="pg",
        database_type="postgresql",
        username="u",
        password=SecretStr(password),
        host="db.internal",
        port=5432,
        database="d",
    )


def _reader(mode: str = "reference", inline: dict | None = None) -> input_schema.NodeDatabaseReader:
    settings = {"connection_mode": mode, "table_name": "T", "query_mode": "table"}
    if mode == "reference":
        settings["database_connection_name"] = "sf"
    else:
        settings["database_connection"] = inline
    return input_schema.NodeDatabaseReader(flow_id=1, node_id=2, database_settings=settings)


def _writer(mode: str = "reference", inline: dict | None = None) -> input_schema.NodeDatabaseWriter:
    settings = {"connection_mode": mode, "table_name": "T"}
    if mode == "reference":
        settings["database_connection_name"] = "sf"
    else:
        settings["database_connection"] = inline
    return input_schema.NodeDatabaseWriter(flow_id=1, node_id=3, depending_on_id=2, database_write_settings=settings)


def test_dialect_uri_options():
    assert dialect_uri_options(_snowflake_reference("x")) == {"auth": "key_pair", "role": "ANALYST", "warehouse": "WH"}
    partial = _snowflake_reference("x").model_copy(update={"role": None, "warehouse": None, "auth_method": "oauth"})
    assert dialect_uri_options(partial) == {"auth": "oauth"}
    assert dialect_uri_options(_postgres_reference("x")) == {}
    assert dialect_uri_options(input_schema.DatabaseConnection(database_type="postgresql")) == {}


def test_reader_payload_carries_the_secret_free_url_and_the_ciphertext():
    ciphertext = encrypt_secret("the-private-key", 1)
    settings = DatabaseExternalReadSettings.create_from_from_node_database_reader(
        _reader(),
        password=ciphertext,
        query="SELECT * FROM T",
        database_reference_settings=_snowflake_reference(ciphertext),
    )
    assert settings.connection.url == SECRET_FREE_URL
    assert settings.connection.password == ciphertext
    dumped = settings.model_dump(mode="json")["connection"]
    assert "the-private-key" not in str(dumped)
    assert set(dumped) == {"database_type", "username", "host", "port", "database", "url", "password", "ssl_enabled"}


def test_writer_payload_carries_the_secret_free_url():
    import polars as pl

    ciphertext = encrypt_secret("token", 1)
    settings = DatabaseExternalWriteSettings.create_from_from_node_database_writer(
        _writer(),
        password=ciphertext,
        table_name="T",
        lf=pl.LazyFrame({"a": [1]}),
        database_reference_settings=_snowflake_reference(ciphertext),
    )
    assert settings.connection.url == SECRET_FREE_URL
    assert settings.connection.password == ciphertext


def test_other_dialect_payloads_are_unchanged():
    ciphertext = encrypt_secret("pw", 1)
    settings = DatabaseExternalReadSettings.create_from_from_node_database_reader(
        _reader(), password=ciphertext, query="SELECT 1", database_reference_settings=_postgres_reference(ciphertext)
    )
    assert settings.connection.model_dump(mode="json") == {
        "database_type": "postgresql",
        "username": "u",
        "host": "db.internal",
        "port": 5432,
        "database": "d",
        "url": None,
        "password": ciphertext,
        "ssl_enabled": False,
    }


_INLINE_SNOWFLAKE = {"database_type": "snowflake", "username": "u", "host": HOST, "password_ref": "s"}


def test_inline_snowflake_is_refused_everywhere():
    with pytest.raises(ValueError, match="must be saved connections"):
        DatabaseExternalReadSettings.create_from_from_node_database_reader(
            _reader("inline", _INLINE_SNOWFLAKE), password="x", query="SELECT 1"
        )
    with pytest.raises(ValueError, match="must be saved connections"):
        DatabaseExternalWriteSettings.create_from_from_node_database_writer(
            _writer("inline", _INLINE_SNOWFLAKE), password="x", table_name="T", lf=None
        )
    settings = _reader("inline", _INLINE_SNOWFLAKE).database_settings
    with pytest.raises(ValueError) as caught:
        _resolve_connection(settings, 1)
    assert str(caught.value) == SNOWFLAKE_INLINE_REFUSAL
    with pytest.raises(ValueError, match="must be saved connections"):
        _resolve_database_credentials(settings, 1)


def test_a_saved_flow_with_an_inline_snowflake_node_still_opens():
    node = _reader("inline", _INLINE_SNOWFLAKE)
    assert node.database_settings.database_connection.database_type == "snowflake"


def test_core_uri_carries_the_options_and_the_secret():
    uri = connection_uri(_snowflake_reference("ignored"), "the/secret+key")
    assert uri == (
        f"snowflake://ME%40EXAMPLE.COM:the%2Fsecret%2Bkey@{HOST}:443/ANALYTICS?auth=key_pair&role=ANALYST&warehouse=WH"
    )
    assert connection_uri(_postgres_reference("x"), "pw").startswith("postgresql://u:pw@db.internal:5432/d")
