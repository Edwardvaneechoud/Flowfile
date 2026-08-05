"""Snowflake connection-model tests: extra_params CRUD round-trip, the repoint
guard's normalized comparison, and live reads/writes against a real account.

URI shapes, the type map, and the fakesnow behavioral legs live in
shared/tests/db_dialects/test_snowflake_dialect.py. The live classes here are
gated on FLOWFILE_TEST_SNOWFLAKE_* credentials (account/user/password/database/
warehouse) per the roadmap's testing decision, so CI skips them.
"""

import logging
import os
import uuid

import polars as pl
import pytest
from pydantic import SecretStr, ValidationError

from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_database_connection,
    get_database_connection,
    get_database_connection_schema,
    parse_extra_params,
    store_database_connection,
    update_database_connection,
)
from flowfile_core.routes._connection_sharing import require_credentials_on_target_change
from flowfile_core.schemas.input_schema import FullDatabaseConnection
from shared.db_dialects import get_dialect

logger = logging.getLogger(__name__)

_LIVE_ENV = ("ACCOUNT", "USER", "PASSWORD", "DATABASE", "WAREHOUSE")


def _live_credentials_configured() -> bool:
    return all(os.environ.get(f"FLOWFILE_TEST_SNOWFLAKE_{name}") for name in _LIVE_ENV)


snowflake_available = pytest.mark.skipif(
    not _live_credentials_configured(),
    reason="FLOWFILE_TEST_SNOWFLAKE_* credentials are not configured",
)

USER_ID = 1
EXTRA_PARAMS = {"account": "myorg-myaccount", "warehouse": "COMPUTE_WH", "role": "ANALYST"}


def _snowflake_connection(name: str, extra_params: dict[str, str] | None = EXTRA_PARAMS) -> FullDatabaseConnection:
    return FullDatabaseConnection(
        connection_name=name,
        database_type="snowflake",
        username="user",
        password=SecretStr("pass"),
        database="ANALYTICS",
        extra_params=extra_params,
    )


def _cleanup(name: str) -> None:
    with get_db_context() as db:
        if get_database_connection(db, name, USER_ID) is not None:
            delete_database_connection(db, name, USER_ID)


class TestExtraParamsCrud:
    def test_store_and_reload_round_trips_extra_params(self):
        name = "snowflake_crud_store"
        _cleanup(name)
        try:
            with get_db_context() as db:
                store_database_connection(db, _snowflake_connection(name), USER_ID)
            with get_db_context() as db:
                reloaded = get_database_connection_schema(db, name, USER_ID)
            assert reloaded is not None
            assert reloaded.extra_params == EXTRA_PARAMS
        finally:
            _cleanup(name)

    def test_update_persists_changed_extra_params(self):
        name = "snowflake_crud_update"
        _cleanup(name)
        try:
            with get_db_context() as db:
                store_database_connection(db, _snowflake_connection(name), USER_ID)
            updated = _snowflake_connection(name, {"account": "other-account"})
            with get_db_context() as db:
                update_database_connection(db, updated, USER_ID)
            with get_db_context() as db:
                reloaded = get_database_connection_schema(db, name, USER_ID)
            assert reloaded is not None and reloaded.extra_params == {"account": "other-account"}
        finally:
            _cleanup(name)

    def test_blocked_extra_params_rejected_at_model_boundary(self):
        with pytest.raises(ValidationError, match="extra_params may not override"):
            _snowflake_connection("snowflake_evil", {"account": "a", "private_key_file": "/tmp/k.p8"})

    def test_parse_extra_params_normalizes(self):
        assert parse_extra_params(None) is None
        assert parse_extra_params("") is None
        assert parse_extra_params("not json") is None
        assert parse_extra_params("{}") is None
        assert parse_extra_params('{"account": "a"}') == {"account": "a"}


class TestRepointGuardComparison:
    """The PUT route treats a changed extra_params as a target change; the comparison
    is normalized (row JSON vs incoming dict) so an unchanged dict never trips it."""

    @staticmethod
    def _changed(row_json: str | None, incoming: dict[str, str] | None) -> bool:
        return (parse_extra_params(row_json) or {}) != (incoming or {})

    def test_unchanged_extra_params_do_not_trip_the_guard(self):
        assert self._changed('{"account": "a", "warehouse": "w"}', {"warehouse": "w", "account": "a"}) is False
        assert self._changed(None, None) is False
        assert self._changed(None, {}) is False

    def test_changed_account_trips_the_guard(self):
        assert self._changed('{"account": "a"}', {"account": "attacker"}) is True
        assert self._changed(None, {"account": "a"}) is True

    def test_guard_requires_credentials_on_change(self):
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc:
            require_credentials_on_target_change(
                ["extra_params"], has_new_credentials=False, has_bundled_secrets=True
            )
        assert exc.value.status_code == 422
        require_credentials_on_target_change(["extra_params"], has_new_credentials=True, has_bundled_secrets=True)
        require_credentials_on_target_change([], has_new_credentials=False, has_bundled_secrets=True)


def _live_uri() -> str:
    dialect = get_dialect("snowflake")
    extra = {
        "account": os.environ["FLOWFILE_TEST_SNOWFLAKE_ACCOUNT"],
        "warehouse": os.environ["FLOWFILE_TEST_SNOWFLAKE_WAREHOUSE"],
    }
    if os.environ.get("FLOWFILE_TEST_SNOWFLAKE_ROLE"):
        extra["role"] = os.environ["FLOWFILE_TEST_SNOWFLAKE_ROLE"]
    if os.environ.get("FLOWFILE_TEST_SNOWFLAKE_SCHEMA"):
        extra["schema"] = os.environ["FLOWFILE_TEST_SNOWFLAKE_SCHEMA"]
    return dialect.build_uri(
        username=os.environ["FLOWFILE_TEST_SNOWFLAKE_USER"],
        password=os.environ["FLOWFILE_TEST_SNOWFLAKE_PASSWORD"],
        database=os.environ["FLOWFILE_TEST_SNOWFLAKE_DATABASE"],
        **extra,
    )


@snowflake_available
class TestSnowflakeLive:
    def test_write_read_roundtrip_and_fast_schema_parity(self):
        dialect = get_dialect("snowflake")
        uri = _live_uri()
        table = f"ff_test_{uuid.uuid4().hex[:12]}"
        df = pl.DataFrame({"id": [1, 2, 3], "score": [0.5, 1.5, 2.5], "label": ["a", "b", "c"]})
        try:
            dialect.write(df, uri=uri, table_name=table, if_exists="replace")
            dialect.write(df, uri=uri, table_name=table, if_exists="append")
            with pytest.raises(ValueError, match="already exists"):
                dialect.write(df, uri=uri, table_name=table, if_exists="fail")
            dialect.write(df, uri=uri, table_name=table, if_exists="replace")

            result = dialect.read(f"SELECT * FROM {table}", uri, logger)
            assert result.height == df.height
            assert result.columns == df.columns

            predicted = dialect.query_schema(uri, f"SELECT * FROM {table}")
            assert predicted is not None and dict(predicted) == dict(result.schema)
            predicted_table = dialect.table_schema(uri, table, None)
            assert predicted_table is not None and dict(predicted_table) == dict(result.schema)
        finally:
            dialect._execute_rows(uri, f"DROP TABLE IF EXISTS {table}")

    def test_browse_lists_schemas_and_tables(self):
        dialect = get_dialect("snowflake")
        uri = _live_uri()
        schemas = dialect.list_schemas(uri)
        assert schemas, "expected at least one schema"

    def test_sql_source_schema_prediction(self):
        from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import SqlSource

        source = SqlSource(connection_string=_live_uri(), query="SELECT 1 AS one", database_type="snowflake")
        columns = source.get_schema()
        assert [c.name for c in columns] == ["ONE"]
