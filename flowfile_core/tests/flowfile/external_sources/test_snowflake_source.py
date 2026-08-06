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
    get_all_database_connections_interface,
    get_database_connection,
    get_database_connection_schema,
    parse_extra_params,
    store_database_connection,
    update_database_connection,
)
from flowfile_core.routes._connection_sharing import require_credentials_on_target_change
from flowfile_core.schemas.input_schema import (
    DatabaseConnection,
    FullDatabaseConnection,
    FullDatabaseConnectionInterface,
)
from flowfile_core.secret_manager.secret_manager import decrypt_secret
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


def _make_pem() -> str:
    """A fresh throwaway RSA key as unencrypted PKCS#8 PEM text."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("ascii")


def _key_pair_connection(
    name: str, private_key: str | None, passphrase: str | None = None
) -> FullDatabaseConnection:
    return FullDatabaseConnection(
        connection_name=name,
        database_type="snowflake",
        username="svc_user",
        password=SecretStr(""),
        database="ANALYTICS",
        extra_params={"account": "myorg-myaccount"},
        auth_method="key_pair",
        private_key=SecretStr(private_key) if private_key is not None else None,
        private_key_passphrase=SecretStr(passphrase) if passphrase is not None else None,
    )


class TestKeyPairCrud:
    def test_store_and_reload_round_trips_ciphertexts(self):
        name = "snowflake_kp_store"
        pem = _make_pem()
        _cleanup(name)
        try:
            with get_db_context() as db:
                store_database_connection(db, _key_pair_connection(name, pem, "pass-phrase"), USER_ID)
            with get_db_context() as db:
                reloaded = get_database_connection_schema(db, name, USER_ID)
            assert reloaded is not None
            assert reloaded.auth_method == "key_pair"
            key_ciphertext = reloaded.private_key.get_secret_value()
            assert key_ciphertext.startswith("$ffsec$"), "schema must return ciphertext, never the PEM"
            assert pem not in key_ciphertext
            assert decrypt_secret(key_ciphertext).get_secret_value() == pem
            passphrase_ciphertext = reloaded.private_key_passphrase.get_secret_value()
            assert passphrase_ciphertext.startswith("$ffsec$")
            assert decrypt_secret(passphrase_ciphertext).get_secret_value() == "pass-phrase"
        finally:
            _cleanup(name)

    def test_create_requires_private_key(self):
        _cleanup("snowflake_kp_missing")
        with get_db_context() as db:
            with pytest.raises(ValueError, match="requires a private key"):
                store_database_connection(db, _key_pair_connection("snowflake_kp_missing", None), USER_ID)

    def test_update_rotates_key_and_empty_keeps_existing(self):
        name = "snowflake_kp_rotate"
        pem = _make_pem()
        _cleanup(name)
        try:
            with get_db_context() as db:
                store_database_connection(db, _key_pair_connection(name, pem), USER_ID)

            with get_db_context() as db:
                update_database_connection(db, _key_pair_connection(name, None), USER_ID)
            with get_db_context() as db:
                kept = get_database_connection_schema(db, name, USER_ID).private_key.get_secret_value()
            assert decrypt_secret(kept).get_secret_value() == pem, "empty key on update must keep the existing secret"

            new_pem = _make_pem()
            with get_db_context() as db:
                update_database_connection(db, _key_pair_connection(name, new_pem), USER_ID)
            with get_db_context() as db:
                rotated = get_database_connection_schema(db, name, USER_ID).private_key.get_secret_value()
            assert decrypt_secret(rotated).get_secret_value() == new_pem
        finally:
            _cleanup(name)

    def test_delete_removes_all_connection_secrets(self):
        from flowfile_core.database.models import Secret

        name = "snowflake_kp_delete"
        _cleanup(name)
        with get_db_context() as db:
            store_database_connection(db, _key_pair_connection(name, _make_pem(), "pp"), USER_ID)
        with get_db_context() as db:
            row = get_database_connection(db, name, USER_ID)
            secret_ids = [row.password_id, row.private_key_id, row.private_key_passphrase_id]
        assert all(secret_id is not None for secret_id in secret_ids)
        with get_db_context() as db:
            delete_database_connection(db, name, USER_ID)
        with get_db_context() as db:
            assert db.query(Secret).filter(Secret.id.in_(secret_ids)).count() == 0

    def test_switching_to_password_detaches_and_deletes_key_secrets(self):
        from flowfile_core.database.models import Secret

        name = "snowflake_kp_flip"
        _cleanup(name)
        try:
            with get_db_context() as db:
                store_database_connection(db, _key_pair_connection(name, _make_pem(), "pp"), USER_ID)
            with get_db_context() as db:
                row = get_database_connection(db, name, USER_ID)
                key_secret_ids = [row.private_key_id, row.private_key_passphrase_id]
            assert all(secret_id is not None for secret_id in key_secret_ids)

            flipped = FullDatabaseConnection(
                connection_name=name,
                database_type="snowflake",
                username="svc_user",
                password=SecretStr("new-pass"),
                database="ANALYTICS",
                extra_params={"account": "myorg-myaccount"},
                auth_method="password",
            )
            with get_db_context() as db:
                update_database_connection(db, flipped, USER_ID)
            with get_db_context() as db:
                row = get_database_connection(db, name, USER_ID)
                assert row.private_key_id is None, "a rotated-away key must not linger on the row"
                assert row.private_key_passphrase_id is None
                assert db.query(Secret).filter(Secret.id.in_(key_secret_ids)).count() == 0
                reloaded = get_database_connection_schema(db, name, USER_ID)
            assert reloaded.private_key is None, "resolvers must no longer see any key material"
        finally:
            _cleanup(name)

    def test_switching_to_password_drops_a_stray_incoming_key(self):
        """auth_method="password" with a non-empty private_key in the same payload (e.g. a
        client that didn't clear the hidden field): the stray key must be ignored AND the
        existing key secrets deleted — build_uri infers key-pair from key presence, so a
        persisted stray key would silently out-vote the password."""
        from flowfile_core.database.models import Secret

        name = "snowflake_kp_stray"
        _cleanup(name)
        try:
            with get_db_context() as db:
                store_database_connection(db, _key_pair_connection(name, _make_pem(), "pp"), USER_ID)
            with get_db_context() as db:
                row = get_database_connection(db, name, USER_ID)
                old_key_ids = [row.private_key_id, row.private_key_passphrase_id]

            flipped = FullDatabaseConnection(
                connection_name=name,
                database_type="snowflake",
                username="svc_user",
                password=SecretStr("new-pass"),
                database="ANALYTICS",
                extra_params={"account": "myorg-myaccount"},
                auth_method="password",
                private_key=SecretStr(_make_pem()),
                private_key_passphrase=SecretStr("stray-pass"),
            )
            with get_db_context() as db:
                update_database_connection(db, flipped, USER_ID)
            with get_db_context() as db:
                row = get_database_connection(db, name, USER_ID)
                assert row.private_key_id is None
                assert row.private_key_passphrase_id is None
                assert db.query(Secret).filter(Secret.id.in_(old_key_ids)).count() == 0
                reloaded = get_database_connection_schema(db, name, USER_ID)
            assert reloaded.private_key is None, "a stray incoming key must never be persisted"
            assert reloaded.private_key_passphrase is None
        finally:
            _cleanup(name)

    def test_update_to_key_pair_without_key_is_rejected(self):
        name = "snowflake_kp_bad_flip"
        _cleanup(name)
        try:
            with get_db_context() as db:
                store_database_connection(db, _snowflake_connection(name), USER_ID)
            with get_db_context() as db:
                with pytest.raises(ValueError, match="requires a private key"):
                    update_database_connection(db, _key_pair_connection(name, None), USER_ID)
        finally:
            _cleanup(name)

    def test_interface_reports_auth_method_without_key_material(self):
        name = "snowflake_kp_interface"
        pem = _make_pem()
        _cleanup(name)
        try:
            with get_db_context() as db:
                store_database_connection(db, _key_pair_connection(name, pem), USER_ID)
            with get_db_context() as db:
                interfaces = get_all_database_connections_interface(db, USER_ID)
            entry = next(i for i in interfaces if i.connection_name == name)
            assert entry.auth_method == "key_pair"
            assert pem not in str(entry.model_dump())
        finally:
            _cleanup(name)


class TestKeyPairModelValidation:
    def test_key_pair_rejected_for_non_supporting_dialect(self):
        with pytest.raises(ValidationError, match="not supported by database type"):
            FullDatabaseConnection(
                connection_name="pg_kp",
                database_type="postgresql",
                username="u",
                password=SecretStr(""),
                auth_method="key_pair",
                private_key=SecretStr("-----BEGIN PRIVATE KEY-----"),
            )

    def test_inline_key_pair_requires_private_key_ref(self):
        with pytest.raises(ValidationError, match="requires a private key"):
            DatabaseConnection(database_type="snowflake", username="u", auth_method="key_pair")
        conn = DatabaseConnection(
            database_type="snowflake", username="u", auth_method="key_pair", private_key_ref="kp_secret"
        )
        assert conn.private_key_ref == "kp_secret"

    def test_raw_empty_key_strings_normalize_to_none(self):
        # JSON clients send "" for the blank form fields; that must not create key secrets.
        conn = FullDatabaseConnection(
            connection_name="pw_conn",
            database_type="postgresql",
            username="u",
            password=SecretStr("pw"),
            private_key="",
            private_key_passphrase="",
        )
        assert conn.private_key is None
        assert conn.private_key_passphrase is None
        # The importer's explicit SecretStr("") placeholder rows pass through untouched.
        refill = FullDatabaseConnection(
            connection_name="kp_refill",
            database_type="snowflake",
            username="u",
            password=SecretStr(""),
            extra_params={"account": "acct"},
            auth_method="key_pair",
            private_key=SecretStr(""),
        )
        assert refill.private_key is not None

    def test_inline_empty_strings_normalize_to_none(self):
        conn = DatabaseConnection(
            database_type="snowflake", username="u", auth_method="", private_key_ref="", private_key_passphrase_ref=""
        )
        assert conn.auth_method is None
        assert conn.private_key_ref is None
        assert conn.private_key_passphrase_ref is None

    def test_interface_model_never_exposes_key_material(self):
        assert "private_key" not in FullDatabaseConnectionInterface.model_fields
        assert "private_key_passphrase" not in FullDatabaseConnectionInterface.model_fields
        assert "auth_method" in FullDatabaseConnectionInterface.model_fields


class TestAuthMethodRepointGuard:
    """The PUT route appends auth_method via a normalized comparison (stored NULL == "password")."""

    @staticmethod
    def _changed(row_value: str | None, incoming: str | None) -> bool:
        return (row_value or "password") != (incoming or "password")

    def test_null_and_password_are_equivalent(self):
        assert self._changed(None, "password") is False
        assert self._changed("password", None) is False
        assert self._changed(None, None) is False
        assert self._changed("key_pair", "key_pair") is False

    def test_auth_method_flip_trips_the_guard(self):
        from fastapi import HTTPException

        assert self._changed(None, "key_pair") is True
        assert self._changed("key_pair", "password") is True
        with pytest.raises(HTTPException) as exc:
            require_credentials_on_target_change(["auth_method"], has_new_credentials=False, has_bundled_secrets=True)
        assert exc.value.status_code == 422
        require_credentials_on_target_change(["auth_method"], has_new_credentials=True, has_bundled_secrets=True)


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


_KEY_PAIR_LIVE_ENV = ("ACCOUNT", "USER", "DATABASE", "WAREHOUSE")


def _live_key_pair_configured() -> bool:
    return bool(os.environ.get("FLOWFILE_TEST_SNOWFLAKE_PRIVATE_KEY_PATH")) and all(
        os.environ.get(f"FLOWFILE_TEST_SNOWFLAKE_{name}") for name in _KEY_PAIR_LIVE_ENV
    )


snowflake_key_pair_available = pytest.mark.skipif(
    not _live_key_pair_configured(),
    reason="FLOWFILE_TEST_SNOWFLAKE_PRIVATE_KEY_PATH and account credentials are not configured",
)


@snowflake_key_pair_available
class TestSnowflakeKeyPairLive:
    def test_key_pair_read_round_trip(self):
        dialect = get_dialect("snowflake")
        with open(os.environ["FLOWFILE_TEST_SNOWFLAKE_PRIVATE_KEY_PATH"], encoding="utf-8") as f:
            pem = f.read()
        extra = {
            "account": os.environ["FLOWFILE_TEST_SNOWFLAKE_ACCOUNT"],
            "warehouse": os.environ["FLOWFILE_TEST_SNOWFLAKE_WAREHOUSE"],
        }
        if os.environ.get("FLOWFILE_TEST_SNOWFLAKE_ROLE"):
            extra["role"] = os.environ["FLOWFILE_TEST_SNOWFLAKE_ROLE"]
        if os.environ.get("FLOWFILE_TEST_SNOWFLAKE_SCHEMA"):
            extra["schema"] = os.environ["FLOWFILE_TEST_SNOWFLAKE_SCHEMA"]
        uri = dialect.build_uri(
            username=os.environ["FLOWFILE_TEST_SNOWFLAKE_USER"],
            database=os.environ["FLOWFILE_TEST_SNOWFLAKE_DATABASE"],
            auth_method="key_pair",
            private_key=pem,
            private_key_passphrase=os.environ.get("FLOWFILE_TEST_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
            **extra,
        )
        result = dialect.read("SELECT 1 AS one", uri, logger)
        assert result.height == 1
        assert result.columns == ["ONE"]
