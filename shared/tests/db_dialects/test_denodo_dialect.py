"""Denodo dialect unit tests: metadata, URI/driver wiring, psycopg2 OID→Polars map.

The generic contract legs run for denodo via test_dialect_contract.py. Live
coverage of the psycopg2 read / LIMIT-0 fast-schema paths runs against the
Postgres Docker fixture (the same wire protocol Denodo's port 9996 speaks) in
flowfile_core/tests/flowfile/external_sources/test_denodo_source.py.
"""

from collections import namedtuple

import polars as pl
import pytest

from shared.db_dialects import POSTGRES_FAMILY, get_dialect
from shared.db_dialects.denodo import _polars_dtype
from shared.sql_utils import construct_sql_uri, get_sqlalchemy_uri

dialect = get_dialect("denodo")

_Column = namedtuple("_Column", "name type_code display_size internal_size precision scale null_ok")


def _col(type_code: int, precision=None, scale=None) -> _Column:
    return _Column("c", type_code, None, None, precision, scale, None)


def test_metadata():
    assert dialect.display_name == "Denodo"
    assert dialect.file_based is False
    assert dialect.default_port == 9996
    assert dialect.supports_ssl is True
    assert dialect.sqlalchemy_driver == "postgresql+psycopg2"
    assert dialect.sqlglot_name == "postgres"
    assert dialect.install_hint == "pip install psycopg2-binary"
    assert dialect.is_available() is True, "psycopg2-binary is a main dependency; the dialect must be available"


def test_denodo_is_postgres_family_so_ssl_and_timeout_params_apply():
    assert "denodo" in POSTGRES_FAMILY
    uri = construct_sql_uri(
        database_type="denodo", host="vdp", port=9996, username="u", password="p", database="db",
        ssl_enabled=True, connect_timeout=5,
    )
    assert uri == "denodo://u:p@vdp:9996/db?sslmode=require&connect_timeout=5"


def test_sqlalchemy_uri_uses_psycopg2_driver():
    assert dialect.sqlalchemy_uri("denodo://u@h/d") == "postgresql+psycopg2://u@h/d"
    assert get_sqlalchemy_uri("denodo://u:p@h:9996/d") == "postgresql+psycopg2://u:p@h:9996/d"


def test_libpq_dsn_rewrites_scheme_only():
    assert dialect.libpq_dsn("denodo://u:p@h:9996/d?sslmode=require") == "postgresql://u:p@h:9996/d?sslmode=require"
    assert dialect.libpq_dsn("postgresql://u@h/d") == "postgresql://u@h/d"


def test_limit_query_is_plain_limit():
    assert dialect.limit_query("SELECT a FROM v", 5) == "SELECT a FROM v LIMIT 5"


def test_polars_dtype_maps_common_oids():
    assert _polars_dtype(_col(16)) == pl.Boolean
    assert _polars_dtype(_col(21)) == pl.Int16
    assert _polars_dtype(_col(23)) == pl.Int32
    assert _polars_dtype(_col(20)) == pl.Int64
    assert _polars_dtype(_col(700)) == pl.Float32
    assert _polars_dtype(_col(701)) == pl.Float64
    assert _polars_dtype(_col(25)) == pl.String
    assert _polars_dtype(_col(1043)) == pl.String
    assert _polars_dtype(_col(2950)) == pl.String, "psycopg2 yields uuid as str"
    assert _polars_dtype(_col(1082)) == pl.Date
    assert _polars_dtype(_col(1083)) == pl.Time
    assert _polars_dtype(_col(1114)) == pl.Datetime("us")
    assert _polars_dtype(_col(1184)) == pl.Datetime("us", "UTC")
    assert _polars_dtype(_col(17)) == pl.Binary


def test_polars_dtype_numeric_needs_a_sane_typmod():
    assert _polars_dtype(_col(1700, 10, 2)) == pl.Decimal(10, 2)
    assert _polars_dtype(_col(1700, None, None)) is None
    assert _polars_dtype(_col(1700, 65535, 65531)) is None, "unconstrained numeric surfaces as typmod -1 garbage"
    assert _polars_dtype(_col(1700, 40, 2)) is None, "beyond Polars' 38-digit ceiling"


def test_polars_dtype_unknown_oid_disables_prediction():
    assert _polars_dtype(_col(114)) is None, "json → Python dict; must fall back, not guess"
    assert _polars_dtype(_col(3802)) is None


class _FakeCursor:
    def __init__(self, executed, rows):
        self._executed, self._rows = executed, rows

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=None):
        self._executed.append((" ".join(query.split()), params))

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, executed, rows):
        self._executed, self._rows = executed, rows
        self.closed = False

    def cursor(self):
        return _FakeCursor(self._executed, self._rows)

    def close(self):
        self.closed = True


def _stub_connect(monkeypatch, rows):
    executed = []
    conn = _FakeConn(executed, rows)
    monkeypatch.setattr(type(dialect), "_connect", lambda self, uri: conn)
    return executed, conn


BROWSE_URI = "denodo://u:p@h:9996/admin"


def test_list_schemas_reads_vdp_databases(monkeypatch):
    executed, conn = _stub_connect(monkeypatch, [("admin",), ("sales",)])
    assert dialect.list_schemas(BROWSE_URI) == ["admin", "sales"]
    assert executed == [("SELECT db_name FROM GET_DATABASES()", None)]
    assert conn.closed, "the browsing connection must not leak"


def test_list_tables_for_one_schema_returns_bare_view_names(monkeypatch):
    executed, _ = _stub_connect(monkeypatch, [("customers",), ("orders",)])
    assert dialect.list_tables(BROWSE_URI, "sales") == ["customers", "orders"]
    query, params = executed[0]
    assert query == "SELECT name FROM GET_VIEWS() WHERE input_database_name = %s AND database_name = %s"
    assert params == ("sales", "sales")


def test_list_tables_without_schema_qualifies_by_database(monkeypatch):
    executed, _ = _stub_connect(monkeypatch, [("sales", "orders"), ("hr", "staff")])
    assert dialect.list_tables(BROWSE_URI, None) == ["sales.orders", "hr.staff"]
    assert executed == [("SELECT database_name, name FROM GET_VIEWS()", None)]


def test_browsing_raises_instead_of_falling_back_to_sqlalchemy(monkeypatch):
    # Returning None would route the caller to SQLAlchemy inspection, whose pg_catalog.version()
    # connect probe Denodo cannot parse.
    def boom(self, uri):
        raise RuntimeError("connection refused")

    monkeypatch.setattr(type(dialect), "_connect", boom)
    with pytest.raises(RuntimeError):
        dialect.list_schemas(BROWSE_URI)
    with pytest.raises(RuntimeError):
        dialect.list_tables(BROWSE_URI, None)
