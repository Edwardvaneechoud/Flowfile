"""Quoted and ``$`` names: the settings take the widest shape, the connection's dialect decides where it is used.

Only Snowflake (``quoted_identifiers``) lets ``"USER$evaneechoud"`` or ``"my.db"."My Schema".T`` into its SQL; every
other dialect, and a source whose dialect is unknown, keeps the plain rule with the historical message.
"""

from types import SimpleNamespace

import polars as pl
import pytest

from flowfile_core.flowfile.execution.backends.worker import _database_read_settings
from flowfile_core.flowfile.execution.payloads import DatabaseReadPayload, DatabaseWritePayload, unpack
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_graph.builders import connectors
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import (
    BaseSqlSource,
    qualified_table,
)
from flowfile_core.schemas import input_schema, schemas
from shared.db_dialects import get_dialect
from shared.sql_validation import UnsafeSQLError

PLAIN_MESSAGE = "Only letters, numbers, and underscores are allowed"
SETTINGS_MODELS = [input_schema.DatabaseSettings, input_schema.DatabaseWriteSettings]
BASE = {"connection_mode": "reference", "database_connection_name": "c"}


@pytest.mark.parametrize("model", SETTINGS_MODELS)
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("table_name", '"USER$evaneechoud"'),
        ("table_name", '"my.db"."My Schema".T'),
        ("table_name", "A$B"),
        ("schema_name", '"My Schema"'),
        ("database_name", '"USER$evaneechoud"'),
    ],
)
def test_the_settings_accept_the_widest_shape(model, field, value):
    settings = model(**BASE, **({"table_name": "T"} | {field: value}))
    assert getattr(settings, field) == value
    assert model.model_validate(settings.model_dump(mode="json")) == settings


@pytest.mark.parametrize("model", SETTINGS_MODELS)
@pytest.mark.parametrize("value", ['a"b', '"unterminated', "a;b", "a b", "a--b", '""', "a..b", "t; DROP TABLE x"])
def test_the_settings_refuse_malformed_names(model, value):
    for field in ("table_name", "schema_name", "database_name"):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            model(**BASE, **({"table_name": "T"} | {field: value}))


@pytest.mark.parametrize("database_type", ["postgresql", "mysql", "sqlite", "duckdb", "mssql", "denodo", None])
@pytest.mark.parametrize(
    "names",
    [
        {"table_name": '"T"'},
        {"table_name": "A$B"},
        {"table_name": "T", "schema_name": '"My Schema"'},
        {"table_name": "T", "schema_name": "S", "database_name": '"USER$evaneechoud"'},
    ],
)
def test_other_dialects_refuse_quoted_names_in_the_from_clause(database_type, names):
    with pytest.raises(UnsafeSQLError, match=PLAIN_MESSAGE):
        BaseSqlSource(database_type=database_type, **names)


def test_snowflake_puts_quoted_names_into_the_from_clause_as_given():
    source = BaseSqlSource(
        database_type="snowflake", table_name="orders", schema_name='"My Schema"', database_name='"USER$evaneechoud"'
    )
    assert source.query == 'SELECT * FROM "USER$evaneechoud"."My Schema".orders'
    assert BaseSqlSource(database_type="snowflake", table_name='"my.db"."S".T').query == 'SELECT * FROM "my.db"."S".T'
    with pytest.raises(UnsafeSQLError, match="double quotes"):
        BaseSqlSource(database_type="snowflake", table_name='a"b')


def test_qualified_table_checks_each_part_with_the_dialect():
    snowflake, sqlite = get_dialect("snowflake"), get_dialect("sqlite")
    assert qualified_table(snowflake, '"USER$evaneechoud"', '"Raw"', "T") == '"USER$evaneechoud"."Raw".T'
    assert qualified_table(sqlite, None, "main", "t") == "main.t"
    with pytest.raises(UnsafeSQLError, match=PLAIN_MESSAGE):
        qualified_table(sqlite, None, '"main"', "t")
    with pytest.raises(UnsafeSQLError, match="double quotes"):
        qualified_table(snowflake, None, None, '"open')


def _writer(tmp_path, **write_settings) -> FlowGraph:
    graph = FlowGraph(flow_settings=schemas.FlowSettings(flow_id=1, name="db_writer", path=str(tmp_path)))
    graph.add_database_writer(
        input_schema.NodeDatabaseWriter(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            database_write_settings=input_schema.DatabaseWriteSettings(
                connection_mode="inline",
                database_connection=input_schema.DatabaseConnection(
                    database_type="sqlite", database=str(tmp_path / "out.db")
                ),
                **write_settings,
            ),
        )
    )
    return graph


def _run_writer(monkeypatch, graph: FlowGraph) -> list:
    sent: list = []
    monkeypatch.setattr(FlowNode, "run_op", lambda self, backend, kind, payload, job_id=None: sent.append(payload))
    graph.get_node(2)._function(FlowDataEngine(pl.DataFrame({"a": [1]})))
    return sent


@pytest.mark.parametrize(
    "write_settings",
    [{"table_name": "A$B"}, {"table_name": '"T"'}, {"table_name": "t", "schema_name": '"main"'}],
)
def test_a_sqlite_writer_refuses_a_quoted_target_when_it_runs(tmp_path, monkeypatch, write_settings):
    graph = _writer(tmp_path, **write_settings)
    with pytest.raises(UnsafeSQLError, match=PLAIN_MESSAGE):
        _run_writer(monkeypatch, graph)
    assert not (tmp_path / "out.db").exists()


def test_a_snowflake_writer_sends_the_quoted_target_as_given(tmp_path, monkeypatch):
    graph = FlowGraph(flow_settings=schemas.FlowSettings(flow_id=1, name="db_writer", path=str(tmp_path)))
    graph.add_database_writer(
        input_schema.NodeDatabaseWriter(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            database_write_settings=input_schema.DatabaseWriteSettings(
                connection_mode="reference",
                database_connection_name="sf",
                database_name='"USER$evaneechoud"',
                schema_name='"Raw"',
                table_name="orders",
            ),
        )
    )
    connection = SimpleNamespace(database_type="snowflake", granted=False, connection_name="sf")
    monkeypatch.setattr(connectors, "_resolve_database_credentials", lambda settings, user_id: (connection, "x", None))
    (payload,) = _run_writer(monkeypatch, graph)
    assert unpack(payload, DatabaseWritePayload).table_name == '"USER$evaneechoud"."Raw".orders'


def _reader_payload(database_type: str, **names) -> DatabaseReadPayload:
    """A reference-mode read of a saved connection of ``database_type`` (Snowflake has no inline connections)."""
    connection = input_schema.FullDatabaseConnection(
        connection_name="c",
        database_type=database_type,
        username="u",
        password="enc",
        host="myorg-acct.snowflakecomputing.com" if database_type == "snowflake" else "localhost",
        database="x.db",
        auth_method="access_token" if database_type == "snowflake" else None,
    )
    settings = input_schema.NodeDatabaseReader(
        flow_id=1,
        node_id=1,
        database_settings=input_schema.DatabaseSettings(
            connection_mode="reference", database_connection_name="c", **names
        ),
    )
    return DatabaseReadPayload(
        flow_id=1,
        node_id=1,
        settings=settings,
        connection=connection,
        encrypted_password="enc",
        cancel_check=lambda: False,
    )


def test_the_worker_read_builds_its_query_with_the_connection_dialect():
    postgres = _reader_payload("postgresql", table_name="t", schema_name="public")
    assert _database_read_settings(postgres).query == "SELECT * FROM public.t"
    with pytest.raises(UnsafeSQLError, match=PLAIN_MESSAGE):
        _database_read_settings(_reader_payload("postgresql", table_name='"t"'))
    snowflake = _reader_payload("snowflake", table_name="orders", schema_name='"Raw"', database_name='"DB"')
    assert _database_read_settings(snowflake).query == 'SELECT * FROM "DB"."Raw".orders'


def test_an_export_writes_a_quoted_name_as_a_valid_python_literal():
    from flowfile_core.flowfile.code_generator.base import ConverterMixinBase

    literal = ConverterMixinBase._py_str('"USER$evaneechoud"."My ""odd"" Schema"')
    assert eval(literal) == '"USER$evaneechoud"."My ""odd"" Schema"'  # noqa: S307
