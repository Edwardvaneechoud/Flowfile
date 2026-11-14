"""The optional ``database_name`` of the database reader and writer: storage, the composed names, the refusals."""

from pathlib import Path

import polars as pl
import pytest
import yaml

from flowfile_core.flowfile.execution.payloads import DatabaseWritePayload, unpack
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import (
    MISSING_SCHEMA_MESSAGE,
    BaseSqlSource,
    SqlSource,
    qualified_schema,
)
from flowfile_core.schemas import input_schema, schemas

REPO_ROOT = Path(__file__).resolve().parents[4]
SAVED_FLOW = REPO_ROOT / "docs" / "assets" / "flows" / "database_transform_write.yaml"


def _saved_setting_inputs() -> dict[str, dict]:
    nodes = yaml.safe_load(SAVED_FLOW.read_text())["nodes"]
    return {node["type"]: node["setting_input"] for node in nodes if node["type"].startswith("database_")}


def _reserialised(node_type: str, setting_input: dict) -> dict:
    model = input_schema.NodeDatabaseReader if node_type == "database_reader" else input_schema.NodeDatabaseWriter
    settings = model.model_validate({**setting_input, "flow_id": 1, "node_id": 1})
    node = schemas.FlowfileNode(id=1, type=node_type, setting_input=settings)
    return node.model_dump(mode="json")["setting_input"]


@pytest.mark.parametrize("node_type", ["database_reader", "database_writer"])
def test_a_saved_flow_without_a_database_round_trips_byte_for_byte(node_type):
    saved = _saved_setting_inputs()[node_type]
    reserialised = _reserialised(node_type, saved)
    # The fixture predates the per-node execution_location, which every node now writes.
    reserialised.pop("execution_location")
    assert reserialised == saved


def test_an_unset_database_is_not_written_and_a_set_one_is():
    settings = input_schema.DatabaseSettings(connection_mode="reference", database_connection_name="c", table_name="T")
    assert "database_name" not in settings.model_dump()
    assert "database_name" not in settings.model_dump(mode="json")
    set_one = settings.model_copy(update={"database_name": "SALES", "schema_name": "PUBLIC"})
    assert set_one.model_dump()["database_name"] == "SALES"
    loaded = input_schema.DatabaseSettings.model_validate(set_one.model_dump(mode="json"))
    assert loaded.database_name == "SALES"


@pytest.mark.parametrize("model", [input_schema.DatabaseSettings, input_schema.DatabaseWriteSettings])
def test_a_blank_database_is_none_and_the_identifier_rule_is_the_schema_rule(model):
    base = {"connection_mode": "reference", "database_connection_name": "c", "table_name": "T"}
    assert model(**base, database_name="  ").database_name is None
    assert model(**base, database_name="").database_name is None
    assert model(**base, database_name="SALES").database_name == "SALES"
    assert model(**base, database_name='"My DB"').database_name == '"My DB"'
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        model(**base, database_name="a;b")
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        model(**base, database_name="SALES.PUBLIC")


def test_default_descriptions_name_the_database_when_set():
    reader = input_schema.NodeDatabaseReader(
        flow_id=1,
        node_id=1,
        database_settings=input_schema.DatabaseSettings(
            connection_mode="reference", database_connection_name="c", table_name="T", schema_name="S"
        ),
    )
    assert reader.get_default_description() == "Read from S.T"
    reader.database_settings.database_name = "DB"
    assert reader.get_default_description() == "Read from DB.S.T"
    writer = input_schema.NodeDatabaseWriter(
        flow_id=1,
        node_id=2,
        database_write_settings=input_schema.DatabaseWriteSettings(
            connection_mode="reference", database_connection_name="c", table_name="T", database_name="DB"
        ),
    )
    assert writer.get_default_description() == "Write to DB.T (append)"


def test_qualified_schema():
    assert qualified_schema(None, None) is None
    assert qualified_schema(None, "") is None
    assert qualified_schema(None, "PUBLIC") == "PUBLIC"
    assert qualified_schema("SALES", "PUBLIC") == "SALES.PUBLIC"
    with pytest.raises(ValueError, match=MISSING_SCHEMA_MESSAGE):
        qualified_schema("SALES", None)


def test_the_from_clause_with_and_without_a_database():
    assert BaseSqlSource(table_name="T").query == "SELECT * FROM T"
    assert BaseSqlSource(table_name="T", schema_name="S").query == "SELECT * FROM S.T"
    source = BaseSqlSource(table_name="CUSTOMER", schema_name="TPCH_SF100", database_name="SNOWFLAKE_SAMPLE_DATA")
    assert source.query == "SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.CUSTOMER"
    assert source.schema_path == "SNOWFLAKE_SAMPLE_DATA.TPCH_SF100"
    assert source.schema_name == "TPCH_SF100"


def test_a_database_without_a_schema_is_refused_in_table_mode_only():
    with pytest.raises(ValueError, match=MISSING_SCHEMA_MESSAGE):
        BaseSqlSource(table_name="T", database_name="SALES")
    query = BaseSqlSource(query="SELECT 1 AS a", database_name="SALES")
    assert query.query == "SELECT 1 AS a"
    assert query.schema_path is None


def test_the_dialect_schema_hook_gets_the_database_qualified_schema(monkeypatch):
    from shared.db_dialects import SnowflakeDialect

    seen: list[tuple] = []

    def table_schema(self, uri, table_name, schema_name):
        seen.append((table_name, schema_name))
        return pl.Schema({"A": pl.Int64})

    monkeypatch.setattr(SnowflakeDialect, "table_schema", table_schema)
    source = SqlSource(
        "snowflake://u:t@h:443/ANALYTICS",
        table_name="CUSTOMER",
        schema_name="TPCH_SF100",
        database_name="SNOWFLAKE_SAMPLE_DATA",
    )
    assert [c.column_name for c in source.get_schema()] == ["A"]
    assert seen == [("CUSTOMER", "SNOWFLAKE_SAMPLE_DATA.TPCH_SF100")]


def _writer_graph(tmp_path, **write_settings) -> FlowGraph:
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


def _written_table_name(monkeypatch, graph: FlowGraph) -> str:
    sent: list = []
    monkeypatch.setattr(FlowNode, "run_op", lambda self, backend, kind, payload, job_id=None: sent.append(payload))
    graph.get_node(2)._function(FlowDataEngine(pl.DataFrame({"a": [1]})))
    (payload,) = sent
    return unpack(payload, DatabaseWritePayload).table_name


@pytest.mark.parametrize(
    ("write_settings", "table_name"),
    [
        ({"table_name": "T"}, "T"),
        ({"table_name": "T", "schema_name": "S"}, "S.T"),
        ({"table_name": "T", "schema_name": "S", "database_name": "DB"}, "DB.S.T"),
    ],
)
def test_the_writer_target_with_and_without_a_database(tmp_path, monkeypatch, write_settings, table_name):
    assert _written_table_name(monkeypatch, _writer_graph(tmp_path, **write_settings)) == table_name


def test_the_writer_refuses_a_database_without_a_schema(tmp_path, monkeypatch):
    graph = _writer_graph(tmp_path, table_name="T", database_name="DB")
    with pytest.raises(ValueError, match=MISSING_SCHEMA_MESSAGE):
        _written_table_name(monkeypatch, graph)
