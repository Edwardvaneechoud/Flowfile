"""A Polars Code or SQL Query node's schema prediction runs as a job of its own and holds nothing once it answered.

The run and the prediction used to share the job ``<hash>-polars-code`` / ``<hash>-sql-query``, so a prediction after a
run replaced the run's held result on the worker with its own empty one and took its place among the node's results.
A prediction before the node's first run still runs under the run's id: nothing is held under it yet, and the run
replaces what the prediction left.
"""

import pytest

from flowfile_core.configs import settings
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema, schemas, transform_schema
from tests.flowfile.execution.test_held_results_release import add_promise, held, rows  # noqa: F401


@pytest.fixture(autouse=True)
def protocol_on(monkeypatch):
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", True)


def _input_then_user_code(flow_id: int, node_type: str) -> FlowGraph:
    """manual_input(1) -> polars_code or sql_query(2), each reading the input's rows."""
    graph = FlowGraph(
        schemas.FlowSettings(
            flow_id=flow_id, name=node_type, path=".", execution_location="local", execution_mode="Development"
        )
    )
    add_promise(graph, 1, "manual_input")
    graph.add_manual_input(input_schema.NodeManualInput(flow_id=flow_id, node_id=1, raw_data_format=rows(5)))
    add_promise(graph, 2, node_type)
    if node_type == "polars_code":
        graph.add_polars_code(
            input_schema.NodePolarsCode(
                flow_id=flow_id,
                node_id=2,
                depending_on_ids=[1],
                polars_code_input=transform_schema.PolarsCodeInput(polars_code="input_df.collect()"),
            )
        )
    else:
        graph.add_sql_query(
            input_schema.NodeSqlQuery(
                flow_id=flow_id,
                node_id=2,
                depending_on_ids=[1],
                sql_query_input=transform_schema.SqlQueryInput(sql_code="SELECT * FROM input_1"),
            )
        )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    return graph


@pytest.mark.parametrize(
    ("node_type", "held_by_the_node"),
    [("polars_code", True), ("sql_query", False)],
    ids=["polars-code-over-collected-rows", "sql-over-a-stored-input"],
)
def test_a_prediction_after_a_run_leaves_the_runs_result(held, node_type, held_by_the_node):  # noqa: F811
    """Collected rows come back as a stored result the node holds; a query over the stored input stays deferred."""
    graph = _input_then_user_code(99_611 if node_type == "polars_code" else 99_612, node_type)
    info = graph.run_graph()
    assert info.success, [(r.node_id, r.error) for r in info.node_step_result]
    node = graph.get_node(2)
    refs = dict(node._operation_refs)
    held_after_run = held()
    assert ("result" in refs and refs["result"].result_id in held_after_run) == held_by_the_node

    predicted = node.get_predicted_schema(force=True)

    assert [column.name for column in predicted] == ["name", "rank"]
    assert node._operation_refs == refs
    assert held() == held_after_run
    assert node.get_resulting_data().collect().height == 5
