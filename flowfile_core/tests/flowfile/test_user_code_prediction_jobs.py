"""A Polars Code or SQL Query node's schema prediction runs as a job of its own and holds nothing once it answered.

The run and the prediction used to share the job ``<hash>-polars-code`` / ``<hash>-sql-query``, so a prediction after a
run replaced the run's held result on the worker with its own empty one and took its place among the node's results.
A prediction before the node's first run still runs under the run's id: nothing is held under it yet, and the run
replaces what the prediction left.
"""

import pytest

from tests.flowfile.execution.conftest import held  # noqa: F401
from tests.flowfile.execution.graph_helpers import code_flow, ranks

pytestmark = pytest.mark.usefixtures("protocol_on")


@pytest.mark.parametrize(
    ("flow_id", "node_type", "code", "held_by_the_node"),
    [(99_611, "polars_code", "input_df.collect()", True), (99_612, "sql_query", "SELECT * FROM input_1", False)],
    ids=["polars-code-over-collected-rows", "sql-over-a-stored-input"],
)
def test_a_prediction_after_a_run_leaves_the_runs_result(
    held, flow_id, node_type, code, held_by_the_node  # noqa: F811
):
    """Collected rows come back as a stored result the node holds; a query over the stored input stays deferred."""
    graph = code_flow(flow_id, node_type, code, ranks(5), execution_location="local")
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
