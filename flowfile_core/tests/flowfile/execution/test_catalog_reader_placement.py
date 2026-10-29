"""A catalog reader over a virtual table whose producer flow runs resolves it on the backend the reader runs on.

With an exchange store configured and the compute protocol flag on, that is the backend the reader's own
``execution_location`` names, so the producer flow's writer is submitted there beside the reader; with the flag off it
is the flow's, the reader's own setting unread. The backends are core's own under two ids, so no worker is needed, and
the schema prefetch is blocked over the build and the run, so each backend's work is what the run sent it.
"""

import pytest

from flowfile_core.configs import settings
from flowfile_core.flowfile.flow_node.flow_node import schema_prefetch_blocked
from flowfile_core.schemas import input_schema
from tests.flowfile import test_virtual_catalog_writer
from tests.flowfile.execution import test_placement
from tests.flowfile.execution.test_placement import add_promise, everything, make_graph

# The fixtures these tests share with the placement and virtual catalog writer tests, bound here.
declare = test_placement.declare
eager_virtual_table_id = test_virtual_catalog_writer.eager_virtual_table_id
clean_state = test_virtual_catalog_writer.clean_state

pytestmark = pytest.mark.usefixtures("clean_state")

SECOND = "worker:second"


@pytest.mark.parametrize("reader", ["table", "sql"])
@pytest.mark.parametrize("flag", [False, True], ids=["flag-off", "flag-on"])
def test_a_virtual_table_is_resolved_on_the_backend_its_reader_runs_on(
    declare, eager_virtual_table_id, monkeypatch, flag, reader
):
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", flag)
    monkeypatch.setenv("FLOWFILE_EXCHANGE_URI", "s3://placement-exchange/runs")
    default = declare(everything())
    second = declare(everything(), SECOND)
    flow_id = 8640 + 2 * flag + (reader == "sql")
    source = (
        {"catalog_table_id": eager_virtual_table_id}
        if reader == "table"
        else {"sql_query": "SELECT * FROM eager_virtual"}
    )
    token = schema_prefetch_blocked.set(True)
    try:
        graph = make_graph(flow_id)
        add_promise(graph, 9, "catalog_reader")
        graph.add_catalog_reader(
            input_schema.NodeCatalogReader(flow_id=flow_id, node_id=9, execution_location=SECOND, **source)
        )
        run_info = graph.run_graph()
    finally:
        schema_prefetch_blocked.reset(token)

    assert run_info.success is True, [result.error for result in run_info.node_step_result]
    runs_on, unread = (second, default) if flag else (default, second)
    assert {"submit 3", "submit 9"} <= set(runs_on.work), (runs_on.work, unread.work)
    assert unread.work == []
    assert second.asked == (1 if flag else 0)
    assert graph.get_node(9).get_resulting_data().collect().height == 3
