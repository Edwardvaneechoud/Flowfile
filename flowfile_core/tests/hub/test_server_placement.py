"""A server joined to its hub with keys of its own refuses, before the run, a node set to a backend no store reaches.

Without an exchange store a node's own Compute backend is not read and the node runs on the run's backend. On a
desktop that stays so; on a joined server, where that backend is another server, the node is refused before any node
starts with one sentence that says what to change, and nothing is sent to the backend it names.
"""

import pytest

from flowfile_core.configs import exchange_config
from shared import hub_node
from shared.tests import hub_join
from tests.flowfile.execution.graph_helpers import input_then_sort, make_graph, ranks

pytestmark = pytest.mark.usefixtures("protocol_on")
join_hub = hub_join.join_hub

ELSEWHERE = "worker:elsewhere"
SENTENCE = (
    f'cannot run on backend "{ELSEWHERE}": no exchange store can carry its data between backends on this server, so '
    f'set the flow\'s Execution location under Flow Settings to "{ELSEWHERE}" to run the whole flow there, or its '
    "Compute backend under General Settings to Auto"
)


@pytest.fixture
def no_store(monkeypatch):
    """No exchange store in the environment or in this storage dir's saved file."""
    for name in (exchange_config.URI_VAR, exchange_config.CONNECTION_VAR):
        monkeypatch.delenv(name, raising=False)
    assert not exchange_config.effective_store().uri


@pytest.fixture
def joined_with_keys(tmp_path, monkeypatch, join_hub):
    """This storage dir joined ``join_hub`` with keys of its own, as a server whose people sign in through its hub."""
    for name in ("FLOWFILE_HUB_JOIN_TOKEN", "FLOWFILE_HUB_URL", "FLOWFILE_NODE_NAME", "FLOWFILE_HUB_ISSUER"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(hub_node, "path", lambda: tmp_path / "storage" / hub_node.FILE_NAME)
    hub_node.join(
        join_hub.url,
        join_hub.mint(enable_on_join=True),
        name="team-1",
        worker_url="http://team-1.example:63579",
        core_url="http://team-1.example:63578",
    )


def flow(flow_id: int, sort_location: str):
    """Manual input 1 → sort 2 on ``local``, the sort's own Compute backend ``sort_location``."""
    graph = input_then_sort(make_graph(flow_id, execution_location="local"), ranks(3), "rank")
    graph.get_node(2).setting_input.execution_location = sort_location
    return graph


def test_a_node_set_to_another_backend_is_refused_before_the_run_with_one_sentence(joined_with_keys, no_store):
    graph = flow(9841, ELSEWHERE)
    try:
        run_info = graph.run_graph()

        assert run_info.success is False
        (row,) = run_info.node_step_result
        assert (row.node_id, row.compute_error, row.error) == (2, "Refusal", SENTENCE)
        assert graph.get_node(1).results.result_ref is None, "no node started"
        preview = graph.preview_placement()
        assert [(refusal.node_id, refusal.backend_id) for refusal in preview.refusals] == [(2, ELSEWHERE)]
    finally:
        graph.close_flow()


@pytest.mark.parametrize("location", ["auto", "local"])
def test_a_node_on_auto_or_on_the_runs_own_backend_runs(joined_with_keys, no_store, location):
    graph = flow(9842, location)
    try:
        run_info = graph.run_graph()

        assert run_info.success is True, [result.error for result in run_info.node_step_result]
    finally:
        graph.close_flow()


def test_a_desktop_still_runs_such_a_node_on_the_runs_backend(no_store):
    graph = flow(9843, ELSEWHERE)
    try:
        run_info = graph.run_graph()

        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        assert graph.get_node(2).results.result_ref.backend_id == "local"
    finally:
        graph.close_flow()
