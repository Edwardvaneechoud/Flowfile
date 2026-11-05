"""A node set to one of the hub's servers that no store reaches is refused before the run, with one sentence.

Without an exchange store a node's own Compute backend is not read and the node runs on the run's backend. A node set
to a backend that is no server of the hub stays so, on a desktop as before; one set to a server of the hub, a row
filled from the hub or this server once joined with keys of its own, is refused before any node starts with one
sentence that says what to change, and nothing is sent to the backend it names.
"""

import pytest

from flowfile_core.configs import exchange_config
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from shared import hub_node
from shared.tests import hub_join
from tests.flowfile.execution.graph_helpers import input_then_sort, make_graph, ranks

pytestmark = pytest.mark.usefixtures("protocol_on")
join_hub = hub_join.join_hub

ELSEWHERE = "worker:elsewhere"
SENTENCE = (
    f'cannot run on backend "{ELSEWHERE}": the step "Sort data 2" is set to it, but no exchange store can carry its '
    f'data between backends on this server, so set the flow\'s Execution location under Flow Settings to "{ELSEWHERE}" '
    "to run the whole flow there, or its Compute backend under General Settings to Auto"
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
    for name in ("FLOWFILE_HUB_JOIN_TOKEN", "FLOWFILE_HUB_URL", "FLOWFILE_NODE_NAME"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(hub_node, "path", lambda: tmp_path / "storage" / hub_node.FILE_NAME)
    hub_node.join(
        join_hub.url,
        join_hub.mint(enable_on_join=True),
        name="team-1",
        worker_url="http://team-1.example:63579",
        core_url="http://team-1.example:63578",
    )


@pytest.fixture
def hub_server():
    """``ELSEWHERE`` as a sync from the hub fills it: a row with the hub's node id and no token, gone after the test."""
    with get_db_context() as db:
        db.add(ConfiguredBackend(id=ELSEWHERE, url="http://elsewhere.example:63579", hub_node_id=7, hub_user_id=1))
        db.commit()
    yield
    registry.forget(ELSEWHERE)
    with get_db_context() as db:
        row = db.get(ConfiguredBackend, ELSEWHERE)
        if row is not None:
            db.delete(row)
            db.commit()


@pytest.fixture
def hand_added():
    """``ELSEWHERE`` as someone added it on the Backends page: a row with its own token and no hub node, gone after."""
    with get_db_context() as db:
        db.add(ConfiguredBackend(id=ELSEWHERE, url="http://elsewhere.example:63579", token="added-here"))
        db.commit()
    yield
    registry.forget(ELSEWHERE)
    with get_db_context() as db:
        row = db.get(ConfiguredBackend, ELSEWHERE)
        if row is not None:
            db.delete(row)
            db.commit()


def flow(flow_id: int, sort_location: str):
    """Manual input 1 → sort 2 on ``local``, the sort's own Compute backend ``sort_location``."""
    graph = input_then_sort(make_graph(flow_id, execution_location="local"), ranks(3), "rank")
    graph.get_node(2).setting_input.execution_location = sort_location
    return graph


@pytest.mark.parametrize("where", ["joined_with_keys", "desktop"])
def test_a_node_set_to_a_server_of_the_hub_is_refused_before_the_run_with_one_sentence(
    request, hub_server, no_store, where
):
    if where == "joined_with_keys":
        request.getfixturevalue("joined_with_keys")
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


@pytest.mark.parametrize("where", ["joined_with_keys", "desktop"])
def test_a_node_set_to_a_backend_that_is_no_server_of_the_hub_still_runs_on_the_runs_backend(
    request, hand_added, no_store, where
):
    if where == "joined_with_keys":
        request.getfixturevalue("joined_with_keys")
    graph = flow(9843, ELSEWHERE)
    try:
        run_info = graph.run_graph()

        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        assert graph.get_node(2).results.result_ref.backend_id == "local"
    finally:
        graph.close_flow()


HUB_STORE = {"uri": "s3://org-bucket/exchange", "connection": "org exchange", "max_rows": 50000}


@pytest.fixture
def hubs_store(monkeypatch):
    """User 1 signed in to a fake hub that sets ``HUB_STORE``, as a server joined with keys asks it for a run."""
    from flowfile_core.hub_client import client, session
    from flowfile_core.hub_client import exchange_store as hub_exchange_store
    from tests.hub_client.conftest import HUB_URL, FakeHub

    fake = FakeHub()
    monkeypatch.setattr(client, "_module_http", fake.http())
    monkeypatch.setattr(hub_exchange_store, "_warned", set())
    fake.exchange_store = dict(HUB_STORE)
    tokens = client.sign_in(HUB_URL, "ana", "s3cret")
    session.save(1, {"hub_url": HUB_URL, "refresh_token": tokens["refresh_token"]})
    return fake


@pytest.fixture
def hub_lists_it(hubs_store):
    """The fake hub lists ``ELSEWHERE`` as ``hub_server``'s row has it, so the refresh before placing keeps that row."""
    from tests.hub_client.conftest import compute_node

    hubs_store.nodes = [compute_node(7, "elsewhere", worker_url="http://elsewhere.example:63579")]
    return hubs_store


@pytest.fixture
def vending(monkeypatch):
    """``local`` and ``ELSEWHERE`` declare that they open what the hub seals to their server."""
    from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
    from tests.flowfile.execution.test_placement import DeclaringBackend, everything

    declared = everything().model_copy(update={"credential_resolution": "vended"})
    registry.reset()
    handshake.reset_handshake()
    for backend_id in (registry.LOCAL_BACKEND_ID, ELSEWHERE):
        backend = DeclaringBackend(backend_id, declared)
        registry.register_backend(backend_id, lambda backend=backend: backend)
    yield
    registry.reset()
    handshake.reset_handshake()


@pytest.fixture
def client():
    from fastapi.testclient import TestClient

    from flowfile_core import main

    with TestClient(main.app) as c:
        token = c.post("/auth/token").json()["access_token"]
    authed = TestClient(main.app)
    authed.headers = {"Authorization": f"Bearer {token}"}
    return authed


@pytest.fixture
def open_flow():
    """Manual input 1 → sort 2 open in user 1's session on ``local``, the sort set to ``ELSEWHERE``."""
    from flowfile_core import flow_file_handler
    from flowfile_core.schemas import schemas
    from tests.flowfile.execution.graph_helpers import add_manual_input, add_sort_on

    flow_id = 9851
    if flow_file_handler.get_flow(flow_id) is not None:
        flow_file_handler.delete_flow(flow_id)
    flow_file_handler.register_flow(
        schemas.FlowSettings(flow_id=flow_id, name="hub_store_preview", path=".", execution_location="local"),
        user_id=1,
    )
    graph = flow_file_handler.get_flow(flow_id)
    add_manual_input(graph, 1, ranks(3))
    add_sort_on(graph, 2, 1, "rank", execution_location=ELSEWHERE)
    yield graph
    flow_file_handler.delete_flow(flow_id)


def test_the_preview_on_a_joined_server_with_the_hubs_store_refuses_nothing_for_a_node_on_a_hub_server(
    joined_with_keys, hub_server, no_store, vending, hubs_store, hub_lists_it, client, open_flow
):
    response = client.get("/flow/placement", params={"flow_id": open_flow.flow_id})

    assert response.status_code == 200, response.text
    preview = response.json()
    assert preview["refusals"] == []
    assert preview["assignment"] == {"1": registry.LOCAL_BACKEND_ID, "2": ELSEWHERE}
    assert preview["boundaries"] == [[1, 2]]
    assert ("POST", "/hub/vend") not in hubs_store.calls, "a preview vends nothing"


def test_the_preview_on_a_joined_server_whose_hub_sets_no_store_keeps_the_refusal(
    joined_with_keys, hub_server, no_store, vending, hubs_store, hub_lists_it, client, open_flow
):
    hubs_store.exchange_store = None

    preview = client.get("/flow/placement", params={"flow_id": open_flow.flow_id}).json()

    assert [(refusal["node_id"], refusal["backend_id"]) for refusal in preview["refusals"]] == [(2, ELSEWHERE)]
    assert "no exchange store can carry its data" in preview["refusals"][0]["reason"]


def test_the_preview_refuses_a_crossing_to_a_backend_that_is_no_server_of_the_hub_with_the_runs_sentence(
    joined_with_keys, hand_added, no_store, vending, hubs_store, client, open_flow
):
    preview = client.get("/flow/placement", params={"flow_id": open_flow.flow_id}).json()

    reasons = {refusal["node_id"]: refusal["reason"] for refusal in preview["refusals"]}
    assert set(reasons) == {1, 2}
    assert all(f'backend "{ELSEWHERE}" is not one of the hub\'s servers' in reason for reason in reasons.values())


@pytest.fixture
def unsealing():
    """``local`` opens what the hub seals to its server; ``ELSEWHERE`` declares that it cannot open a sealed one."""
    from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
    from tests.flowfile.execution.test_placement import DeclaringBackend, everything

    registry.reset()
    handshake.reset_handshake()
    for backend_id, resolution in ((registry.LOCAL_BACKEND_ID, "vended"), (ELSEWHERE, "master_key")):
        declared = everything().model_copy(update={"credential_resolution": resolution})
        backend = DeclaringBackend(backend_id, declared)
        registry.register_backend(backend_id, lambda backend=backend: backend)
    yield
    registry.reset()
    handshake.reset_handshake()


def test_the_preview_names_the_step_whose_input_cannot_cross_through_the_hubs_store_by_its_canvas_name(
    joined_with_keys, hub_server, no_store, unsealing, hubs_store, client, open_flow
):
    open_flow.get_node(1).setting_input.description = "The ranks"

    preview = client.get("/flow/placement", params={"flow_id": open_flow.flow_id}).json()

    (refusal,) = preview["refusals"]
    assert (refusal["node_id"], refusal["backend_id"]) == (2, ELSEWHERE)
    assert refusal["reason"].startswith(
        f'its input from the step "The ranks" crosses through the hub\'s exchange store, but on backend "{ELSEWHERE}" '
    )
    assert "node 1" not in refusal["reason"]


def test_a_step_is_named_by_the_first_line_of_its_description_else_by_its_types_name_and_number():
    from flowfile_core.flowfile.execution.placement import run_here, step_label, steps_in_words

    graph = input_then_sort(make_graph(9852, execution_location="local"), ranks(3), "rank")
    try:
        manual, sort = graph.get_node(1), graph.get_node(2)
        sort.setting_input.description = "  Sort the ranks\nhighest last"

        assert (step_label(manual), step_label(sort)) == ("Manual input 1", "Sort the ranks")
        assert steps_in_words([manual]) == 'the step "Manual input 1"'
        assert steps_in_words([manual, sort, manual]) == 'the steps "Manual input 1", "Sort the ranks" and "Manual input 1"'
        assert run_here("it") == "run it on this server (set its Compute backend under General Settings)"
        assert run_here("them", 2, "this computer") == (
            "run them on this computer (set their Compute backend under General Settings)"
        )
    finally:
        graph.close_flow()
