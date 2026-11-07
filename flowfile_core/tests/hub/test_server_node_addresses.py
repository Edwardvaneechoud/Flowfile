"""A server joined to its hub reads its hub's servers before a run that names one, so a run reaches the address the hub
has now with nobody opening the Compute page, a server the hub no longer lists is gone and one it lists gets its row;
a hub that cannot be read leaves the rows as they are, and a hub server that cannot be reached is named in the run's
error, with why in a few words and where to check its address, while the transport's own text goes to the log.
Another person's row stays theirs on a joined server, and a desktop's sync is its one user's list, as before."""

import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from flowfile_core.configs import logger as core_logger
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.hub_client import client, node_tokens, session
from shared import hub_node
from shared.tests import hub_join
from tests.conftest import closed_url
from tests.flowfile.execution.graph_helpers import add_manual_input, make_graph, ranks
from tests.hub_client.conftest import FakeHub, compute_node, serve_fake, sign_in_through_hub, store  # noqa: F401

pytestmark = pytest.mark.usefixtures("protocol_on")
join_hub = hub_join.join_hub

USER_ID = 47
OTHER_ID = 48
NODE_ID = 3
BACKEND_ID = "worker:team"
SALES_ID = 4
SALES = "worker:sales"
HUB_ADVICE = "Start it, or check its address on the hub's Compute page."


def did_not_answer(url: str) -> str:
    return f'The hub\'s server "team" did not answer at {url} (connection refused). {HUB_ADVICE}'


class Recorder:
    """A worker address that answers every request 503 and keeps each one's path and the token it was signed with."""

    def __init__(self):
        self.requests: list[tuple[str, str]] = []
        recorder = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                recorder.requests.append((self.path, self.headers.get("X-Flowfile-Internal", "")))
                self.send_response(503)
                self.send_header("Content-Length", "0")
                self.end_headers()

            do_POST = do_GET

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    registry.reset()
    handshake.reset_handshake()
    monkeypatch.setattr(node_tokens, "_tokens", {})
    monkeypatch.setattr(client, "_module_http", None)
    yield
    with get_db_context() as db:
        for row in db.query(ConfiguredBackend).all():
            db.delete(row)
        db.commit()
    registry.reset()
    handshake.reset_handshake()


@pytest.fixture
def joined(node_file, monkeypatch, join_hub):
    """This storage dir joined ``join_hub`` with keys of its own."""
    for name in ("FLOWFILE_HUB_JOIN_TOKEN", "FLOWFILE_HUB_URL", "FLOWFILE_NODE_NAME"):
        monkeypatch.delenv(name, raising=False)
    hub_node.join(join_hub.url, join_hub.mint(enable_on_join=True), name="core-1", worker_url="http://core-1:63579")


@pytest.fixture
def hub(serve_fake, store):  # noqa: F811
    """A thread-served hub that lists ``team`` at an address nothing listens on, and a person signed in to it whose
    session the ``team`` row, filled by an earlier sync at that same address, belongs to."""
    fake = FakeHub()
    fake.url = serve_fake(fake)
    old = closed_url()
    fake.nodes = [compute_node(NODE_ID, "team", worker_url=old)]
    tokens = client.sign_in(fake.url, "ana", "s3cret")
    stored = {"hub_url": fake.url, "refresh_token": tokens["refresh_token"], "device_name": "core-1"}
    session.save(USER_ID, stored)
    with get_db_context() as db:
        db.add(ConfiguredBackend(id=BACKEND_ID, url=old, hub_node_id=NODE_ID, hub_user_id=USER_ID))
        db.commit()
    fake.calls.clear()
    return fake


@pytest.fixture
def recorder():
    worker = Recorder()
    yield worker
    worker.close()


def moved(hub: FakeHub, url: str) -> None:
    hub.nodes = [compute_node(NODE_ID, "team", worker_url=url + "/")]


def row_url() -> str:
    with get_db_context() as db:
        return db.get(ConfiguredBackend, BACKEND_ID).url


@pytest.fixture
def signed_in(hub):
    """The rows' person signed in through the hub, as a person on a joined server is."""
    sign_in_through_hub(hub, USER_ID)
    hub.calls.clear()
    return hub


def rows() -> dict[str, tuple[str, int | None, int | None]]:
    with get_db_context() as db:
        return {row.id: (row.url, row.hub_node_id, row.hub_user_id) for row in db.query(ConfiguredBackend).all()}


def sales_row_of(user_id: int) -> None:
    """A ``worker:sales`` row an earlier sync wrote for ``user_id``."""
    with get_db_context() as db:
        db.add(ConfiguredBackend(id=SALES, url=closed_url(), hub_node_id=SALES_ID, hub_user_id=user_id))
        db.commit()


def run(flow_id: int, backend_id: str = BACKEND_ID):
    """A run of ``USER_ID``'s, the person the rows belong to."""
    graph = make_graph(flow_id, execution_location=backend_id)
    add_manual_input(graph, 1, ranks(3))
    try:
        with node_tokens.acting_for(USER_ID):
            run_info = graph.run_graph()
    finally:
        graph.close_flow()
    assert run_info.success is False
    return run_info


def test_a_server_the_hub_moved_is_reached_at_its_new_address_with_nobody_opening_compute(
    joined, hub, signed_in, recorder
):
    moved(hub, recorder.url)

    run(9911)

    assert row_url() == recorder.url
    assert hub.calls.count(("GET", "/hub/compute-nodes")) == 1
    token = node_tokens.token_for(USER_ID, NODE_ID, "team")
    assert recorder.requests and all(sent == token for _, sent in recorder.requests)
    assert recorder.requests[0][0] == "/v1/capabilities"


def test_an_unreachable_hub_server_is_named_in_the_runs_error_with_the_hubs_compute_page(
    joined, hub, signed_in, caplog
):
    new = closed_url()
    moved(hub, new)

    with caplog.at_level(logging.WARNING):
        (row,) = run(9912).node_step_result

    assert row.compute_error == "ComputeUnavailable"
    assert row.error == did_not_answer(new)
    assert "HTTPConnectionPool" not in row.error
    logged = [r.getMessage() for r in caplog.records if r.name == "flowfile_core.flowfile.execution.errors"]
    assert logged and all(m.startswith(f"Could not reach the worker at {new}: HTTPConnectionPool") for m in logged)


def test_a_hub_that_cannot_be_read_leaves_the_row_and_the_run_uses_it(joined, hub, signed_in, caplog):
    old = row_url()
    node_tokens.token_for(USER_ID, NODE_ID, "team")
    moved(hub, closed_url())
    hub.down = True
    core_logger.addHandler(caplog.handler)  # core's logger does not propagate
    try:
        (row,) = run(9913).node_step_result
    finally:
        core_logger.removeHandler(caplog.handler)

    assert row_url() == old
    assert row.error == did_not_answer(old)
    warned = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any(m.startswith(f"Could not read {hub.url}'s servers before the run") for m in warned), warned


def test_a_desktop_does_not_read_its_hub_before_a_run(hub, recorder):
    old = row_url()
    moved(hub, recorder.url)

    run(9914)

    assert row_url() == old
    assert ("GET", "/hub/compute-nodes") not in hub.calls
    assert recorder.requests == []


def test_a_server_the_hub_no_longer_lists_is_gone_at_the_next_run(joined, hub, signed_in):
    hub.nodes = []

    (row,) = run(9915).node_step_result

    assert rows() == {}
    assert hub.calls.count(("GET", "/hub/compute-nodes")) == 1
    assert "Unknown compute backend 'worker:team'" in row.error


def test_a_server_the_hub_lists_gets_its_row_before_a_run_that_names_it(joined, hub, signed_in, recorder):
    hub.nodes = [*hub.nodes, compute_node(SALES_ID, "sales", worker_url=recorder.url)]

    run(9916, SALES)

    assert rows()[SALES] == (recorder.url, SALES_ID, USER_ID)
    token = node_tokens.token_for(USER_ID, SALES_ID, "sales")
    assert recorder.requests and all(sent == token for _, sent in recorder.requests)


def test_another_persons_row_stays_theirs_while_they_are_signed_in_through_the_hub(joined, hub, signed_in):
    sales_row_of(OTHER_ID)
    sign_in_through_hub(hub, OTHER_ID)

    run(9917)

    assert rows()[SALES][2] == OTHER_ID
    assert rows()[BACKEND_ID][2] == USER_ID


def test_a_row_of_a_person_no_longer_signed_in_follows_the_next_persons_list(joined, hub, signed_in):
    sales_row_of(OTHER_ID)
    with get_db_context() as db:
        db.get(ConfiguredBackend, BACKEND_ID).hub_user_id = OTHER_ID
        db.commit()

    run(9918)

    assert SALES not in rows()
    assert rows()[BACKEND_ID][2] == USER_ID


def test_a_desktops_sync_is_its_one_users_list(hub):
    from flowfile_core.hub_client import node_addresses

    sales_row_of(OTHER_ID)
    with get_db_context() as db:
        node_addresses.sync_nodes(db, USER_ID, hub.url, session.access_token(USER_ID))

    assert set(rows()) == {BACKEND_ID}
