"""A hub's server on an older Flowfile knows only its internal token, so it answers a hub's run token 401 "Invalid or
missing internal token"; a backend filled from the hub says so in one sentence that names the server and the way out,
and a worker that is no hub server keeps the worker's own answer."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.errors import TaskFailed
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.hub_client import client, node_tokens, session
from tests.hub_client.conftest import FakeHub, compute_node, serve_fake, store  # noqa: F401

pytestmark = pytest.mark.usefixtures("protocol_on")

USER_ID = 48
NODE_ID = 4
BACKEND_ID = "worker:team"
SENTENCE = (
    "The hub's server team runs an older Flowfile, which can't take this hub's runs; an admin joins it again with the "
    "line from the hub's Add a server."
)
WORKERS_ANSWER = "The worker answered 401: Invalid or missing internal token"


class OlderWorker:
    """A worker address that answers every request 401 "Invalid or missing internal token", as a worker that knows
    only its internal token answers a hub's run token."""

    def __init__(self):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                body = json.dumps({"detail": "Invalid or missing internal token"}).encode()
                self.send_response(401)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

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
def older_worker():
    worker = OlderWorker()
    yield worker
    worker.close()


@pytest.fixture
def hub_server(serve_fake, store, older_worker):  # noqa: F811
    """``team`` as a sync from the hub filled it, at the older worker's address, with the hub session of the person
    whose row it is, so the backend signs with the token the hub mints for ``team``."""
    fake = FakeHub()
    fake.url = serve_fake(fake)
    fake.nodes = [compute_node(NODE_ID, "team", worker_url=older_worker.url)]
    tokens = client.sign_in(fake.url, "ana", "s3cret")
    stored = {"hub_url": fake.url, "refresh_token": tokens["refresh_token"], "device_name": "ana-laptop"}
    session.save(USER_ID, stored)
    with get_db_context() as db:
        db.add(ConfiguredBackend(id=BACKEND_ID, url=older_worker.url, remote_id=NODE_ID, account_user_id=USER_ID))
        db.commit()
    return fake


def test_a_hub_server_on_an_older_flowfile_is_said_in_one_sentence_and_its_verdict_carries_it(hub_server):
    backend = registry.backend(BACKEND_ID)

    with pytest.raises(TaskFailed) as reading_capabilities:
        backend.capabilities()
    with pytest.raises(TaskFailed) as at_the_gate:
        handshake.ensure_handshake(backend.url, backend_id=BACKEND_ID)

    assert str(reading_capabilities.value) == SENTENCE
    assert str(at_the_gate.value) == SENTENCE
    assert handshake.last_verdict(BACKEND_ID).error == SENTENCE


def test_a_worker_that_is_no_hub_server_keeps_the_workers_answer(older_worker):
    backend = WorkerBackend(url=older_worker.url, backend_id="worker:plain", token="row-token")

    with pytest.raises(TaskFailed) as refused:
        backend.capabilities()

    assert str(refused.value) == WORKERS_ANSWER
