"""Every call core makes to a hub's worker for a run carries the run token the hub minted for that node.

A worker joined to a hub has an internal token of its own, so core's process token is refused there and the hub's
``run`` token is the one way in. The handshake, ``capabilities`` and the ``/v1`` calls always signed with the
backend's token; the task routes (a submit, a status poll, a fetch, a cancel, a clear, the legacy socket) signed with
the process token, so a Delta write or any run with the protocol flag off answered "Invalid or missing internal
token". Here a second worker, started as a hub node with an internal token core does not hold, runs a flow and a
catalog write placed on its hub-filled row, with the protocol flag off and on, and every request core sent it carried
the hub's token and none was refused. The module skips when no second worker can be started.
"""

import os
import secrets
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator

import polars as pl
import pytest
import requests

from flowfile_core.auth.jwt import get_internal_token
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.hub_client import node_tokens
from shared.tests import hub_jwks
from shared.tests.hub_jwks import NODE_NAME
from tests.conftest import _free_port, protocol_states
from tests.flowfile.conftest import (
    add_test_catalog_writer,
    add_test_manual_input,
    catalog_cleanup,
    create_test_graph,
    create_test_namespace,
)
from tests.flowfile.execution.graph_helpers import input_then_sort, make_graph, ranks
from tests.flowfile.execution.test_hub_node_tokens_e2e import STARTUP_TIMEOUT, _stop

hub_signer = hub_jwks.hub_signer
protocol = protocol_states()

NODE = f"worker:{NODE_NAME}"
USER_ID, NODE_ID = 43, 5


class Sent:
    """Every request this process sent to one worker URL: its route, the token it carried and the status answered."""

    def __init__(self, url: str) -> None:
        self.url = url
        self.calls: list[tuple[str, str | None, int]] = []

    def tokens(self) -> set[str | None]:
        return {token for _, token, _ in self.calls}

    def refused(self) -> list[tuple[str, int]]:
        return [(path, status) for path, _, status in self.calls if status in (401, 403)]


@pytest.fixture
def node_worker(hub_signer: hub_jwks.HubSigner) -> Iterator[str]:
    """A worker joined to the test's hub as node ``team``, with an internal token of its own; its URL."""
    port = _free_port()
    env = {
        **os.environ,
        "FLOWFILE_WORKER_LOCAL_FILES": "1",
        "FLOWFILE_HUB_URL": hub_signer.url,
        "FLOWFILE_NODE_NAME": NODE_NAME,
        "FLOWFILE_INTERNAL_TOKEN": secrets.token_urlsafe(24),
    }
    env.pop("FLOWFILE_HUB_ISSUER", None)
    url = f"http://127.0.0.1:{port}"
    with tempfile.TemporaryFile(mode="w+") as output:
        proc = subprocess.Popen(
            [sys.executable, "-m", "flowfile_worker", "--port", str(port)],
            stdout=output,
            stderr=subprocess.STDOUT,
            env=env,
            preexec_fn=os.setsid if hasattr(os, "setsid") else None,
        )
        try:
            deadline = time.monotonic() + STARTUP_TIMEOUT
            while time.monotonic() < deadline and proc.poll() is None:
                try:
                    if requests.get(f"{url}/docs", timeout=2).ok:
                        break
                except requests.RequestException:
                    time.sleep(0.5)
            else:
                output.seek(0)
                tail = output.read()[-2000:]
                _stop(proc)
                pytest.skip(f"a second flowfile_worker could not be started on {url}:\n{tail}")
            yield url
        finally:
            _stop(proc)


@pytest.fixture
def node_row(node_worker: str, hub_signer: hub_jwks.HubSigner, monkeypatch) -> Iterator[tuple[str, str]]:
    """The hub-filled ``worker:team`` row at the node's URL, its run token in the cache; the URL and the token."""
    registry.reset()
    handshake.reset_handshake()
    token = hub_signer.token(uid=USER_ID, aud=NODE_NAME)
    monkeypatch.setattr(node_tokens, "_tokens", {(USER_ID, NODE_ID): (token, time.time() + 3600)})
    with get_db_context() as db:
        db.add(ConfiguredBackend(id=NODE, url=node_worker, hub_node_id=NODE_ID, hub_user_id=USER_ID))
        db.commit()
    try:
        yield node_worker, token
    finally:
        with get_db_context() as db:
            row = db.get(ConfiguredBackend, NODE)
            if row is not None:
                db.delete(row)
                db.commit()
        registry.reset()
        handshake.reset_handshake()


@pytest.fixture
def sent(node_row, monkeypatch) -> Sent:
    """Records each HTTP request and socket core sends the node, through the real transport."""
    from flowfile_core.flowfile.flow_data_engine.subprocess_operations import streaming

    url, _ = node_row
    recorder = Sent(url)
    real_send = requests.Session.send
    real_connect = streaming.connect

    def send(self, request, **kwargs):
        response = real_send(self, request, **kwargs)
        if request.url.startswith(url):
            path = request.url[len(url) :]
            recorder.calls.append((path, request.headers.get("X-Flowfile-Internal"), response.status_code))
        return response

    def connect(uri, **kwargs):
        recorder.calls.append((uri, kwargs["additional_headers"].get("X-Flowfile-Internal"), 101))
        return real_connect(uri, **kwargs)

    monkeypatch.setattr(requests.Session, "send", send)
    monkeypatch.setattr(streaming, "connect", connect)
    return recorder


def test_the_node_refuses_cores_own_token(node_row):
    url, token = node_row

    refused = requests.get(f"{url}/status/nothing", headers={"X-Flowfile-Internal": get_internal_token()}, timeout=5)
    taken = requests.get(f"{url}/status/nothing", headers={"X-Flowfile-Internal": token}, timeout=5)

    assert refused.status_code == 401
    assert taken.status_code != 401


def test_a_flow_placed_on_the_node_runs_with_its_run_token_on_every_call(protocol, node_row, sent):
    _, token = node_row
    graph = input_then_sort(make_graph(9831, execution_location=NODE), ranks(3), "rank")
    try:
        run_info = graph.run_graph()

        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        sort = graph.get_node(2)
        assert [row["rank"] for row in sort.get_table_example(True).data] == [1, 2, 3]
        assert sort.results.result_ref.backend_id == NODE
    finally:
        graph.close_flow()

    assert sent.calls, "the run reached the node"
    assert sent.tokens() == {token}
    assert sent.refused() == []
    if not protocol:
        assert any(path.startswith("/submit_query/") or path.endswith("/ws/submit") for path, _, _ in sent.calls)


@pytest.fixture
def clean_catalog():
    catalog_cleanup()
    yield
    catalog_cleanup()


def test_a_catalog_writer_placed_on_the_node_writes_its_table_with_the_run_token(
    protocol, node_row, sent, clean_catalog
):
    from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository

    _, token = node_row
    namespace_id = create_test_namespace()
    graph = create_test_graph(flow_id=9832, execution_location=NODE)
    add_test_manual_input(graph, [{"id": 1, "name": "ada"}, {"id": 2, "name": "grace"}], node_id=1)
    add_test_catalog_writer(graph, node_id=2, depending_on_id=1, table_name="written", namespace_id=namespace_id)
    try:
        run_info = graph.run_graph()

        assert run_info.success is True, [result.error for result in run_info.node_step_result]
    finally:
        graph.close_flow()

    with get_db_context() as db:
        (table,) = SQLAlchemyCatalogRepository(db).list_tables(namespace_id=namespace_id)
    assert table.name == "written"
    assert sorted(pl.read_delta(table.file_path).rows()) == [(1, "ada"), (2, "grace")]
    assert any(path.startswith("/submit_query/") or path.endswith("/ws/submit") for path, _, _ in sent.calls)
    assert sent.tokens() == {token}
    assert sent.refused() == []
