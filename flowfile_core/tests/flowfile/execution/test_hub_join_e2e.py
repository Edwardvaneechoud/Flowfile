"""A worker that joins a hub with one join token runs a desktop's node there, end to end.

An operator makes a worker a node on a hub with one command, ``flowfile_worker --join <token> --hub <url>``: before it
serves, the worker posts the token with its name and address to the hub, keeps the hub's answer in ``hub_node.json``
in its storage dir, and from then on takes the hub's tokens for that name, with no name or issuer set by hand. The
join and the verifier each have their own tests in one process; here the worker is a second process started with the
flags an operator types, on a free port and a storage dir of its own, against a fake hub that answers the join and
serves its keys from one address. A node a desktop places on the joined worker runs there with a ``run`` token for
the joined name, held as a connected desktop holds it; a restart, with the same command or with none, does not join
again; a used or an expired join token stops the worker with the hub's sentence before it serves and leaves no file;
a worker named by hand through ``FLOWFILE_HUB_URL`` and ``FLOWFILE_NODE_NAME`` behaves as before. Each test skips
when no second worker can be started.
"""

import json
import os
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
import requests

from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.hub_client import node_tokens
from shared import hub_node
from shared.compute_protocol import PROTOCOL_VERSION, PROTOCOL_VERSION_HEADER
from shared.tests.hub_join import EXPIRED, JOIN_PATH, USED, FakeJoinHub
from shared.tests.hub_jwks import JWKS_PATH, HubSigner
from tests.conftest import _free_port
from tests.flowfile.execution.graph_helpers import input_then_sort, make_graph, ranks
from tests.flowfile.execution.test_hub_node_tokens_e2e import NODE_ID, STARTUP_TIMEOUT, USER_ID, _stop, prime

pytestmark = pytest.mark.usefixtures("protocol_on", "running_worker")

JOINED = "joined"
ROW_ID = f"worker:{JOINED}"
HUB_VARIABLES = ("FLOWFILE_HUB_URL", "FLOWFILE_NODE_NAME", "FLOWFILE_HUB_ISSUER", "FLOWFILE_HUB_JOIN_TOKEN")


@pytest.fixture
def fake_hub() -> Iterator[FakeJoinHub]:
    """A hub at one address: it answers joins and serves the keys of the signer it names as its issuer."""
    signer = HubSigner()
    hub = FakeJoinHub(signer)
    try:
        yield hub
    finally:
        hub.close()
        signer.close()


def url_of(port: int) -> str:
    return f"http://127.0.0.1:{port}"


def log_of(storage_dir: Path) -> Path:
    """Where ``start_worker`` keeps the output of the worker it last started on ``storage_dir``: a file beside it."""
    return storage_dir.with_name(f"{storage_dir.name}.log")


def output(storage_dir: Path) -> str:
    return log_of(storage_dir).read_text()


def answers(port: int) -> bool:
    try:
        return requests.get(f"{url_of(port)}/docs", timeout=2).ok
    except requests.RequestException:
        return False


def start_worker(storage_dir: Path, *args: str, port: int, env: dict[str, str] | None = None) -> subprocess.Popen:
    """``python -m flowfile_worker --port <port> *args`` on ``storage_dir``, as an operator starts one; the process.

    The environment is the session worker's, with ``FLOWFILE_STORAGE_DIR`` set to ``storage_dir`` and none of the hub
    variables unless ``env`` sets them. Returns once the worker serves ``/docs``, or once it exits, whichever comes
    first within the startup timeout (a refused join exits); a worker that does neither is stopped and the test
    skipped.
    """
    environment = {name: value for name, value in os.environ.items() if name not in HUB_VARIABLES}
    environment.update({"FLOWFILE_WORKER_LOCAL_FILES": "1", "FLOWFILE_STORAGE_DIR": str(storage_dir), **(env or {})})
    with log_of(storage_dir).open("w") as log:
        proc = subprocess.Popen(
            [sys.executable, "-m", "flowfile_worker", "--port", str(port), *args],
            stdout=log,
            stderr=subprocess.STDOUT,
            env=environment,
            preexec_fn=os.setsid if hasattr(os, "setsid") else None,
        )
    deadline = time.monotonic() + STARTUP_TIMEOUT
    while time.monotonic() < deadline:
        if proc.poll() is not None or answers(port):
            return proc
        time.sleep(0.5)
    _stop(proc)
    pytest.skip(f"a second flowfile_worker did not answer on {url_of(port)}:\n{output(storage_dir)[-2000:]}")


def join_command(token: str, hub_url: str, port: int) -> tuple[str, ...]:
    """The flags that join the hub at ``hub_url`` as ``joined``, reached at the worker's own address."""
    return ("--join", token, "--hub", hub_url, "--name", JOINED, "--url", url_of(port))


def joins(hub: FakeJoinHub) -> list[dict | None]:
    return [body for path, body in hub.calls if path == JOIN_PATH]


def capabilities(port: int, token: str) -> requests.Response:
    """The worker's ``GET /v1/capabilities`` answer to a desktop signing with ``token``."""
    headers = {"X-Flowfile-Internal": token, PROTOCOL_VERSION_HEADER: str(PROTOCOL_VERSION)}
    return requests.get(f"{url_of(port)}/v1/capabilities", headers=headers, timeout=10)


def spent(hub: FakeJoinHub) -> str:
    """A join token another node has joined with already."""
    token = hub.mint()
    body = {"token": token, "name": "first", "worker_url": "http://127.0.0.1:1", "core_url": None}
    assert requests.post(f"{hub.url}{JOIN_PATH}", json=body, timeout=10).status_code == 200
    return token


@pytest.fixture
def joined_row(monkeypatch) -> Iterator[int]:
    """The ``worker:joined`` row a connected desktop holds for the node, hub-filled and token-less; its port.

    The row names a free port the test starts the joined worker on.
    """
    port = _free_port()
    registry.reset()
    handshake.reset_handshake()
    monkeypatch.setattr(node_tokens, "_tokens", {})
    with get_db_context() as db:
        db.add(ConfiguredBackend(id=ROW_ID, url=url_of(port), hub_node_id=NODE_ID, hub_user_id=USER_ID))
        db.commit()
    try:
        yield port
    finally:
        with get_db_context() as db:
            row = db.get(ConfiguredBackend, ROW_ID)
            if row is not None:
                db.delete(row)
                db.commit()
        registry.reset()
        handshake.reset_handshake()


def test_a_worker_joins_with_one_command_and_runs_a_desktops_node(fake_hub, joined_row, tmp_path):
    storage, port = tmp_path / "node", joined_row
    token = fake_hub.mint()
    proc = start_worker(storage, *join_command(token, fake_hub.url, port), port=port)
    graph = None
    try:
        assert proc.poll() is None, output(storage)
        kept = (storage / hub_node.FILE_NAME).read_text()
        assert token not in kept
        node = json.loads(kept)
        assert (node["hub_url"], node["issuer"], node["name"]) == (fake_hub.url, fake_hub.issuer, JOINED)
        assert joins(fake_hub) == [{"token": token, "name": JOINED, "worker_url": url_of(port), "core_url": None}]

        assert capabilities(port, fake_hub.signer.token(uid=USER_ID, aud=JOINED)).status_code == 200
        refused = capabilities(port, fake_hub.signer.token(uid=USER_ID, aud="another"))
        assert refused.status_code == 401 and "wrong audience" in refused.json()["detail"]
        assert (JWKS_PATH, None) in fake_hub.calls, "the worker fetched the keys from the hub it joined"

        run_token = fake_hub.signer.token(uid=USER_ID, aud=JOINED)
        prime(run_token)
        graph = input_then_sort(make_graph(9811, execution_location=ROW_ID), ranks(3), "rank")
        run_info = graph.run_graph()

        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        sort = graph.get_node(2)
        assert [row["rank"] for row in sort.get_table_example(True).data] == [1, 2, 3]
        assert sort.results.result_ref.backend_id == ROW_ID
        backend = registry.backend(ROW_ID)
        assert isinstance(backend, WorkerBackend)
        assert (backend.url, backend.token) == (url_of(port), run_token)
    finally:
        if graph is not None:
            graph.close_flow()
        _stop(proc)


def test_a_restart_does_not_join_again(fake_hub, tmp_path):
    storage, port = tmp_path / "node", _free_port()
    command = join_command(fake_hub.mint(), fake_hub.url, port)
    run_token = fake_hub.signer.token(uid=USER_ID, aud=JOINED)
    for start, args in enumerate((command, command, ())):
        proc = start_worker(storage, *args, port=port)
        try:
            assert proc.poll() is None, output(storage)
            assert len(joins(fake_hub)) == 1
            if start == 1:
                assert f"Already joined {fake_hub.url} as {JOINED}; the join token is not used" in output(storage)
            assert capabilities(port, run_token).status_code == 200
        finally:
            _stop(proc)


@pytest.mark.parametrize(
    ("token_from", "sentence"),
    [pytest.param(spent, USED, id="used"), pytest.param(lambda hub: hub.mint(expired=True), EXPIRED, id="expired")],
)
def test_a_used_or_expired_token_stops_the_worker(fake_hub, tmp_path, token_from, sentence):
    storage, port = tmp_path / "node", _free_port()
    token = token_from(fake_hub)
    proc = start_worker(storage, "--join", token, "--hub", fake_hub.url, "--name", JOINED, port=port)
    try:
        printed = output(storage)
        assert proc.returncode == 2, printed
        assert f"Could not join {fake_hub.url}: {sentence}" in printed
        assert token not in printed
        assert not (storage / hub_node.FILE_NAME).exists()
        with pytest.raises(requests.ConnectionError):
            requests.get(f"{url_of(port)}/docs", timeout=2)
    finally:
        _stop(proc)


def test_a_worker_named_by_hand_works_as_before(fake_hub, tmp_path):
    storage, port = tmp_path / "node", _free_port()
    by_hand = {"FLOWFILE_HUB_URL": fake_hub.url, "FLOWFILE_NODE_NAME": "by-hand"}
    proc = start_worker(storage, port=port, env=by_hand)
    try:
        assert proc.poll() is None, output(storage)
        assert joins(fake_hub) == []
        assert not (storage / hub_node.FILE_NAME).exists()
        # Named by hand, the node expects the hub's own URL as the issuer, as before.
        token = fake_hub.signer.token(uid=USER_ID, aud="by-hand", iss=fake_hub.url)
        assert capabilities(port, token).status_code == 200
    finally:
        _stop(proc)
