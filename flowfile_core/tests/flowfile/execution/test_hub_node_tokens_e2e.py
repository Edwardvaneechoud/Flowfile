"""A node placed on a worker that trusts a configured issuer runs there with a token from that issuer, end to end.

A worker started as a node on a hub (``FLOWFILE_HUB_URL``, ``FLOWFILE_NODE_NAME``) verifies the tokens the hub signs
against the hub's published keys, and a desktop connected to that hub holds a ``compute_backends`` row for the node
whose backend signs with the ``run`` token the hub minted for it. Each half has its own tests; here they meet in one
checkout, with no hub process: the test signs with its own key and serves the JWKS from a thread, as the verifier's
tests do, starts a second worker with those settings on a free port the way the session worker is started, fills the
row and primes the desktop's token cache with a token of its own signing, and runs a small flow placed on
``worker:team``. With a token the worker finds expired the run fails naming the worker's 401 and its reason, and
nothing runs anywhere else. The module skips when no second worker can be started.
"""

import os
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator

import pytest
import requests

from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.hub_client import node_tokens
from shared.tests import hub_jwks
from shared.tests.hub_jwks import NODE_NAME
from tests.conftest import _free_port
from tests.flowfile.execution.graph_helpers import input_then_sort, make_graph, ranks

pytestmark = pytest.mark.usefixtures("protocol_on")
hub_signer = hub_jwks.hub_signer

TEAM = f"worker:{NODE_NAME}"
USER_ID, NODE_ID = 41, 3
STARTUP_TIMEOUT = int(os.environ.get("FLOWFILE_STARTUP_TIMEOUT", 30))
SHUTDOWN_TIMEOUT = int(os.environ.get("FLOWFILE_SHUTDOWN_TIMEOUT", 15))


def _stop(proc: subprocess.Popen) -> None:
    """End the worker's process group as the session worker is ended: SIGTERM, then SIGKILL after the timeout."""
    if proc.poll() is not None:
        return
    group = hasattr(os, "killpg")
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM) if group else proc.terminate()
        try:
            proc.wait(timeout=SHUTDOWN_TIMEOUT)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL) if group else proc.kill()
            proc.wait(timeout=5)
    except (ProcessLookupError, OSError):
        pass


@pytest.fixture
def team_worker(hub_signer: hub_jwks.HubSigner, running_worker) -> Iterator[str]:
    """A second worker, a node named ``team`` on the test's issuer, on a free port; its URL.

    Started as the session worker is, the same program on another port, with the issuer's URL and the node's name in
    its environment; skipped when it does not answer within the startup timeout.
    """
    port = _free_port()
    env = {
        **os.environ,
        "FLOWFILE_WORKER_LOCAL_FILES": "1",
        "FLOWFILE_HUB_URL": hub_signer.url,
        "FLOWFILE_NODE_NAME": NODE_NAME,
    }
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
                why = f"exited with {proc.returncode}" if proc.poll() is not None else "did not answer"
                pytest.skip(f"a second flowfile_worker could not be started on {url} ({why}):\n{tail}")
            yield url
        finally:
            _stop(proc)


@pytest.fixture
def team_row(team_worker: str, monkeypatch) -> Iterator[str]:
    """The ``worker:team`` row a connected desktop holds for the node: hub-filled, token-less, at the worker's URL."""
    registry.reset()
    handshake.reset_handshake()
    monkeypatch.setattr(node_tokens, "_tokens", {})
    with get_db_context() as db:
        db.add(ConfiguredBackend(id=TEAM, url=team_worker, remote_id=NODE_ID, account_user_id=USER_ID))
        db.commit()
    try:
        yield team_worker
    finally:
        with get_db_context() as db:
            row = db.get(ConfiguredBackend, TEAM)
            if row is not None:
                db.delete(row)
                db.commit()
        registry.reset()
        handshake.reset_handshake()


def prime(token: str) -> None:
    """Hold ``token`` in the desktop's cache as a token with the hour ahead of it, as a mint through the hub would."""
    node_tokens._tokens[(USER_ID, NODE_ID)] = (token, time.time() + 3600)


def test_a_node_placed_on_the_worker_runs_there_with_the_issuers_token(hub_signer, team_row):
    token = hub_signer.token(uid=USER_ID, aud=NODE_NAME)
    prime(token)
    graph = input_then_sort(make_graph(9801, execution_location=TEAM), ranks(3), "rank")
    try:
        run_info = graph.run_graph()

        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        sort = graph.get_node(2)
        assert [row["rank"] for row in sort.get_table_example(True).data] == [1, 2, 3]
        assert sort.results.result_ref.backend_id == TEAM
        backend = registry.backend(TEAM)
        assert isinstance(backend, WorkerBackend)
        assert (backend.url, backend.token) == (team_row, token)
        assert hub_signer.fetches >= 1, "the worker verified the token against the issuer's published keys"
    finally:
        graph.close_flow()


def test_an_expired_token_in_the_cache_fails_the_run_with_the_workers_refusal(hub_signer, team_row):
    prime(hub_signer.token(uid=USER_ID, aud=NODE_NAME, ttl=-120))
    graph = input_then_sort(make_graph(9802, execution_location=TEAM), ranks(3), "rank")
    try:
        run_info = graph.run_graph()

        assert run_info.success is False
        rows = {result.node_id: result for result in run_info.node_step_result}
        assert rows[1].success is False and rows[1].compute_error == "TaskFailed", rows[1].error
        assert "The worker answered 401: Hub token refused: expired" in rows[1].error
        assert 2 not in rows or rows[2].success is not True
        assert graph.get_node(1).results.result_ref is None
        assert graph.get_node(2).results.resulting_data is None
    finally:
        graph.close_flow()
