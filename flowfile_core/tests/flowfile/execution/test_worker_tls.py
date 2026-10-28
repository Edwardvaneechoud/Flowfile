"""A worker serving TLS from a self-signed certificate, reached through a backend that names the certificate as its CA.

The worker runs on a port of its own with ``FLOWFILE_WORKER_TLS_CERT`` and ``FLOWFILE_WORKER_TLS_KEY``. A backend
made with ``ca_cert`` reads its capabilities over ``https`` and submits a plan over ``wss``; one made without it is
refused as unreachable before anything is sent. Core trusts a CA per worker URL, so the backend that names none reaches
the same worker by its other name, ``localhost``, which the certificate covers too. Every case runs under both states of
the protocol flag and starts with the handshake not yet run, so the gate's own request and its probe go over TLS too.
About one fresh ``wss`` open in a hundred to a uvicorn server over TLS stalls before the upgrade reaches the app, so the
submit case dials a stalled socket again rather than read the server stack's stall as core falling back to the routes.
"""

import io
import os
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest
import requests

from flowfile_core.auth.jwt import get_internal_token
from flowfile_core.flowfile.execution.backends import worker as worker_backend
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.errors import ComputeUnavailable
from flowfile_core.flowfile.execution.protocol import SubmitHints
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake, streaming
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import subprocess_operations as subops
from shared._version import get_version
from shared.compute_protocol import PROTOCOL_VERSION, PROTOCOL_VERSION_HEADER
from test_utils.tls import self_signed_certificate
from tests.conftest import _free_port, protocol_states

STARTUP_TIMEOUT = 120
SOCKET_DIALS = 3
HINTS = SubmitHints(must_materialise=True, wide=False, fanout=1, preview_rows=100)
THREE_ROWS = pl.LazyFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})


@dataclass(frozen=True)
class TlsWorker:
    """The worker's URL by its address, the same worker by its other name, and its certificate as PEM and as a file."""

    url: str
    url_by_name: str
    pem: str
    cert_path: Path


@pytest.fixture(scope="module")
def tls_worker(tmp_path_factory) -> Iterator[TlsWorker]:
    """A worker of this checkout serving TLS on a free port, signing with this process's internal token."""
    folder = tmp_path_factory.mktemp("worker_tls")
    pem, key = self_signed_certificate("flowfile-worker-tls", dns=["localhost"], ips=["127.0.0.1"])
    cert_path, key_path, log_path = folder / "worker.crt", folder / "worker.key", folder / "worker.log"
    cert_path.write_text(pem)
    key_path.write_text(key)
    port = _free_port()
    env = {
        **os.environ,
        "FLOWFILE_WORKER_TLS_CERT": str(cert_path),
        "FLOWFILE_WORKER_TLS_KEY": str(key_path),
        "FLOWFILE_WORKER_LOCAL_FILES": "1",
    }
    with log_path.open("w") as log:
        process = subprocess.Popen(
            [sys.executable, "-m", "flowfile_worker", "--port", str(port)], env=env, stdout=log, stderr=log
        )
    url = f"https://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + STARTUP_TIMEOUT
        while True:
            if process.poll() is not None:
                pytest.fail(f"The TLS worker exited with {process.returncode}:\n{log_path.read_text()[-4000:]}")
            try:
                if requests.get(f"{url}/docs", verify=str(cert_path), timeout=5).ok:
                    break
            except requests.RequestException:
                pass
            if time.monotonic() > deadline:
                pytest.fail(f"The TLS worker did not answer within {STARTUP_TIMEOUT}s:\n{log_path.read_text()[-4000:]}")
            time.sleep(0.5)
        yield TlsWorker(url=url, url_by_name=f"https://localhost:{port}", pem=pem, cert_path=cert_path)
    finally:
        # uvicorn waits on the session's pooled connection before it exits
        subops.trust_ca(url, None)
        process.terminate()
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


flag = protocol_states()


@pytest.fixture(autouse=True)
def unchecked_worker() -> Iterator[None]:
    """Each test makes the process's first worker contact, so the gate's request and probe reach the TLS worker."""
    handshake.reset_handshake()
    yield
    handshake.reset_handshake()


@pytest.fixture
def no_rest_fallback(monkeypatch, flag) -> None:
    """Fail any plan that would be posted instead of sent over a socket, so a submit that passes went over ``wss``."""

    def refuse_task_post(*args, **kwargs):
        raise AssertionError("the plan was posted to the task routes, not sent over the socket")

    monkeypatch.setattr(subops, "trigger_df_operation", refuse_task_post)
    if flag:
        real_v1 = WorkerBackend._v1

        def refuse_v1_post(self, method, path, **kwargs):
            if (method, path) == ("POST", "/submit"):
                raise AssertionError("the plan was posted to /v1/submit, not sent over the socket")
            return real_v1(self, method, path, **kwargs)

        monkeypatch.setattr(WorkerBackend, "_v1", refuse_v1_post)


@pytest.fixture
def redialled_socket(monkeypatch, tls_worker) -> None:
    """Dial a socket whose opening handshake stalled again, and fail one that does not carry the CA the backend names.

    The server stack, not core, stalls about one fresh ``wss`` open in a hundred: the TLS handshake completes, the
    upgrade never reaches the app and the client's ``open_timeout`` raises ``TimeoutError``. Nothing was sent on such a
    socket, so a second dial cannot run a plan twice. Both the transport's socket and the ``/v1`` one, the handshake's
    probes included, go through it, so none of them takes the REST route the case refuses.
    """

    def wrap(real_connect):
        def dial(uri, *args, **kwargs):
            assert kwargs.get("ssl") is not None
            assert kwargs["ssl"] is subops.ssl_context_for(tls_worker.url), f"{uri} was dialled without the trusted CA"
            for attempt in range(1, SOCKET_DIALS + 1):
                try:
                    return real_connect(uri, *args, **kwargs)
                except TimeoutError:
                    if attempt == SOCKET_DIALS:
                        raise

        return dial

    monkeypatch.setattr(worker_backend, "connect", wrap(worker_backend.connect))
    monkeypatch.setattr(streaming, "connect", wrap(streaming.connect))


def trusted(tls_worker: TlsWorker) -> WorkerBackend:
    return WorkerBackend(tls_worker.url, backend_id="worker:tls", token=get_internal_token(), ca_cert=tls_worker.pem)


def held_anywhere(tls_worker: TlsWorker, result_id: str) -> list[str]:
    """Which of the worker's routes still hold ``result_id``, asked with the certificate as the trust anchor."""
    headers = {"X-Flowfile-Internal": get_internal_token(), PROTOCOL_VERSION_HEADER: str(PROTOCOL_VERSION)}
    verify = str(tls_worker.cert_path)
    held = []
    if requests.get(f"{tls_worker.url}/status/{result_id}", headers=headers, verify=verify, timeout=10).ok:
        held.append("task routes")
    if requests.head(f"{tls_worker.url}/v1/results/{result_id}", headers=headers, verify=verify, timeout=10).ok:
        held.append("/v1")
    return held


def test_a_backend_naming_the_ca_reads_the_workers_capabilities_over_https(tls_worker, flag):
    capabilities = trusted(tls_worker).capabilities()

    assert capabilities.protocol_version == PROTOCOL_VERSION
    assert capabilities.app_version == get_version()
    assert handshake.fetch_worker_capabilities(tls_worker.url) == capabilities


def test_a_backend_naming_the_ca_submits_over_wss(tls_worker, flag, no_rest_fallback, redialled_socket):
    backend = trusted(tls_worker)

    result = backend.submit(THREE_ROWS, [], HINTS, f"t-worker-tls-{uuid.uuid4().hex}", flow_id=-1, node_id=-1)
    try:
        assert pa.ipc.open_stream(io.BytesIO(result.sample)).read_all().num_rows == 3
        assert backend.exists(result.ref) is True
    finally:
        backend.release(result.ref)
    assert handshake.last_verdict("worker:tls").ok is True


def test_a_backend_not_naming_the_ca_is_refused_before_anything_is_sent(tls_worker, flag):
    backend = WorkerBackend(tls_worker.url_by_name, backend_id="worker:untrusted", token=get_internal_token())
    result_id = f"t-worker-tls-{uuid.uuid4().hex}"

    with pytest.raises(ComputeUnavailable) as read:
        backend.capabilities()
    with pytest.raises(ComputeUnavailable) as submitted:
        backend.submit(THREE_ROWS, [], HINTS, result_id, flow_id=-1, node_id=-1)

    assert read.value.backend_id == "worker:untrusted"
    assert submitted.value.backend_id == "worker:untrusted"
    for refusal in (read.value, submitted.value):
        assert tls_worker.url_by_name in str(refusal)
        assert "CERTIFICATE_VERIFY_FAILED" in str(refusal)
    assert subops.ssl_context_for(tls_worker.url_by_name) is None
    assert handshake.last_verdict("worker:untrusted").ok is False
    assert held_anywhere(tls_worker, result_id) == []
