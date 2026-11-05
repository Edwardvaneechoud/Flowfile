"""On a server joined to its hub with keys of its own, a run on one of the hub's servers carries the run token minted
for the person who runs it, end to end.

Core runs in process as the joined server ``team-1`` (docker mode, its own master key, joined with keys under
``tmp_path``), and two people sign in to it through the hub. The hub is T45's ``OrganisationHub`` that also answers
``POST /hub/node-tokens``: it signs a ``run`` token for the person whose delegated access token asked, with the key a
worker verifies against, and records whom each token it minted is for. The hub's server ``team`` is a second worker
started by the test on a free port above 64980 with an internal token core does not hold, so a run token is its one way
in. The hub-filled row ``worker:team`` belongs to the first person, who synced the hub's servers; each person's run is
signed with their own token all the same, the first person's sign-out leaves the other's runs as they were, and a run
for nobody, or for the person who signed out, is refused before anything reaches the worker. The module skips when no
second worker can be started.
"""

import json
import os
import secrets
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator

import jwt
import pytest
import requests
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend, FlowRun
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.hub_client import node_tokens
from flowfile_core.routes import routes
from shared.tests.hub_jwks import HubSigner
from tests import test_hub_sign_in
from tests.conftest import protocol_states
from tests.flowfile.execution.graph_helpers import add_manual_input, add_sort_on, ranks
from tests.flowfile.execution.test_hub_node_tokens_e2e import STARTUP_TIMEOUT, _stop
from tests.hub.test_server_run_report import OrganisationHub
from tests.test_hub_sign_in import NODE, bearer, join

node_file = test_hub_sign_in.node_file
protocol = protocol_states()
client = TestClient(main.app)

SERVER = "team"
BACKEND = f"worker:{SERVER}"
NODE_ID = 5
ADA, DEE = 7, 8
REFUSED = (
    f"You are not signed in through the hub, so its server {SERVER!r} cannot run this flow for you; "
    "sign in through the hub again."
)


def run_refused(*steps: str) -> str:
    """The sentence a run or fetch for a person with no sign-in through the hub is refused with, naming ``steps``."""
    named = " and ".join(f'"{step}"' for step in steps)
    return (
        f"You are not signed in through the hub, so its server {SERVER!r} cannot run the steps {named} for you; "
        "sign in through the hub again, or run them on this server (set their Compute backend under General "
        "Settings)."
    )


RUN_REFUSED = run_refused("Manual input 1", "Sort data 2")


class MintingHub(OrganisationHub):
    """``OrganisationHub`` that answers ``POST /hub/node-tokens`` for the server ``team`` as the hub does: a ``run``
    token for the person whose access token asked, signed with the key the worker verifies against.

    ``minted`` keeps the subject (the person's username) of each token it minted, in order.
    """

    def __init__(self, signer: HubSigner):
        super().__init__(signer)
        self.minted: list[str] = []
        hub = self

        class Handler(self._server.RequestHandlerClass):
            def do_POST(self):
                if self.path != "/hub/node-tokens":
                    super().do_POST()
                    return
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length)
                self._answer(*hub.node_token(self.headers.get("Authorization"), body))

        self._server.RequestHandlerClass = Handler

    def node_token(self, authorization: str | None, body: bytes) -> tuple[int, dict]:
        scheme, _, token = (authorization or "").partition(" ")
        with self._lock:
            uid = self.issued.get(token) if scheme == "Bearer" else None
        if uid is None:
            return 401, {"detail": "Could not validate credentials"}
        if json.loads(body).get("node_id") != NODE_ID:
            return 404, {"detail": "no such compute node"}
        subject = self.people[uid]["username"]
        minted = self.signer.token(sub=subject, uid=uid, aud=SERVER, ttl=3600)
        with self._lock:
            self.minted.append(subject)
        return 201, {
            "token": minted,
            "node_id": NODE_ID,
            "node_name": SERVER,
            "scope": "run",
            "kid": self.signer.key.kid,
            "issued_at": "2026-11-05T12:00:00Z",
            "expires_at": "2026-11-05T13:00:00Z",
            "issuer": self.issuer,
        }


def _port_above(floor: int) -> int:
    for port in range(floor + 1, 65500):
        with socket.socket() as probe:
            try:
                probe.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    raise RuntimeError(f"no free port above {floor}")


@pytest.fixture
def signer() -> Iterator[HubSigner]:
    signer = HubSigner()
    try:
        yield signer
    finally:
        signer.close()


@pytest.fixture
def server(signer: HubSigner) -> Iterator[str]:
    """The hub's server ``team``: a worker that takes the hub's ``run`` tokens and an internal token of its own."""
    port = _port_above(64980)
    env = {
        **os.environ,
        "FLOWFILE_WORKER_LOCAL_FILES": "1",
        "FLOWFILE_HUB_URL": signer.url,
        "FLOWFILE_NODE_NAME": SERVER,
        "FLOWFILE_INTERNAL_TOKEN": secrets.token_urlsafe(24),
    }
    env.pop("FLOWFILE_MODE", None)
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
def hub(node_file, signer, server, monkeypatch) -> Iterator[MintingHub]:
    """A ``MintingHub`` this server joined with its own keys, enabled; its people's accounts removed afterwards."""
    monkeypatch.setenv("FLOWFILE_MASTER_KEY", Fernet.generate_key().decode())
    monkeypatch.setattr(node_tokens, "_tokens", {})
    fake = MintingHub(signer)
    join(fake)
    registry.reset()
    handshake.reset_handshake()
    try:
        yield fake
    finally:
        fake.close()
        registry.reset()
        handshake.reset_handshake()
        with get_db_context() as db:
            row = db.get(ConfiguredBackend, BACKEND)
            if row is not None:
                db.delete(row)
            db.query(FlowRun).delete()
            mappings = db.query(db_models.IssuerAccount).filter(db_models.IssuerAccount.issuer == fake.issuer)
            user_ids = [mapping.user_id for mapping in mappings]
            mappings.delete(synchronize_session=False)
            db.query(db_models.User).filter(db_models.User.id.in_(user_ids)).delete(synchronize_session=False)
            db.commit()


def sign_in(hub: MintingHub, uid: int, name: str) -> tuple[int, str]:
    """The hub's person ``uid`` signs in through the hub as ``name``: their account here and core's access token."""
    signed_in = client.post("/auth/hub/signin", json={"token": hub.sign_in_token(NODE, uid=uid, username=name)})
    assert signed_in.status_code == 200, signed_in.text
    access_token = signed_in.json()["access_token"]
    me = client.get("/auth/users/me", headers=bearer(access_token))
    assert me.status_code == 200, me.text
    return me.json()["id"], access_token


@pytest.fixture
def people(hub, server) -> dict[str, tuple[int, str]]:
    """``ada`` and ``dee`` signed in through the hub, and the hub-filled ``worker:team`` row ``ada``'s sync left."""
    suffix = secrets.token_hex(4)
    signed_in = {"ada": sign_in(hub, ADA, f"ada-{suffix}"), "dee": sign_in(hub, DEE, f"dee-{suffix}")}
    with get_db_context() as db:
        db.add(ConfiguredBackend(id=BACKEND, url=server, hub_node_id=NODE_ID, hub_user_id=signed_in["ada"][0]))
        db.commit()
    return signed_in


@pytest.fixture
def subjects(server, monkeypatch) -> list[str | None]:
    """The subject of the token on every HTTP request and socket core sends the server, ``None`` for a request
    without one or with one that is no hub token; a request the server refused is recorded as ``"refused"``."""
    from flowfile_core.flowfile.execution.backends import worker as worker_backend
    from flowfile_core.flowfile.flow_data_engine.subprocess_operations import streaming

    seen: list[str | None] = []
    real_send = requests.Session.send
    real_connect = streaming.connect
    socket_url = server.replace("http://", "ws://")

    def subject(token: str | None) -> str | None:
        try:
            return jwt.decode(token or "", options={"verify_signature": False}).get("sub")
        except jwt.PyJWTError:
            return None

    def send(self, request, **kwargs):
        response = real_send(self, request, **kwargs)
        if request.url.startswith(server):
            refused = response.status_code in (401, 403)
            seen.append("refused" if refused else subject(request.headers.get("X-Flowfile-Internal")))
        return response

    def connect(uri, **kwargs):
        if uri.startswith(socket_url):
            seen.append(subject(kwargs["additional_headers"].get("X-Flowfile-Internal")))
        return real_connect(uri, **kwargs)

    monkeypatch.setattr(requests.Session, "send", send)
    monkeypatch.setattr(streaming, "connect", connect)
    monkeypatch.setattr(worker_backend, "connect", connect)
    return seen


@pytest.fixture
def open_flow(tmp_path):
    """Opens a flow in a person's session, manual input → sort in Development mode, so each run stores its results on
    the hub's server ``team``."""
    opened = []

    def open_one(user_id: int, name: str):
        flow_id = routes.flow_file_handler.add_flow(
            name=name, flow_path=str(tmp_path / f"{name}.yaml"), user_id=user_id
        )
        opened.append(flow_id)
        flow = routes.flow_file_handler.get_flow(flow_id)
        flow.flow_settings.execution_location = BACKEND
        flow.flow_settings.execution_mode = "Development"
        add_manual_input(flow, 1, ranks(3))
        add_sort_on(flow, 2, 1, "rank")
        return flow

    yield open_one
    for flow_id in opened:
        routes.flow_file_handler.delete_flow(flow_id)


def run(flow, access_token: str):
    ran = client.post("/flow/run/", params={"flow_id": flow.flow_id}, headers=bearer(access_token))
    assert ran.status_code == 200, ran.text
    return flow.get_run_info()


def sorted_ranks(flow) -> list[int]:
    return [row["rank"] for row in flow.get_node(2).get_table_example(True).data]


def errors(run_info) -> list[tuple[int, str | None]]:
    return [(result.node_id, result.error) for result in run_info.node_step_result if not result.success]


def test_each_persons_run_on_the_hubs_server_carries_their_own_token(protocol, hub, people, subjects, open_flow):
    (ada, ada_token), (dee, dee_token) = people["ada"], people["dee"]
    ada_name, dee_name = (hub.people[uid]["username"] for uid in (ADA, DEE))

    ada_run = run(open_flow(ada, "adas_flow"), ada_token)
    ada_saw = list(subjects)
    subjects.clear()
    dee_flow = open_flow(dee, "dees_flow")
    dee_run = run(dee_flow, dee_token)
    dee_saw = list(subjects)

    assert ada_run.success is True, errors(ada_run)
    assert dee_run.success is True, errors(dee_run)
    assert ada_saw and set(ada_saw) == {ada_name}
    assert dee_saw and set(dee_saw) == {dee_name}
    assert hub.minted == [ada_name, dee_name]
    assert sorted_ranks(dee_flow) == [1, 2, 3]


def test_one_persons_sign_out_leaves_the_others_runs_and_refuses_their_own_with_one_sentence(
    protocol, hub, people, subjects, open_flow
):
    (ada, ada_token), (dee, dee_token) = people["ada"], people["dee"]
    dee_name = hub.people[DEE]["username"]
    ada_flow, dee_flow = open_flow(ada, "adas_flow"), open_flow(dee, "dees_flow")
    assert run(ada_flow, ada_token).success is True
    assert client.post("/auth/logout", headers=bearer(ada_token)).status_code == 204
    subjects.clear()

    dee_run = run(dee_flow, dee_token)
    dee_saw = list(subjects)
    subjects.clear()
    refused = run(ada_flow, ada_token)

    assert dee_run.success is True, errors(dee_run)
    assert dee_saw and set(dee_saw) == {dee_name}
    assert refused.success is False
    assert errors(refused) == [(1, RUN_REFUSED), (2, RUN_REFUSED)]
    assert subjects == []
    assert hub.minted.count(hub.people[ADA]["username"]) == 1


def test_a_run_for_nobody_is_refused_and_never_takes_the_rows_person(protocol, hub, people, subjects, open_flow):
    ada, _ = people["ada"]
    flow = open_flow(ada, "scheduled")

    assert flow.try_claim_run()
    refused = flow.run_graph(claimed=True)

    assert refused.success is False
    assert errors(refused) == [(1, RUN_REFUSED), (2, RUN_REFUSED)]
    assert subjects == [] and hub.minted == []
