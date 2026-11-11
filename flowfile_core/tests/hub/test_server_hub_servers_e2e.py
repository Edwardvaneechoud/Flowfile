"""On a server joined to its hub with keys of its own, the hub's servers become backends with each person's own sign-in
through the hub, end to end: nobody syncs on the Compute page.

Core runs in process as the joined server ``team-1`` (docker mode, its own master key), with no ``compute_backends``
row. The hub is the run-token test's ``MintingHub`` that also lists, for each person, the servers it lets them run
on (``GET /hub/compute-nodes``). Its server ``team`` is a second worker started by the test, which takes only the
hub's run tokens. A member's first sign-in lists ``team`` as a backend and a run on it works, with the compute
protocol flag off or on; with it on (where a run is placed) a server the hub no longer lists leaves the backends at the
next run, and a server only another person may use stays while they are signed in and goes once they are not. The
module skips when no second worker can be started.
"""

import secrets
from collections.abc import Iterator

import pytest
from cryptography.fernet import Fernet

from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend, FlowRun
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.hub_client import node_tokens
from tests import test_hub_sign_in
from tests.conftest import protocol_states
from tests.hub.test_server_run_tokens_e2e import (  # noqa: F401
    BACKEND,
    NODE_ID,
    MintingHub,
    client,
    errors,
    open_flow,
    run,
    server,
    signer,
    sorted_ranks,
)
from tests.hub_client.conftest import compute_node
from tests.test_hub_sign_in import NODE, bearer, join

node_file = test_hub_sign_in.node_file
protocol = protocol_states()

ADA, DEE = 17, 18
SALES = "worker:sales"
SALES_ID = 6


class ListingHub(MintingHub):
    """``MintingHub`` that answers ``GET /hub/compute-nodes`` for an access token it issued: the person's own list in
    ``lists`` when it holds one, else ``nodes``; ``listed`` keeps whose list each answer was."""

    def __init__(self, signer):
        super().__init__(signer)
        self.nodes: list[dict] = []
        self.lists: dict[int, list[dict]] = {}
        self.listed: list[int] = []
        hub = self

        class Handler(self._server.RequestHandlerClass):
            def do_GET(self):
                if self.path != "/hub/compute-nodes":
                    super().do_GET()
                    return
                self._answer(*hub.compute_nodes(self.headers.get("Authorization")))

        self._server.RequestHandlerClass = Handler

    def compute_nodes(self, authorization: str | None) -> tuple[int, object]:
        scheme, _, token = (authorization or "").partition(" ")
        with self._lock:
            uid = self.issued.get(token) if scheme == "Bearer" else None
            if uid is None:
                return 401, {"detail": "Could not validate credentials"}
            self.listed.append(uid)
            return 200, self.lists.get(uid, self.nodes)


@pytest.fixture
def hub(node_file, signer, server, monkeypatch) -> Iterator[ListingHub]:  # noqa: F811
    """A ``ListingHub`` this server joined with its own keys, listing ``team`` at the test's worker; no row here."""
    monkeypatch.setenv("FLOWFILE_MASTER_KEY", Fernet.generate_key().decode())
    monkeypatch.setattr(node_tokens, "_tokens", {})
    fake = ListingHub(signer)
    fake.nodes = [compute_node(NODE_ID, "team", worker_url=server)]
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
            db.query(ConfiguredBackend).delete()
            db.query(FlowRun).delete()
            mappings = db.query(db_models.IssuerAccount).filter(db_models.IssuerAccount.issuer == fake.issuer)
            user_ids = [mapping.user_id for mapping in mappings]
            mappings.delete(synchronize_session=False)
            db.query(db_models.User).filter(db_models.User.id.in_(user_ids)).delete(synchronize_session=False)
            db.commit()


def sign_in(hub: ListingHub, uid: int, name: str, *, node_admin: bool = False) -> tuple[int, str]:
    token = hub.sign_in_token(NODE, uid=uid, username=f"{name}-{secrets.token_hex(4)}", node_admin=node_admin)
    signed_in = client.post("/auth/hub/signin", json={"token": token})
    assert signed_in.status_code == 200, signed_in.text
    access_token = signed_in.json()["access_token"]
    me = client.get("/auth/users/me", headers=bearer(access_token))
    assert me.status_code == 200, me.text
    return me.json()["id"], access_token


def backends(access_token: str) -> list[dict]:
    listed = client.get("/compute_backends", headers=bearer(access_token))
    assert listed.status_code == 200, listed.text
    return listed.json()


def owners() -> dict[str, int | None]:
    with get_db_context() as db:
        return {row.id: row.account_user_id for row in db.query(ConfiguredBackend).all()}


def test_a_members_first_sign_in_lists_the_hubs_servers_and_a_run_on_one_works(protocol, hub, open_flow):
    assert owners() == {}

    dee, dee_token = sign_in(hub, DEE, "dee")

    [listed] = backends(dee_token)
    assert (listed["id"], listed["source"], listed["url"]) == (BACKEND, "remote", None)
    assert owners() == {BACKEND: dee}
    assert hub.listed == [DEE, DEE]
    flow = open_flow(dee, "dees_flow")
    ran = run(flow, dee_token)
    assert ran.success is True, errors(ran)
    assert sorted_ranks(flow) == [1, 2, 3]
    assert hub.minted == [hub.people[DEE]["username"]]
    assert hub.listed == ([DEE] * 3 if protocol else [DEE, DEE])


def test_a_server_removed_on_the_hub_leaves_the_backends_at_the_next_run(protocol_on, hub, open_flow):
    dee, dee_token = sign_in(hub, DEE, "dee")
    flow = open_flow(dee, "dees_flow")
    assert [backend["id"] for backend in backends(dee_token)] == [BACKEND]
    minted = list(hub.minted)

    hub.nodes = []
    ran = run(flow, dee_token)

    assert ran.success is False
    assert all(f"Unknown compute backend {BACKEND!r}" in error for _, error in errors(ran)), errors(ran)
    assert backends(dee_token) == []
    assert hub.minted == minted


def test_a_server_only_another_person_may_use_stays_while_they_are_signed_in(protocol_on, hub, open_flow, server):
    hub.lists[ADA] = [*hub.nodes, compute_node(SALES_ID, "sales", worker_url=server + "/sales")]
    ada, ada_token = sign_in(hub, ADA, "ada", node_admin=True)
    assert owners() == {BACKEND: ada, SALES: ada}

    dee, dee_token = sign_in(hub, DEE, "dee")
    assert owners() == {BACKEND: ada, SALES: ada}
    assert client.post("/auth/logout", headers=bearer(ada_token)).status_code == 204
    flow = open_flow(dee, "dees_flow")
    ran = run(flow, dee_token)

    assert ran.success is True, errors(ran)
    assert owners() == {BACKEND: dee}
    assert hub.minted == [hub.people[DEE]["username"]]
