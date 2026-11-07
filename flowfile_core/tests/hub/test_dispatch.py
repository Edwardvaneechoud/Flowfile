"""A server joined to its hub with keys of its own runs a flow the hub sends it (T132): ``POST /hub/dispatch`` takes the
hub's ``dispatch`` token, signs the person in here when they are not, checks the version out as them, runs it and
reports it to the hub under the hub's run id, failed or not.

The hub is the run report walk's fake (``tests/hub/test_server_run_report.OrganisationHub``) with two more things the
real hub has: ``GET /hub/flows/{uuid}/checkout`` answers a seeded document, and its token exchange takes a ``dispatch``
token as a sign-in, as ``POST /hub/token`` does on the hub. Core runs in process in docker mode, as a server does, its
``hub_node.json``, its keys and its secure store under the test's ``tmp_path``; each dispatch runs in its own thread
on the worker embedded here, and a test waits for those threads before it reads the report.
"""

import re
import threading
import time
import uuid

import jwt
import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.configs.flow_logger import get_flow_log_file
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.hub_client import signin
from flowfile_core.schemas import input_schema, transform_schema
from shared.run_logs import run_log_path
from shared.tests.hub_join import TOKEN_EXCHANGE, TOKEN_PATH
from shared.tests.hub_jwks import HubSigner
from tests import test_hub_sign_in
from tests.flowfile.conftest import CATALOG_SAMPLE_DATA, add_test_manual_input, create_test_graph
from tests.flowfile.execution.graph_helpers import add_promise, connect
from tests.hub.test_server_run_report import OrganisationHub
from tests.test_hub_sign_in import NODE, join

node_file = test_hub_sign_in.node_file
username = test_hub_sign_in.username

client = TestClient(main.app)
UID = 7
VERSION = 3
CHECKOUT = re.compile(r"^/hub/flows/([^/?]+)/checkout(\?.*)?$")
NOT_A_SIGN_IN = "this is not a sign-in token for this node"


class DispatchHub(OrganisationHub):
    """``OrganisationHub`` that also holds published flows and takes a ``dispatch`` token as a sign-in.

    ``flows`` maps a flow uuid to what its checkout answers; ``checkouts`` keeps each checkout as ``(uuid, version,
    bearer)``; ``refuse_checkout`` set makes every checkout a 403 with that sentence; ``traded`` keeps the run id of
    each dispatch token the exchange took as a sign-in.
    """

    def __init__(self, signer: HubSigner):
        super().__init__(signer)
        self.flows: dict[str, dict] = {}
        self.checkouts: list[tuple[str, int | None, str]] = []
        self.refuse_checkout: str | None = None
        self.traded: list[str] = []
        hub = self

        class Handler(self._server.RequestHandlerClass):
            def do_GET(self):
                match = CHECKOUT.match(self.path)
                if match is None:
                    super().do_GET()
                    return
                hub.calls.append((self.path, None))
                _, _, bearer = (self.headers.get("Authorization") or "").partition(" ")
                self._answer(*hub.checkout(match.group(1), self.path, bearer))

        self._server.RequestHandlerClass = Handler

    def seed(self, flow_uuid: str, name: str, definition: dict) -> None:
        self.flows[flow_uuid] = {"flow_uuid": flow_uuid, "version": VERSION, "name": name, "definition": definition}

    def checkout(self, flow_uuid: str, path: str, bearer: str) -> tuple[int, dict]:
        wanted = re.search(r"version=(\d+)", path)
        version = int(wanted.group(1)) if wanted else None
        with self._lock:
            self.checkouts.append((flow_uuid, version, bearer))
            known = bearer in self.issued
        if not known:
            return 401, {"detail": "this access token was not issued by the hub"}
        if self.refuse_checkout:
            return 403, {"detail": self.refuse_checkout}
        flow = self.flows.get(flow_uuid)
        if flow is None or (version is not None and version != flow["version"]):
            return 404, {"detail": "no published version"}
        return 200, {**flow, "registration_id": 7, "namespace_id": 2}

    def _sign_in(self, node: str, form: dict) -> tuple[str | None, dict | None]:
        refusal, delegation = super()._sign_in(node, form)
        if refusal != NOT_A_SIGN_IN:
            return refusal, delegation
        public_key = self.signer.key.private.public_key()
        claims = jwt.decode(form["subject_token"], public_key, algorithms=["ES256"], audience=node, issuer=self.issuer)
        if claims["scope"] != "dispatch" or claims.get("may_act") != {"sub": f"node:{node}"}:
            return refusal, None
        with self._lock:
            if claims["jti"] in self._spent_sign_ins:
                return "this dispatch token has been used", None
            self._spent_sign_ins.add(claims["jti"])
            self.traded.append(claims["run_id"])
        person = {"uid": claims["uid"], "username": claims["sub"], "full_name": None, "node_admin": False}
        self.people[claims["uid"]] = person
        return None, {"node": node, "uid": claims["uid"], "person": person, "signed_in_at": time.time()}


@pytest.fixture
def hub(node_file, monkeypatch):
    """A ``DispatchHub`` this server joined with its own keys, enabled; the accounts its people got are removed
    afterwards. The master key is the server's own, as docker mode reads it. A checked-out document opens as ``auto``
    (the protocol's rule for a file saved ``local``), so the process's default backend is pinned to the worker embedded
    here: the suite starts none beside it under ``SKIP_WORKER_TESTS``."""
    monkeypatch.setenv("FLOWFILE_MASTER_KEY", Fernet.generate_key().decode())
    monkeypatch.setenv("FLOWFILE_COMPUTE_BACKEND", "local")
    signer = HubSigner()
    fake = DispatchHub(signer)
    join(fake)
    try:
        yield fake
    finally:
        wait_for_runs()
        fake.close()
        signer.close()
        with get_db_context() as db:
            mappings = db.query(db_models.IssuerAccount).filter(db_models.IssuerAccount.issuer == fake.issuer)
            user_ids = [row.user_id for row in mappings]
            mappings.delete(synchronize_session=False)
            db.query(db_models.User).filter(db_models.User.id.in_(user_ids)).delete(synchronize_session=False)
            db.commit()


def definition(failing: bool = False) -> dict:
    """A one-node flow over the sample rows, run on the worker embedded here; ``failing`` adds a Polars code node that
    selects a column the rows do not have."""
    graph = create_test_graph(flow_id=99, execution_location="local")
    add_test_manual_input(graph, CATALOG_SAMPLE_DATA, node_id=1)
    if failing:
        add_promise(graph, 2, "polars_code")
        code = transform_schema.PolarsCodeInput(polars_code='output_df = input_df.select(pl.col("nope"))')
        graph.add_polars_code(
            input_schema.NodePolarsCode(flow_id=graph.flow_id, node_id=2, depending_on_ids=[1], polars_code_input=code)
        )
        connect(graph, 1, 2)
    return graph.get_flowfile_data().model_dump(mode="json")


def dispatch_token(hub: DispatchHub, name: str, *, run_id: str | None = None, flow_uuid: str | None = None, **changes):
    """A dispatch token as the hub mints one for this server: scope ``dispatch``, the run id, the flow and version;
    ``changes`` add or override claims (``drop`` and ``key`` reach the signer)."""
    claims = {
        "scope": "dispatch",
        "sub": name,
        "uid": UID,
        "aud": NODE,
        "ttl": 600,
        "may_act": {"sub": f"node:{NODE}"},
        "run_id": run_id or uuid.uuid4().hex,
        "flow_uuid": flow_uuid or str(uuid.uuid4()),
        "flow_version": VERSION,
    }
    claims.update(changes)
    return hub.signer.token(**claims)


def wait_for_runs(timeout: float = 60.0) -> None:
    for thread in threading.enumerate():
        if thread.name.startswith("hub-dispatch-"):
            thread.join(timeout=timeout)


def dispatch(token: str):
    answered = client.post("/hub/dispatch", json={"token": token})
    wait_for_runs()
    return answered


def exchanges(hub: DispatchHub) -> list[str]:
    return [form["grant_type"] for path, form in list(hub.calls) if path == TOKEN_PATH]


def seeded(hub: DispatchHub, name: str, failing: bool = False) -> tuple[str, str, str]:
    """A flow on the hub and a dispatch token for a run of it: ``(flow uuid, run id, token)``."""
    flow_uuid, run_id = str(uuid.uuid4()), uuid.uuid4().hex
    hub.seed(flow_uuid, "demo-flow", definition(failing))
    return flow_uuid, run_id, dispatch_token(hub, name, run_id=run_id, flow_uuid=flow_uuid)


def test_a_good_token_signs_the_person_in_checks_the_version_out_as_them_runs_it_and_reports_under_the_hubs_run_id(
    hub, username
):
    flow_uuid, run_id, token = seeded(hub, username)

    answered = dispatch(token)

    assert (answered.status_code, answered.json()) == (202, {"run_id": run_id})
    assert exchanges(hub) == [TOKEN_EXCHANGE] and hub.traded == [run_id]
    [delegated] = hub.issued
    assert hub.issued[delegated] == UID
    assert hub.checkouts == [(flow_uuid, VERSION, delegated)]
    assert hub.sent_with("POST", "/hub/runs") == [delegated]
    [posted] = hub.catalog.reports
    assert (posted["run_id"], posted["flow_uuid"], posted["flow_version"]) == (run_id, flow_uuid, VERSION)
    assert posted["success"] is True, [node.get("error") for node in posted["nodes"]]
    assert [(node["node_type"], node["success"]) for node in posted["nodes"]] == [("manual_input", True)]
    with get_db_context() as db:
        account = signin.account(db, UID)
        assert account is not None and account.username == username and account.is_admin is False
        [row] = db.query(db_models.FlowRun).filter_by(flow_uuid=flow_uuid).all()
        assert (row.run_type, row.registration_id, row.flow_name, row.user_id) == ("hub", None, "demo-flow", account.id)
        assert (row.success, row.nodes_completed, row.number_of_nodes) == (True, 1, 1)
        assert row.ended_at is not None and run_log_path(row.id).exists()
    assert (get_flow_log_file(1).parent / f"hub_run_{run_id}.log").exists()


def test_a_person_signed_in_here_already_keeps_that_sign_in_and_the_token_is_traded_for_nothing(hub, username):
    signed_in = client.post("/auth/hub/signin", json={"token": hub.sign_in_token(NODE, uid=UID, username=username)})
    assert signed_in.status_code == 200, signed_in.text
    [delegated] = hub.issued
    flow_uuid, run_id, token = seeded(hub, username)

    assert dispatch(token).status_code == 202

    assert exchanges(hub) == [TOKEN_EXCHANGE] and hub.traded == []
    assert list(hub.issued) == [delegated]
    assert hub.checkouts == [(flow_uuid, VERSION, delegated)]
    assert hub.sent_with("POST", "/hub/runs") == [delegated]
    [posted] = hub.catalog.reports
    assert (posted["run_id"], posted["success"]) == (run_id, True)


def test_the_same_token_twice_runs_once_and_the_second_is_409(hub, username):
    flow_uuid, run_id, token = seeded(hub, username)
    assert dispatch(token).status_code == 202

    again = dispatch(token)

    assert again.status_code == 409 and run_id in again.json()["detail"]
    assert len(hub.catalog.reports) == 1 and len(hub.checkouts) == 1


@pytest.mark.parametrize(
    "changes, reason",
    [
        ({"aud": "chuwi", "may_act": {"sub": "node:chuwi"}}, "wrong audience"),
        ({"ttl": -3600}, "expired"),
        ({"scope": "run"}, "a run token is not accepted here"),
        ({"drop": ("run_id",)}, "missing claim run_id"),
        ({"flow_version": "three"}, "malformed claim flow_version"),
    ],
    ids=["another server", "expired", "a run token", "no run id", "a version that is no number"],
)
def test_a_token_that_does_not_hold_is_401_with_the_reason_and_runs_nothing(hub, username, changes, reason):
    flow_uuid = str(uuid.uuid4())
    hub.seed(flow_uuid, "demo-flow", definition())

    answered = dispatch(dispatch_token(hub, username, flow_uuid=flow_uuid, **changes))

    assert answered.status_code == 401, answered.text
    assert answered.json()["detail"] == f"The hub's dispatch token was refused: {reason}."
    assert hub.checkouts == [] and hub.catalog.reports == [] and exchanges(hub) == []


def test_a_string_that_is_no_jwt_is_401_and_a_token_signed_by_another_key_too(hub, username):
    answered = dispatch("nope")
    assert (answered.status_code, answered.json()["detail"]) == (401, "This is not a dispatch token from the hub.")

    other = HubSigner()
    try:
        forged = dispatch_token(hub, username, key=other.key)
    finally:
        other.close()
    answered = dispatch(forged)
    assert answered.status_code == 401 and "unknown key" in answered.json()["detail"]
    assert hub.checkouts == [] and hub.catalog.reports == []


def test_a_flow_that_fails_is_reported_failed_under_the_hubs_run_id(hub, username):
    flow_uuid, run_id, token = seeded(hub, username, failing=True)

    assert dispatch(token).status_code == 202

    [posted] = hub.catalog.reports
    assert (posted["run_id"], posted["flow_uuid"], posted["success"]) == (run_id, flow_uuid, False)
    failed = [node for node in posted["nodes"] if not node["success"]]
    assert [node["node_type"] for node in failed] == ["polars_code"]
    assert "nope" in failed[0]["error"]
    with get_db_context() as db:
        [row] = db.query(db_models.FlowRun).filter_by(flow_uuid=flow_uuid).all()
        assert (row.run_type, row.success, row.ended_at is not None) == ("hub", False, True)


def test_a_checkout_the_hub_refuses_is_reported_failed_with_no_nodes_so_the_hub_does_not_wait(hub, username):
    hub.refuse_checkout = "You can't read this flow"
    flow_uuid, run_id, token = seeded(hub, username)

    assert dispatch(token).status_code == 202

    assert hub.checkouts[0][:2] == (flow_uuid, VERSION)
    [posted] = hub.catalog.reports
    assert (posted["run_id"], posted["flow_uuid"], posted["flow_version"]) == (run_id, flow_uuid, VERSION)
    assert (posted["success"], posted["nodes"]) == (False, [])
    with get_db_context() as db:
        assert db.query(db_models.FlowRun).filter_by(flow_uuid=flow_uuid).count() == 0


def test_off_a_server_not_joined_with_keys_the_route_is_not_there(node_file):
    answered = client.post("/hub/dispatch", json={"token": "a.b.c"})
    assert answered.status_code == 404, answered.text
    assert "not joined to a hub with keys" in answered.json()["detail"]
