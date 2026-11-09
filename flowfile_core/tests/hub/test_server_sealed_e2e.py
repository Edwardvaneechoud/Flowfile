"""A server joined to its hub with keys of its own runs steps on a compute-only server of the hub, end to end.

Core runs here as the joined server ``team-1`` (docker mode, its own master key, joined with keys under ``tmp_path``,
T45's fixtures), and its worker is the one embedded in this process (``local``), which opens with core's node key.
The compute-only server is a second ``flowfile_worker`` started with the hub's card command, ``--hub <hub> --join
<token>``, a home of its own and no ``FLOWFILE_*`` variable, so it joins with keys of its own and has its own master
key and storage dir: no ``$ffsec$`` ciphertext or stored connection of core's means anything there. The hub is a
thread-served fake implementing the vend's contract with targets: it signs each credential with its node-token key
and seals it to every target's ``enc`` key from its join, and it sets the organisation's exchange store. Data is on
the suite's MinIO; the module skips without it, or when the second worker cannot start.

Done when a catalog writer placed on the compute-only server writes its hub table through an input that crossed to it
through the hub's exchange store, and a flow whose steps cross both ways between the two servers runs, with core
never sending a secret to the compute-only server.
"""

import socket
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator

import polars as pl
import pytest
import requests
from cryptography.fernet import Fernet

from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.hub_client import node_tokens
from flowfile_core.schemas import input_schema, transform_schema
from shared.tests.hub_jwks import HubSigner
from tests import test_hub_sign_in
from tests.flowfile.conftest import add_test_manual_input, catalog_cleanup
from tests.flowfile.execution.graph_helpers import add_promise, connect
from tests.flowfile.execution.test_hub_card_e2e import shell_environment
from tests.flowfile.execution.test_hub_node_tokens_e2e import STARTUP_TIMEOUT, _stop
from tests.flowfile.execution.test_hub_tables_e2e import EU, MINIO_OPTIONS, ORDERS
from tests.hub.test_server_run_report import OrganisationHub, client, open_flow, sign_in  # noqa: F401
from tests.hub_client.conftest import SealingHubCatalog
from tests.test_catalog_cloud_virtual import _BUCKET, get_minio_client, requires_minio
from tests.test_hub_sign_in import bearer, join

node_file = test_hub_sign_in.node_file
username = test_hub_sign_in.username

pytestmark = [requires_minio, pytest.mark.usefixtures("protocol_on")]

SERVER = "worker"
BACKEND = f"worker:{SERVER}"
SECRET = MINIO_OPTIONS["aws_secret_access_key"].encode()


class SealingHub(OrganisationHub):
    """``OrganisationHub`` whose catalog seals what it vends to the servers a run names, with the ``enc`` key each sent
    in its join, and that answers ``GET /hub/exchange-store``."""

    def __init__(self, signer: HubSigner):
        super().__init__(signer)
        self.catalog = SealingCatalog(self, signer)
        self.catalog._bearer_user = self._person

    @staticmethod
    def holds(path: str) -> bool:
        return OrganisationHub.holds(path) or path.split("?", 1)[0] == "/hub/exchange-store"


class SealingCatalog(SealingHubCatalog):
    def __init__(self, hub: SealingHub, signer: HubSigner):
        super().__init__(signer, issuer=hub.issuer)
        self.hub = hub

    def _vend(self, body: dict):
        with self.hub._lock:
            nodes = dict(self.hub._nodes)
        self.enc_keys = {name: node["keys"]["enc"] for name, node in nodes.items() if node.get("keys")}
        return super()._vend(body)


def _port_above(floor: int) -> int:
    for port in range(floor + 1, floor + 200):
        with socket.socket() as probe:
            try:
                probe.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    raise RuntimeError(f"no free port above {floor}")


@pytest.fixture(autouse=True)
def clean_catalog():
    catalog_cleanup()
    yield
    with get_db_context() as db:
        db.query(db_models.FlowRun).delete()
        db.commit()
    catalog_cleanup()


@pytest.fixture
def hub(node_file, monkeypatch) -> Iterator[SealingHub]:
    """A ``SealingHub`` this server joined as ``team-1`` with its own keys; its people's accounts removed after."""
    monkeypatch.setenv("FLOWFILE_MASTER_KEY", Fernet.generate_key().decode())
    signer = HubSigner()
    fake = SealingHub(signer)
    join(fake)
    try:
        yield fake
    finally:
        fake.close()
        signer.close()
        with get_db_context() as db:
            mappings = db.query(db_models.IssuerAccount).filter(db_models.IssuerAccount.issuer == fake.issuer)
            user_ids = [row.user_id for row in mappings]
            mappings.delete(synchronize_session=False)
            db.query(db_models.User).filter(db_models.User.id.in_(user_ids)).delete(synchronize_session=False)
            db.commit()


@pytest.fixture
def compute_server(hub, tmp_path) -> Iterator[str]:
    """The compute-only server ``worker``: the card's command with a home of its own, on a free port above 64980."""
    port = _port_above(64980)
    home = tmp_path / "compute-home"
    home.mkdir()
    url = f"http://127.0.0.1:{port}"
    log = tmp_path / "compute-server.log"
    command = [
        sys.executable,
        "-m",
        "flowfile_worker",
        *("--host", "127.0.0.1", "--port", str(port), "--name", SERVER, "--url", url),
        *("--core-host", "127.0.0.1", "--core-port", str(_port_above(port))),
        *("--hub", hub.url, "--join", hub.mint(enable_on_join=True)),
    ]
    with log.open("w") as out:
        proc = subprocess.Popen(
            command,
            stdout=out,
            stderr=subprocess.STDOUT,
            env=shell_environment(home),
            cwd=tmp_path,
            start_new_session=True,
        )
    try:
        deadline = time.monotonic() + STARTUP_TIMEOUT
        while time.monotonic() < deadline and proc.poll() is None:
            try:
                if requests.get(f"{url}/health", timeout=2).ok:
                    break
            except requests.RequestException:
                time.sleep(0.5)
        else:
            _stop(proc)
            pytest.skip(f"the compute-only server could not be started on {url}:\n{log.read_text()[-2000:]}")
        yield url
    finally:
        _stop(proc)


@pytest.fixture
def store(hub) -> Iterator[str]:
    """The hub's catalog on MinIO, its schema ``sales`` with the Delta table ``orders``, and the organisation's
    exchange store under the same root; the prefix is emptied afterwards."""
    root = f"hub_{uuid.uuid4().hex[:8]}"
    storage_uri = f"s3://{_BUCKET}/{root}"
    catalog = hub.catalog.seed_catalog(root, storage_uri=storage_uri)
    sales = hub.catalog.seed_schema(catalog, "sales")
    hub.catalog.grants.update({catalog["id"]: "manage", sales["id"]: "manage"})
    hub.catalog.vend_options[storage_uri] = dict(MINIO_OPTIONS)
    exchange = f"{storage_uri}/exchange"
    hub.catalog.vend_options[exchange] = dict(MINIO_OPTIONS)
    hub.catalog.exchange_store = {
        "uri": exchange,
        "connection": "org-exchange",
        "max_rows": None,
        "set_by": 1,
        "set_at": "2026-11-04T12:00:00Z",
    }
    frame = pl.DataFrame(ORDERS)
    frame.write_delta(f"{storage_uri}/orders", storage_options=MINIO_OPTIONS)
    columns = [{"name": c, "dtype": str(t)} for c, t in frame.schema.items()]
    orders = hub.catalog.seed_table(
        sales, "orders", file_path=f"{storage_uri}/orders", is_remote_storage=True, schema_columns=columns
    )
    hub.sales, hub.orders = sales, orders
    try:
        yield root
    finally:
        minio = get_minio_client()
        for listed in minio.list_objects_v2(Bucket=_BUCKET, Prefix=f"{root}/").get("Contents", []):
            minio.delete_object(Bucket=_BUCKET, Key=listed["Key"])


@pytest.fixture
def person(hub, store, compute_server, username, monkeypatch) -> Iterator[tuple[int, str]]:
    """A person signed in through the hub, and the hub-filled ``worker:worker`` row of theirs at the server's URL with
    the hub's run token for it; their account here and core's access token."""
    person_id, access_token = sign_in(hub, username)
    node_id = hub._node_ids[SERVER]
    token = hub.signer.token(uid=person_id, aud=SERVER, iss=hub.issuer)
    monkeypatch.setattr(node_tokens, "_tokens", {(person_id, node_id): (token, time.time() + 3600)})
    registry.reset()
    handshake.reset_handshake()
    with get_db_context() as db:
        db.add(ConfiguredBackend(id=BACKEND, url=compute_server, hub_node_id=node_id, hub_user_id=person_id))
        db.commit()
    try:
        yield person_id, access_token
    finally:
        with get_db_context() as db:
            row = db.get(ConfiguredBackend, BACKEND)
            if row is not None:
                db.delete(row)
                db.commit()
        registry.reset()
        handshake.reset_handshake()


@pytest.fixture
def sent(compute_server, monkeypatch) -> list[tuple[str, bytes]]:
    """The route and body of every HTTP request, and each frame of every socket, core sends the compute-only server."""
    from flowfile_core.flowfile.execution.backends import worker as worker_backend
    from flowfile_core.flowfile.flow_data_engine.subprocess_operations import streaming

    recorded: list[tuple[str, bytes]] = []
    real_send = requests.Session.send
    real_connect = streaming.connect
    socket_url = compute_server.replace("http://", "ws://")

    def send(self, request, **kwargs):
        if request.url.startswith(compute_server):
            body = request.body or b""
            recorded.append((request.url[len(compute_server) :], body if isinstance(body, bytes) else body.encode()))
        return real_send(self, request, **kwargs)

    def connect(uri, **kwargs):
        connection = real_connect(uri, **kwargs)
        if uri.startswith(socket_url):
            real_frame = connection.send

            def frame(message, *args, **frame_kwargs):
                data = message if isinstance(message, bytes) else str(message).encode()
                recorded.append((uri[len(socket_url) :], data))
                return real_frame(message, *args, **frame_kwargs)

            connection.send = frame
        return connection

    monkeypatch.setattr(requests.Session, "send", send)
    monkeypatch.setattr(streaming, "connect", connect)
    monkeypatch.setattr(worker_backend, "connect", connect)
    return recorded


def on(flow, node_id: int, backend: str) -> None:
    flow.get_node(node_id).setting_input.execution_location = backend


def sales_schema(hub) -> str:
    """The qualified name of the hub's ``sales`` schema, as a writer step names it (``<catalog>.sales``)."""
    return hub.orders["qualified_name"].rpartition(".")[0]


def add_writer(flow, node_id: int, depending_on: int, person: int, schema: str, name: str) -> None:
    """A writer of ``name`` into the hub schema ``schema`` (its qualified name), set by ``person``."""
    add_promise(flow, node_id, "catalog_writer")
    flow.add_catalog_writer(
        input_schema.NodeCatalogWriter(
            flow_id=flow.flow_id,
            node_id=node_id,
            depending_on_id=depending_on,
            user_id=person,
            catalog_write_settings=input_schema.CatalogWriteSettings(
                table_name=name,
                source="hub",
                namespace_full_name=schema,
                write_mode="overwrite",
            ),
        )
    )
    connect(flow, depending_on, node_id)


def run(flow, access_token: str):
    ran = client.post("/flow/run/", params={"flow_id": flow.flow_id}, headers=bearer(access_token))
    assert ran.status_code == 200, ran.text
    run_info = flow.get_run_info()
    assert run_info.success is True, [(r.node_id, r.error) for r in run_info.node_step_result]
    return run_info


def written(hub, name: str) -> pl.DataFrame:
    [registered] = [body for body in hub.catalog.registered if body["name"] == name]
    assert registered["namespace_id"] == hub.sales["id"]
    return pl.scan_delta(registered["table_path"], storage_options=MINIO_OPTIONS).collect().sort("id")


def exchange_objects(store: str) -> list[str]:
    listed = get_minio_client().list_objects_v2(Bucket=_BUCKET, Prefix=f"{store}/exchange/")
    return [item["Key"] for item in listed.get("Contents", [])]


def test_a_catalog_writer_on_the_compute_only_server_writes_its_table_through_the_hubs_exchange_store(
    hub, store, person, sent, open_flow
):
    person_id, access_token = person
    flow = open_flow(person_id, "write_on_the_compute_server")
    add_test_manual_input(flow, [{"id": 1, "name": "ada"}, {"id": 2, "name": "grace"}], node_id=1)
    add_writer(flow, 2, 1, person_id, sales_schema(hub), "people")
    on(flow, 2, BACKEND)

    run_info = run(flow, access_token)

    assert (1, 2) in [tuple(boundary) for boundary in run_info.boundaries]
    assert written(hub, "people").rows() == [(1, "ada"), (2, "grace")]
    [vend] = hub.catalog.vend_calls
    assert (vend["exchange"], sorted(vend["targets"])) == (True, sorted({SERVER, test_hub_sign_in.NODE}))
    assert hub.sent_with("GET", "/hub/exchange-store") and hub.sent_with("POST", "/hub/vend")
    assert sent and not [route for route, body in sent if SECRET in body or b"$ffsec$" in body]
    assert any(route.endswith("/import") and b'"sealed"' in body for route, body in sent)
    assert any(route.endswith("/ws/submit") and b'"sealed"' in body for route, body in sent)
    assert exchange_objects(store) == []


def test_a_flow_whose_steps_cross_both_ways_between_the_servers_runs_through_the_hubs_exchange_store(
    hub, store, person, sent, open_flow
):
    person_id, access_token = person
    flow = open_flow(person_id, "cross_both_ways")
    add_promise(flow, 1, "catalog_reader")
    flow.add_catalog_reader(
        input_schema.NodeCatalogReader(
            flow_id=flow.flow_id,
            node_id=1,
            user_id=person_id,
            source="hub",
            catalog_full_table_name=hub.orders["qualified_name"],
            catalog_table_name="orders",
        )
    )
    add_promise(flow, 2, "filter")
    flow.add_filter(
        input_schema.NodeFilter(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(
                mode="basic",
                basic_filter=transform_schema.BasicFilter(field="country", operator="in", value="NL, DE"),
            ),
        )
    )
    connect(flow, 1, 2)
    add_writer(flow, 3, 2, person_id, sales_schema(hub), "orders_eu")
    on(flow, 1, BACKEND)
    on(flow, 3, BACKEND)

    run_info = run(flow, access_token)

    assert {tuple(boundary) for boundary in run_info.boundaries} == {(1, 2), (2, 3)}
    assert flow.get_node(2).results.result_ref.backend_id == "local"
    assert written(hub, "orders_eu").equals(pl.DataFrame(ORDERS).filter(pl.col("country").is_in(EU)).sort("id"))
    [vend] = hub.catalog.vend_calls
    assert vend["exchange"] is True and set(vend["targets"]) == {SERVER, test_hub_sign_in.NODE}
    assert not [route for route, body in sent if SECRET in body or b"$ffsec$" in body]
    assert exchange_objects(store) == []


def test_a_crossing_to_a_backend_the_hub_has_no_server_for_is_refused_before_the_run(hub, store, person, open_flow):
    person_id, access_token = person
    with get_db_context() as db:
        db.add(ConfiguredBackend(id="worker:by-hand", url="http://127.0.0.1:9", token=None))
        db.commit()
    try:
        flow = open_flow(person_id, "to_a_server_of_no_hub")
        add_test_manual_input(flow, [{"id": 1, "name": "ada"}], node_id=1)
        add_writer(flow, 2, 1, person_id, sales_schema(hub), "nowhere")
        on(flow, 2, "worker:by-hand")

        ran = client.post("/flow/run/", params={"flow_id": flow.flow_id}, headers=bearer(access_token))

        assert ran.status_code == 200, ran.text
        errors = [result.error for result in flow.get_run_info().node_step_result if not result.success]
        assert errors and all('backend "worker:by-hand" is not one of the hub\'s servers' in error for error in errors)
        assert hub.catalog.vend_calls == [] and hub.catalog.registered == []
    finally:
        with get_db_context() as db:
            db.delete(db.get(ConfiguredBackend, "worker:by-hand"))
            db.commit()


def test_a_server_whose_hub_sets_no_store_still_refuses_the_crossing_with_t49s_sentence(hub, store, person, open_flow):
    person_id, access_token = person
    hub.catalog.exchange_store = None
    flow = open_flow(person_id, "no_store_on_the_hub")
    add_test_manual_input(flow, [{"id": 1, "name": "ada"}], node_id=1)
    add_writer(flow, 2, 1, person_id, sales_schema(hub), "nowhere")
    on(flow, 2, BACKEND)

    client.post("/flow/run/", params={"flow_id": flow.flow_id}, headers=bearer(access_token))

    errors = [result.error for result in flow.get_run_info().node_step_result if not result.success]
    assert errors and "no exchange store can carry its data between backends" in errors[0]
    assert hub.catalog.registered == []
