"""A desktop connected to a hub runs a step on a hub table on a compute-only server of the hub, end to end.

Core runs here as a desktop: electron mode, no ``hub_node.json``, a secure store of the test's own holding the suite's
keys and the person's hub session. It never joined the hub, so it shares its master key with no server of the hub. The
compute-only server is a second ``flowfile_worker`` started with the hub's card command, ``--hub <hub> --join
<token>``, a home of its own and no ``FLOWFILE_*`` variable, so it joins with keys of its own (the shape of the joined
server's end-to-end test, whose fakes and fixtures this module takes). The hub is a thread-served fake implementing the
vend's contract with targets; it seals each credential to every target's ``enc`` key from its join. Data is on the
suite's MinIO; the module skips without it, or when the second worker cannot start.

Done when a hub table read on the compute-only server is written back to the hub by a step there with the credential
the hub sealed to it, core never sending it a secret; when a hub that seals nothing refuses the run before it starts,
each sealed step failing with the hub's own sentence (the rule's item 4: the hub's refusal is the hub's to word); when a
run with one hub-table step on that server and one on this computer runs both, the second with an unsealed vend of its
own (``RunVend.own``); and when a step on the desktop's own worker keeps today's vend.
"""

# ruff: noqa: F811

import time
import uuid
from collections.abc import Iterator

import polars as pl
import pytest

from flowfile_core.auth import secrets as core_secrets
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.placement import PYTHON_FUNCTIONS
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.hub_client import client as hub_client
from flowfile_core.hub_client import node_tokens, session
from flowfile_core.schemas import input_schema
from shared import hub_node, issuer_tokens
from shared.tests import plan_stamps
from shared.tests.hub_jwks import HubSigner
from tests.flowfile.conftest import catalog_cleanup
from tests.flowfile.execution.graph_helpers import add_promise
from tests.flowfile.execution.test_hub_tables_e2e import MINIO_OPTIONS, ORDERS
from tests.hub.test_server_run_report import client, open_flow  # noqa: F401
from tests.hub.test_server_sealed_e2e import (  # noqa: F401
    SECRET,
    SERVER,
    SealingHub,
    add_writer,
    compute_server,
    on,
    sales_schema,
    sent,
    written,
)
from tests.hub_client.conftest import FakeHubWithCatalog
from tests.test_catalog_cloud_virtual import _BUCKET, get_minio_client, requires_minio
from tests.test_hub_sign_in import bearer

pytestmark = [requires_minio, pytest.mark.usefixtures("protocol_on")]

BACKEND = f"worker:{SERVER}"
DESKTOP_USER = 1
UID = 7


@pytest.fixture(autouse=True)
def clean_catalog():
    catalog_cleanup()
    yield
    with get_db_context() as db:
        db.query(db_models.FlowRun).delete()
        db.commit()
    catalog_cleanup()


@pytest.fixture
def desktop(tmp_path, monkeypatch) -> str:
    """A desktop that never joined a hub: electron mode, no ``hub_node.json``, a secure store of the test's own holding
    the suite's keys; its own access token."""
    core_secrets.get_master_key()
    keys = core_secrets._storage._read_store("flowfile")
    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    monkeypatch.delenv("FLOWFILE_EXCHANGE_URI", raising=False)
    monkeypatch.setattr(hub_node, "path", lambda: tmp_path / "storage" / hub_node.FILE_NAME)
    monkeypatch.setattr(issuer_tokens, "_client", None)
    monkeypatch.setattr(hub_client, "_module_http", None)
    secure = tmp_path / "secure_storage"
    monkeypatch.setenv("FLOWFILE_SECURE_STORAGE_PATH", str(secure))
    monkeypatch.setenv("SECURE_STORAGE_PATH", str(secure))
    monkeypatch.setattr(core_secrets, "_storage", core_secrets.SecureStorage())
    monkeypatch.setattr(session, "_access_tokens", {})
    for name, value in keys.items():
        core_secrets.set_password("flowfile", name, value)
    assert hub_node.joined() is None
    return client.post("/auth/token").json()["access_token"]


@pytest.fixture
def hub(desktop) -> Iterator[SealingHub]:
    """A ``SealingHub`` the desktop's person ``UID`` is connected to, its catalog on MinIO with ``orders`` in ``sales``;
    the desktop never joins it. The catalog's objects are removed afterwards."""
    signer = HubSigner()
    fake = SealingHub(signer)
    root = f"hub_{uuid.uuid4().hex[:8]}"
    storage_uri = f"s3://{_BUCKET}/{root}"
    catalog = fake.catalog.seed_catalog(root, storage_uri=storage_uri)
    sales = fake.catalog.seed_schema(catalog, "sales")
    fake.catalog.grants.update({catalog["id"]: "manage", sales["id"]: "manage"})
    fake.catalog.vend_options[storage_uri] = dict(MINIO_OPTIONS)
    frame = pl.DataFrame(ORDERS)
    frame.write_delta(f"{storage_uri}/orders", storage_options=MINIO_OPTIONS)
    columns = [{"name": c, "dtype": str(t)} for c, t in frame.schema.items()]
    fake.orders = fake.catalog.seed_table(
        sales, "orders", file_path=f"{storage_uri}/orders", is_remote_storage=True, schema_columns=columns
    )
    fake.sales = sales
    access_token = f"hub-access-{uuid.uuid4().hex}"
    fake.people[UID] = {"uid": UID, "username": "ada", "full_name": "Ada", "node_admin": False}
    fake.issued[access_token] = UID
    session.save(DESKTOP_USER, {"hub_url": fake.url, "refresh_token": "hub-refresh-desktop", "device_name": "e2e"})
    session._access_tokens[DESKTOP_USER] = (access_token, time.time() + 3600)
    try:
        yield fake
    finally:
        session.clear(DESKTOP_USER)
        fake.close()
        signer.close()
        minio = get_minio_client()
        for listed in minio.list_objects_v2(Bucket=_BUCKET, Prefix=f"{root}/").get("Contents", []):
            minio.delete_object(Bucket=_BUCKET, Key=listed["Key"])


@pytest.fixture
def server_row(hub, compute_server, monkeypatch) -> Iterator[str]:
    """The desktop's hub-filled ``worker:worker`` row at the compute-only server's URL, with its hub run token."""
    node_id = hub._node_ids[SERVER]
    token = hub.signer.token(uid=UID, aud=SERVER, iss=hub.issuer)
    monkeypatch.setattr(node_tokens, "_tokens", {(DESKTOP_USER, node_id): (token, time.time() + 3600)})
    registry.reset()
    handshake.reset_handshake()
    with get_db_context() as db:
        db.add(ConfiguredBackend(id=BACKEND, url=compute_server, remote_id=node_id, account_user_id=DESKTOP_USER))
        db.commit()
    try:
        yield BACKEND
    finally:
        with get_db_context() as db:
            row = db.get(ConfiguredBackend, BACKEND)
            if row is not None:
                db.delete(row)
                db.commit()
        registry.reset()
        handshake.reset_handshake()


def add_reader(flow, node_id: int, hub) -> None:
    add_promise(flow, node_id, "catalog_reader")
    flow.add_catalog_reader(
        input_schema.NodeCatalogReader(
            flow_id=flow.flow_id,
            node_id=node_id,
            user_id=DESKTOP_USER,
            source="hub",
            catalog_full_table_name=hub.orders["qualified_name"],
            catalog_table_name="orders",
        )
    )


def copy_flow(open_flow, hub, location: str, name: str):
    """A flow on ``location`` reading ``orders`` and writing it back to ``sales`` as ``name``."""
    flow = open_flow(DESKTOP_USER, name)
    flow.flow_settings.execution_location = location
    add_reader(flow, 1, hub)
    add_writer(flow, 2, 1, DESKTOP_USER, sales_schema(hub), name)
    return flow


def errors_of(flow) -> dict[int, str]:
    return {result.node_id: result.error for result in flow.get_run_info().node_step_result if not result.success}


def test_a_desktops_hub_table_read_on_a_server_joined_with_keys_opens_the_credential_the_hub_sealed_to_it(
    hub,
    server_row,
    sent,
    desktop,
    open_flow,
):
    flow = copy_flow(open_flow, hub, server_row, "orders_copy")

    ran = client.post("/flow/run/", params={"flow_id": flow.flow_id}, headers=bearer(desktop))

    assert ran.status_code == 200, ran.text
    run_info = flow.get_run_info()
    assert run_info.success is True, errors_of(flow)
    assert {result.node_id: result.backend_id for result in run_info.node_step_result} == {1: server_row, 2: server_row}
    assert written(hub, "orders_copy").equals(pl.DataFrame(ORDERS).sort("id"))
    [vend] = hub.catalog.vend_calls
    assert vend["targets"] == [SERVER] and not vend.get("exchange")
    assert sent and not [route for route, body in sent if SECRET in body or b"$ffsec$" in body]
    assert any(route.endswith("/ws/submit") and b'"sealed"' in body for route, body in sent)


def test_a_desktops_hub_table_read_runs_on_a_server_on_another_python_patch_version(
    hub,
    server_row,
    sent,
    desktop,
    open_flow,
    monkeypatch,
):
    """Every plan core serialises is stamped as a desktop on another Python patch version would stamp it, which is the
    one thing the server's Polars compares: the handshake's Python probe, a cloudpickled function, is refused there,
    while the read's plan holds only an object pickled by reference, and runs."""
    other = plan_stamps.other_micro()
    serialize = pl.LazyFrame.serialize

    def as_another_python(self, *args, **kwargs):
        plan = serialize(self, *args, **kwargs)
        return plan_stamps.restamped(plan, other) if isinstance(plan, bytes) else plan

    monkeypatch.setattr(pl.LazyFrame, "serialize", as_another_python)
    flow = copy_flow(open_flow, hub, server_row, "orders_other_python")

    ran = client.post("/flow/run/", params={"flow_id": flow.flow_id}, headers=bearer(desktop))

    assert ran.status_code == 200, ran.text
    run_info = flow.get_run_info()
    assert run_info.success is True, errors_of(flow)
    assert {result.node_id: result.backend_id for result in run_info.node_step_result} == {1: server_row, 2: server_row}
    assert written(hub, "orders_other_python").equals(pl.DataFrame(ORDERS).sort("id"))
    table = hub.orders["file_path"].encode()
    reads = [body for route, body in sent if route.endswith("/ws/submit") and table in body]
    assert reads and all(plan_stamps.python_objects(read, other) == [plan_stamps.PICKLE] for read in reads)
    probe = handshake.last_verdict(server_row).absent[PYTHON_FUNCTIONS]
    assert "does not match the Python version used to serialize" in probe


def older_hub(hub) -> str:
    """The hub answers the vend as one from before targets: plain options, no sealed token. Answers the sentence core
    gives for an answer that is no Flowfile hub's."""
    hub.catalog._vend = lambda body: FakeHubWithCatalog._vend(hub.catalog, body)
    return (
        f"The hub did not give this run access to its tables: {hub.url} did not answer /hub/vend as a Flowfile hub "
        "does."
    )


def refuses_the_server(hub) -> str:
    """The hub refuses the server for this person; answers the hub's sentence."""
    hub.catalog.refused_nodes.add(SERVER)
    return f"You may not run on the server {SERVER}"


@pytest.mark.parametrize("hub_answer", [older_hub, refuses_the_server], ids=["older hub", "hub refuses the server"])
def test_a_desktops_step_the_hub_seals_nothing_for_is_refused_before_the_run_with_a_sentence(
    hub,
    server_row,
    sent,
    desktop,
    open_flow,
    hub_answer,
):
    sentence = hub_answer(hub)
    flow = copy_flow(open_flow, hub, server_row, "orders_nowhere")

    ran = client.post("/flow/run/", params={"flow_id": flow.flow_id}, headers=bearer(desktop))

    assert ran.status_code == 200, ran.text
    errors = errors_of(flow)
    assert errors == {1: sentence, 2: sentence}
    assert not [error for error in errors.values() if "InvalidToken" in error]
    assert sent == [] and hub.catalog.registered == []


def test_a_desktops_hub_table_step_on_a_server_beside_one_on_this_computer_runs_each_with_a_vend_of_its_own(
    hub,
    server_row,
    sent,
    desktop,
    open_flow,
    monkeypatch,
):
    monkeypatch.setenv("FLOWFILE_EXCHANGE_URI", f"s3://{_BUCKET}/exchange")
    flow = open_flow(DESKTOP_USER, "split_reads")
    add_reader(flow, 1, hub)
    add_reader(flow, 2, hub)
    on(flow, 1, server_row)

    ran = client.post("/flow/run/", params={"flow_id": flow.flow_id}, headers=bearer(desktop))

    assert ran.status_code == 200, ran.text
    run_info = flow.get_run_info()
    assert run_info.success is True, errors_of(flow)
    assert {result.node_id: result.backend_id for result in run_info.node_step_result} == {1: server_row, 2: "local"}
    sealed, own = hub.catalog.vend_calls
    assert sealed["targets"] == [SERVER] and "targets" not in own
    assert sealed["run_id"] == own["run_id"]
    assert sealed["read_namespace_ids"] == own["read_namespace_ids"] == [hub.sales["id"]]
    assert sent and not [route for route, body in sent if SECRET in body or b"$ffsec$" in body]


def test_a_desktops_hub_table_steps_on_its_own_worker_keep_the_vend_without_targets(
    hub,
    server_row,
    desktop,
    open_flow,
):
    flow = copy_flow(open_flow, hub, "local", "orders_here")

    ran = client.post("/flow/run/", params={"flow_id": flow.flow_id}, headers=bearer(desktop))

    assert ran.status_code == 200, ran.text
    assert flow.get_run_info().success is True, errors_of(flow)
    [vend] = hub.catalog.vend_calls
    assert "targets" not in vend
    assert written(hub, "orders_here").equals(pl.DataFrame(ORDERS).sort("id"))
