"""A server that joined its hub with keys of its own reports every run of a person signed in through the hub to that
hub, as that person: with their delegated access token, naming no run row of its own and only the hub's tables.

The hub is the sign-in walk's fake (``tests/hub/test_server_sign_in.PersonHub``) holding the organisation's catalog
too: core's catalog reads, ``POST /hub/vend``, a new table's registration and ``POST /hub/runs`` are answered by
``tests/hub_client``'s ``FakeHubWithCatalog`` for an access token the hub issued at a sign-in, and every such request
is kept with the bearer it came with. Core runs in process in docker mode, as a server does, its ``hub_node.json``,
its keys and its secure store under the test's ``tmp_path``. Each case runs with ``FLOWFILE_HUB_URL`` unset and set to
the hub, as an install may set it on a server that joined. The case that reads and writes the hub's tables stores
them on the suite's MinIO and skips without it.
"""

import uuid
from pathlib import Path

import httpx
import polars as pl
import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import FlowRun
from flowfile_core.hub_client import report
from flowfile_core.routes import routes
from flowfile_core.schemas import input_schema, transform_schema
from flowfile_core.schemas.output_model import NodeResult, RunInformation
from shared import hub_node_key
from shared.storage_config import storage
from shared.tests.hub_jwks import HubSigner
from tests import test_hub_sign_in
from tests.flowfile.conftest import (
    CATALOG_SAMPLE_DATA,
    add_test_catalog_writer,
    add_test_manual_input,
    catalog_cleanup,
    create_test_graph,
    create_test_namespace,
)
from tests.flowfile.execution.graph_helpers import add_promise, connect
from tests.flowfile.execution.test_hub_tables_e2e import EU, MINIO_OPTIONS, ORDERS
from tests.hub.test_server_sign_in import PersonHub
from tests.hub_client.conftest import FakeHubWithCatalog
from tests.test_catalog_cloud_virtual import _BUCKET, get_minio_client, requires_minio
from tests.test_hub_sign_in import NODE, bearer, join

node_file = test_hub_sign_in.node_file
username = test_hub_sign_in.username
local_user = test_hub_sign_in.local_user

client = TestClient(main.app)
UID = 7
CATALOG_PATHS = (
    "/catalog/namespaces",
    "/catalog/namespaces/tree",
    "/catalog/tables",
    "/catalog/tables/from-data",
    "/hub/flows",
    "/hub/vend",
    "/hub/runs",
)


class OrganisationHub(PersonHub):
    """``PersonHub`` with the organisation's catalog: its catalog reads and writes, vends and run reports are answered
    by ``catalog``, a ``FakeHubWithCatalog`` that takes an access token this hub issued as that person's.

    ``received`` keeps each of those requests as ``(method, path, bearer)``.
    """

    def __init__(self, signer: HubSigner):
        super().__init__(signer)
        self.catalog = FakeHubWithCatalog()
        self.catalog._bearer_user = self._person
        self.received: list[tuple[str, str, str]] = []
        hub = self

        class Handler(self._server.RequestHandlerClass):
            def do_GET(self):
                if hub.holds(self.path):
                    self._forward()
                else:
                    super().do_GET()

            def do_POST(self):
                if hub.holds(self.path):
                    self._forward()
                else:
                    super().do_POST()

            def _forward(self):
                length = int(self.headers.get("Content-Length") or 0)
                request = httpx.Request(
                    self.command,
                    hub.url + self.path,
                    headers=list(self.headers.items()),
                    content=self.rfile.read(length),
                )
                _, _, token = (self.headers.get("Authorization") or "").partition(" ")
                with hub._lock:
                    hub.received.append((self.command, request.url.path, token))
                response = hub.catalog.handle(request)
                self._answer(response.status_code, response.json())

        self._server.RequestHandlerClass = Handler

    @staticmethod
    def holds(path: str) -> bool:
        route = path.split("?", 1)[0]
        return route in CATALOG_PATHS or route.startswith("/catalog/tables/")

    def _person(self, request: httpx.Request) -> str | None:
        scheme, _, token = request.headers.get("Authorization", "").partition(" ")
        with self._lock:
            uid = self.issued.get(token) if scheme == "Bearer" else None
        return None if uid is None else self.people[uid]["username"]

    def sent_with(self, method: str, path: str) -> list[str]:
        """The bearer of each ``method`` request to ``path`` this hub received for its catalog."""
        return [token for sent, route, token in list(self.received) if (sent, route) == (method, path)]

    def asked_for_exchange_store(self) -> bool:
        return any(path.startswith("/hub/exchange-store") for path, _ in list(self.calls))


@pytest.fixture(autouse=True)
def clean_catalog():
    catalog_cleanup()
    yield
    with get_db_context() as db:
        db.query(FlowRun).delete()
        db.commit()
    catalog_cleanup()


@pytest.fixture
def hub(node_file, monkeypatch):
    """An ``OrganisationHub`` this server joined with its own keys, enabled; the accounts its people got are removed
    afterwards. The master key is the server's own, as docker mode reads it."""
    monkeypatch.setenv("FLOWFILE_MASTER_KEY", Fernet.generate_key().decode())
    signer = HubSigner()
    fake = OrganisationHub(signer)
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


@pytest.fixture(params=[False, True], ids=["hub-url-unset", "hub-url-set"])
def hub_url(request, hub, monkeypatch) -> str:
    """``FLOWFILE_HUB_URL`` unset, or naming the hub this server joined."""
    value = hub.url if request.param else ""
    if request.param:
        monkeypatch.setenv("FLOWFILE_HUB_URL", value)
    return value


@pytest.fixture
def open_flow():
    """Opens a new flow in a user's session, saved in the server's flows folder (a server registers a flow nowhere
    else) and run on the worker embedded here."""
    opened: list[int] = []
    files: list[Path] = []

    def open_one(user_id: int, name: str):
        storage.flows_directory.mkdir(parents=True, exist_ok=True)
        path = storage.flows_directory / f"{name}_{uuid.uuid4().hex[:8]}.yaml"
        files.append(path)
        flow_id = routes.flow_file_handler.add_flow(name=name, flow_path=str(path), user_id=user_id)
        opened.append(flow_id)
        flow = routes.flow_file_handler.get_flow(flow_id)
        flow.flow_settings.execution_location = "local"
        return flow

    yield open_one
    for flow_id in opened:
        routes.flow_file_handler.delete_flow(flow_id)
    for path in files:
        path.unlink(missing_ok=True)


def sign_in(hub: OrganisationHub, name: str) -> tuple[int, str]:
    """The person ``UID`` signs in through the hub as ``name``: their account here and core's access token."""
    signed_in = client.post("/auth/hub/signin", json={"token": hub.sign_in_token(NODE, uid=UID, username=name)})
    assert signed_in.status_code == 200, signed_in.text
    access_token = signed_in.json()["access_token"]
    me = client.get("/auth/users/me", headers=bearer(access_token))
    assert me.status_code == 200, me.text
    return me.json()["id"], access_token


def run_of(flow) -> FlowRun:
    with get_db_context() as db:
        [row] = db.query(FlowRun).filter_by(flow_path=flow.flow_settings.path).all()
        db.expunge(row)
        return row


def test_every_run_of_a_person_signed_in_through_the_hub_is_reported_once_with_their_delegated_token(
    hub, hub_url, username, open_flow
):
    person, access_token = sign_in(hub, username)
    [delegated] = hub.issued
    flow = open_flow(person, "made_on_the_server")
    add_test_manual_input(flow, CATALOG_SAMPLE_DATA, node_id=1)
    assert flow.flow_settings.hub_origin is None

    ran = client.post("/flow/run/", params={"flow_id": flow.flow_id}, headers=bearer(access_token))

    assert ran.status_code == 200, ran.text
    run = run_of(flow)
    assert (run.success, run.user_id) == (True, person)
    assert hub.sent_with("POST", "/hub/runs") == [delegated]
    [posted] = hub.catalog.reports
    uuid.UUID(posted["run_id"])
    assert (posted["flow_uuid"], posted["success"]) == (run.flow_uuid, True)
    assert "core_run_id" not in posted
    assert (posted["inputs"], posted["outputs"], posted["vend_ids"]) == ([], [], [])
    assert [node["node_type"] for node in posted["nodes"]] == ["manual_input"]
    assert not hub.asked_for_exchange_store()


def test_a_run_of_an_account_that_did_not_sign_in_through_the_hub_is_not_reported(
    hub, hub_url, local_user, username, open_flow
):
    account = local_user(username)
    flow = open_flow(account, "from_before_the_join")
    add_test_manual_input(flow, CATALOG_SAMPLE_DATA, node_id=1)

    assert flow.try_claim_run()
    routes._run_and_track(flow, user_id=account)

    assert run_of(flow).success is True
    assert hub.received == [] and hub.catalog.reports == []
    assert not hub.asked_for_exchange_store()


def test_a_report_names_only_the_hubs_tables_with_the_servers_keys_or_without(hub, hub_url, username):
    person, _ = sign_in(hub, username)
    catalog = hub.catalog.seed_catalog("sales", storage_uri="s3://acme/sales")
    eu = hub.catalog.seed_schema(catalog, "eu")
    orders = hub.catalog.seed_table(eu, "orders", file_path="s3://acme/sales/orders", is_remote_storage=True)
    namespace_id = create_test_namespace()
    graph = create_test_graph(flow_id=8131)
    add_test_manual_input(graph, CATALOG_SAMPLE_DATA, node_id=1)
    add_test_catalog_writer(graph, node_id=2, depending_on_id=1, table_name="server_people", namespace_id=namespace_id)
    add_promise(graph, 3, "catalog_reader")
    graph.add_catalog_reader(
        input_schema.NodeCatalogReader(
            flow_id=graph.flow_id,
            node_id=3,
            user_id=person,
            source="hub",
            catalog_full_table_name="sales.eu.orders",
            catalog_table_name="orders",
        )
    )
    results = []
    for node_id in (1, 2, 3):
        result = NodeResult(node_id=node_id)
        result.finish(success=True, error="")
        results.append(result)
    finished = RunInformation(
        flow_id=graph.flow_id,
        success=True,
        nodes_completed=3,
        number_of_nodes=3,
        node_step_result=results,
        run_type="full_run",
    )
    hub_table = {"namespace_id": eu["id"], "table_id": orders["id"], "table_name": "orders"}

    joined = report.build_run_report(graph, finished, flow_uuid=str(uuid.uuid4()))

    assert "core_run_id" not in joined
    assert (joined["inputs"], joined["outputs"]) == ([hub_table], [])

    hub_node_key.path().unlink()
    without_keys = report.build_run_report(graph, finished, flow_uuid=str(uuid.uuid4()))

    assert "core_run_id" not in without_keys
    assert (without_keys["inputs"], without_keys["outputs"]) == ([hub_table], [])


@pytest.fixture
def minio_catalog(hub):
    """The hub's catalog ``hub_<8 hex>`` on MinIO with the Delta table ``orders`` in its schema ``sales``, which the
    hub vends the MinIO keys for; ``(catalog, sales, orders)`` as the hub lists them. Removed from MinIO afterwards."""
    name = f"hub_{uuid.uuid4().hex[:8]}"
    storage_uri = f"s3://{_BUCKET}/{name}"
    catalog = hub.catalog.seed_catalog(name, storage_uri=storage_uri)
    sales = hub.catalog.seed_schema(catalog, "sales")
    hub.catalog.grants.update({catalog["id"]: "manage", sales["id"]: "manage"})
    hub.catalog.vend_options[storage_uri] = dict(MINIO_OPTIONS)
    frame = pl.DataFrame(ORDERS)
    frame.write_delta(f"{storage_uri}/orders", storage_options=MINIO_OPTIONS)
    columns = [{"name": c, "dtype": str(t)} for c, t in frame.schema.items()]
    orders = hub.catalog.seed_table(
        sales, "orders", file_path=f"{storage_uri}/orders", is_remote_storage=True, schema_columns=columns
    )
    try:
        yield catalog, sales, orders
    finally:
        minio = get_minio_client()
        for listed in minio.list_objects_v2(Bucket=_BUCKET, Prefix=f"{name}/").get("Contents", []):
            minio.delete_object(Bucket=_BUCKET, Key=listed["Key"])


def add_hub_tables_flow(flow, person: int, sales: dict, orders: dict) -> None:
    """Reader of the hub's ``orders`` → filter on ``country`` → writer of ``orders_eu`` into ``sales``, all set by
    ``person``."""
    add_promise(flow, 1, "catalog_reader")
    flow.add_catalog_reader(
        input_schema.NodeCatalogReader(
            flow_id=flow.flow_id,
            node_id=1,
            user_id=person,
            source="hub",
            catalog_full_table_name=orders["qualified_name"],
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
    add_promise(flow, 3, "catalog_writer")
    flow.add_catalog_writer(
        input_schema.NodeCatalogWriter(
            flow_id=flow.flow_id,
            node_id=3,
            depending_on_id=2,
            user_id=person,
            catalog_write_settings=input_schema.CatalogWriteSettings(
                table_name="orders_eu",
                source="hub",
                namespace_full_name=orders["qualified_name"].rpartition(".")[0],
                write_mode="overwrite",
            ),
        )
    )
    connect(flow, 2, 3)


@requires_minio
@pytest.mark.usefixtures("protocol_on")
def test_a_person_reads_and_writes_the_hubs_tables_on_its_storage_and_the_run_is_reported_as_theirs(
    hub, hub_url, minio_catalog, username, open_flow
):
    catalog, sales, orders = minio_catalog
    person, access_token = sign_in(hub, username)
    [delegated] = hub.issued

    tree = client.get("/catalog/namespaces/tree", headers=bearer(access_token))
    assert tree.status_code == 200, tree.text
    [listed] = [row for row in tree.json() if row["source"] == "hub"]
    assert (listed["hub_id"], listed["name"]) == (catalog["id"], catalog["name"])
    [schema] = listed["children"]
    assert (schema["hub_id"], [table["hub_id"] for table in schema["tables"]]) == (sales["id"], [orders["id"]])

    flow = open_flow(person, "hub_tables_on_the_server")
    add_hub_tables_flow(flow, person, sales, orders)
    hub.received.clear()
    ran = client.post("/flow/run/", params={"flow_id": flow.flow_id}, headers=bearer(access_token))

    assert ran.status_code == 200, ran.text
    run_info = flow.get_run_info()
    assert run_info.success is True, [(r.node_id, r.error) for r in run_info.node_step_result]
    [vend] = hub.catalog.vend_calls
    assert (vend["read_namespace_ids"], vend["write_namespace_ids"]) == ([sales["id"]], [sales["id"]])
    assert hub.sent_with("POST", "/hub/vend") == [delegated]
    assert flow.last_hub_vend == (vend["run_id"], hub.catalog._vends) and flow.hub_vend is None

    [registered] = hub.catalog.registered
    assert (registered["name"], registered["namespace_id"]) == ("orders_eu", sales["id"])
    written = pl.scan_delta(registered["table_path"], storage_options=MINIO_OPTIONS).collect().sort("id")
    assert written.equals(pl.DataFrame(ORDERS).filter(pl.col("country").is_in(EU)).sort("id"))
    new_table = next(table for table in hub.catalog.tables if table["name"] == "orders_eu")

    assert hub.sent_with("POST", "/hub/runs") == [delegated]
    [posted] = hub.catalog.reports
    assert (posted["run_id"], posted["vend_ids"]) == (vend["run_id"], [hub.catalog._vends])
    assert (posted["flow_uuid"], posted["success"]) == (run_of(flow).flow_uuid, True)
    assert posted["inputs"] == [{"namespace_id": sales["id"], "table_id": orders["id"], "table_name": "orders"}]
    assert posted["outputs"] == [{"namespace_id": sales["id"], "table_id": new_table["id"], "table_name": "orders_eu"}]
    assert {token for _, _, token in hub.received} == {delegated}
    assert not hub.asked_for_exchange_store()


@requires_minio
@pytest.mark.usefixtures("protocol_on")
def test_a_performance_run_over_the_hubs_tables_reports_the_rows_its_write_counted(
    hub, hub_url, minio_catalog, username, open_flow
):
    _, sales, orders = minio_catalog
    person, access_token = sign_in(hub, username)
    flow = open_flow(person, "hub_tables_in_performance")
    flow.flow_settings.execution_mode = "Performance"
    add_hub_tables_flow(flow, person, sales, orders)

    ran = client.post("/flow/run/", params={"flow_id": flow.flow_id}, headers=bearer(access_token))

    assert ran.status_code == 200, ran.text
    [posted] = hub.catalog.reports
    assert posted["success"] is True, posted["nodes"]
    eu_rows = pl.DataFrame(ORDERS).filter(pl.col("country").is_in(EU)).height
    # Nothing says how many rows the filter read, so the read stays unknown.
    assert {node["node_id"]: node["rows_out"] for node in posted["nodes"]} == {1: None, 2: eu_rows, 3: eu_rows}
