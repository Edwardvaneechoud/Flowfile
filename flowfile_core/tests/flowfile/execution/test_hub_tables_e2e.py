"""A desktop reads a table in its hub's catalog on object storage and writes one next to it, end to end.

The hub is a fake served over HTTP from a thread on a loopback port; the hub client, the session refresh and the run
report reach it with their own ``httpx`` clients. The storage is the suite's MinIO: the hub's catalog lives under a
prefix of the test bucket, a Delta table ``orders`` is written there first, and the fake vends the MinIO keys as the
run's credential. A flow reading ``orders``, filtering it and writing ``orders_eu`` into the same schema runs through
the designer's run path on the session worker and on the embedded one. The hub sees one vend for the run, one new
table and one run report naming both tables by its ids and the vend; the desktop keeps nothing of the credential.
Without a hub session the same flow ends before anything is placed, and nothing is reported.

The module skips when MinIO is not available; the ``remote`` cases skip when the session worker is not running.
"""

import json
import uuid
from collections.abc import Iterator
from pathlib import Path

import polars as pl
import pytest

from flowfile_core.auth import secrets as core_secrets
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogTable, FlowRun
from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.hub_client import client, session
from flowfile_core.hub_client.catalog import NOT_CONNECTED
from flowfile_core.routes import routes
from flowfile_core.schemas import input_schema, transform_schema
from shared.cloud_storage.storage_options import build_storage_options
from test_utils.s3.fixtures import MINIO_ACCESS_KEY, MINIO_ENDPOINT_URL, MINIO_SECRET_KEY
from tests.flowfile.conftest import catalog_cleanup, create_test_flow_registration, create_test_namespace
from tests.flowfile.execution.graph_helpers import add_promise, connect, make_graph
from tests.hub_client import conftest as hub_conftest
from tests.hub_client.conftest import FakeHubWithCatalog
from tests.test_catalog_cloud_virtual import _BUCKET, get_minio_client, requires_minio

pytestmark = [requires_minio, pytest.mark.usefixtures("protocol_on")]
serve_fake = hub_conftest.serve_fake

MINIO_OPTIONS = build_storage_options(
    storage_type="s3",
    auth_method="access_key",
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    aws_region="us-east-1",
    endpoint_url=MINIO_ENDPOINT_URL,
    aws_allow_unsafe_html=True,
)
ORDERS = {"id": [1, 2, 3, 4, 5], "country": ["NL", "US", "DE", "NL", "BR"], "amount": [10, 20, 30, 40, 50]}
EU = {"NL", "DE"}


class HubCatalog:
    """The served fake, the hub's catalog on MinIO and the hub ids the flow names."""

    def __init__(self, fake: FakeHubWithCatalog, url: str, storage_uri: str, sales: dict, orders: dict):
        self.fake = fake
        self.url = url
        self.storage_uri = storage_uri
        self.sales = sales
        self.orders = orders


@pytest.fixture
def store(tmp_path, monkeypatch) -> Path:
    """A secure store of the test's own, holding the suite's ``flowfile`` keys (the master key, the internal token)
    that the session worker reads from the suite's store."""
    core_secrets.get_master_key()
    keys = core_secrets._storage._read_store("flowfile")
    path = tmp_path / "secure_storage"
    monkeypatch.setenv("FLOWFILE_SECURE_STORAGE_PATH", str(path))
    monkeypatch.setenv("SECURE_STORAGE_PATH", str(path))
    monkeypatch.setattr(core_secrets, "_storage", core_secrets.SecureStorage())
    monkeypatch.setattr(session, "_access_tokens", {})
    for name, value in keys.items():
        core_secrets.set_password("flowfile", name, value)
    return path


def store_contents(path: Path) -> dict[str, dict]:
    """Every service in the secure store at ``path`` and what it holds."""
    services = (p.name.removesuffix(".json.enc") for p in path.iterdir() if p.name.endswith(".json.enc"))
    return {service: core_secrets._storage._read_store(service) for service in sorted(services)}


@pytest.fixture
def hub(serve_fake, store, monkeypatch) -> Iterator[HubCatalog]:
    """A served fake hub whose catalog ``hub_<8 hex>`` lives on MinIO, with ``orders`` in its schema ``sales``.

    User 1 is connected to it as the Hub tab connects; the session lives in the test's own secure store. The
    catalog's objects are removed from MinIO afterwards.
    """
    catalog_cleanup()
    fake = FakeHubWithCatalog()
    url = serve_fake(fake)
    name = f"hub_{uuid.uuid4().hex[:8]}"
    storage_uri = f"s3://{_BUCKET}/{name}"
    root = fake.seed_catalog(name, storage_uri=storage_uri)
    sales = fake.seed_schema(root, "sales")
    fake.grants.update({root["id"]: "manage", sales["id"]: "manage"})
    fake.vend_options[storage_uri] = dict(MINIO_OPTIONS)
    frame = pl.DataFrame(ORDERS)
    frame.write_delta(f"{storage_uri}/orders", storage_options=MINIO_OPTIONS)
    columns = [{"name": c, "dtype": str(t)} for c, t in frame.schema.items()]
    orders = fake.seed_table(
        sales, "orders", file_path=f"{storage_uri}/orders", is_remote_storage=True, schema_columns=columns
    )
    tokens = client.sign_in(url, "ana", "s3cret")
    session.save(1, {"hub_url": url, "refresh_token": tokens["refresh_token"], "device_name": "e2e"})
    fake.calls.clear()
    try:
        yield HubCatalog(fake, url, storage_uri, sales, orders)
    finally:
        minio = get_minio_client()
        listed = minio.list_objects_v2(Bucket=_BUCKET, Prefix=f"{name}/").get("Contents", [])
        for obj in listed:
            minio.delete_object(Bucket=_BUCKET, Key=obj["Key"])
        catalog_cleanup()
        with get_db_context() as db:
            db.query(FlowRun).delete()
            db.commit()


@pytest.fixture(params=["remote", "local"])
def location(request) -> str:
    """Where the flow runs: the session worker (``remote``) or the embedded worker (``local``)."""
    if request.param == "remote":
        request.getfixturevalue("running_worker")
    return request.param


@pytest.fixture
def backend_calls(monkeypatch) -> list[str]:
    """Every ``submit`` and ``run_op`` a worker backend or the embedded one receives, by name; still forwarded."""
    calls: list[str] = []
    for backend in (WorkerBackend, EmbeddedWorkerBackend):
        for name in ("submit", "run_op"):
            original = getattr(backend, name)

            def recording(self, *args, _original=original, _name=f"{backend.__name__}.{name}", **kwargs):
                calls.append(_name)
                return _original(self, *args, **kwargs)

            monkeypatch.setattr(backend, name, recording)
    return calls


def build_flow(hub: HubCatalog, location: str, flow_path: Path):
    """Reader of the hub's ``orders`` → filter on ``country`` → writer of ``orders_eu`` into ``sales``, saved to
    ``flow_path`` and registered locally."""
    flow_id = 9900 + (0 if location == "remote" else 1)
    graph = make_graph(flow_id, execution_location=location, path=str(flow_path))
    add_promise(graph, 1, "catalog_reader")
    graph.add_catalog_reader(
        input_schema.NodeCatalogReader(
            flow_id=flow_id,
            node_id=1,
            user_id=1,
            source="hub",
            catalog_full_table_name=hub.orders["qualified_name"],
            catalog_table_name="orders",
        )
    )
    add_promise(graph, 2, "filter")
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(
                mode="basic",
                basic_filter=transform_schema.BasicFilter(field="country", operator="in", value="NL, DE"),
            ),
        )
    )
    connect(graph, 1, 2)
    add_promise(graph, 3, "catalog_writer")
    graph.add_catalog_writer(
        input_schema.NodeCatalogWriter(
            flow_id=flow_id,
            node_id=3,
            depending_on_id=2,
            user_id=1,
            catalog_write_settings=input_schema.CatalogWriteSettings(
                table_name="orders_eu",
                source="hub",
                namespace_full_name=hub.fake._full_name(hub.sales["id"]),
                write_mode="overwrite",
            ),
        )
    )
    connect(graph, 2, 3)
    graph.save_flow(flow_path=str(flow_path))
    create_test_flow_registration(create_test_namespace(), name="hub_tables_e2e", path=str(flow_path))
    return graph


def run(graph):
    """Run ``graph`` as the designer's run route does, its completion hook included; the run's information."""
    assert graph.try_claim_run()
    routes._run_and_track(graph, user_id=1, node_ids=None)
    return graph.get_run_info()


def run_row(flow_path: Path) -> FlowRun:
    with get_db_context() as db:
        [row] = db.query(FlowRun).filter_by(flow_path=str(flow_path)).all()
        db.expunge(row)
        return row


def test_a_desktop_reads_and_writes_hub_tables_with_one_vend_and_one_report(hub, location, store, tmp_path):
    flow_path = tmp_path / "hub_tables.yaml"
    graph = build_flow(hub, location, flow_path)
    before = store_contents(store)
    assert set(before) == {"flowfile", session.SERVICE}
    try:
        run_info = run(graph)

        assert run_info.success is True, [(r.node_id, r.error) for r in run_info.node_step_result]
        assert {r.backend_id for r in run_info.node_step_result} == {"worker" if location == "remote" else "local"}
        [vend] = hub.fake.vend_calls
        assert vend["read_namespace_ids"] == [hub.sales["id"]]
        assert vend["write_namespace_ids"] == [hub.sales["id"]]
        vend_id = hub.fake._vends
        assert graph.last_hub_vend == (vend["run_id"], vend_id)
        assert graph.hub_vend is None

        [registered] = hub.fake.registered
        assert (registered["name"], registered["namespace_id"]) == ("orders_eu", hub.sales["id"])
        table_path = registered["table_path"]
        assert table_path.startswith(f"{hub.storage_uri}/orders_eu")
        expected = pl.DataFrame(ORDERS).filter(pl.col("country").is_in(EU)).sort("id")
        assert registered["row_count"] == expected.height
        prefix = table_path.removeprefix(f"s3://{_BUCKET}/")
        keys = [o["Key"] for o in get_minio_client().list_objects_v2(Bucket=_BUCKET, Prefix=prefix)["Contents"]]
        assert any("/_delta_log/" in key for key in keys), keys
        written = pl.scan_delta(table_path, storage_options=MINIO_OPTIONS).collect().sort("id")
        assert written.equals(expected)
        new_table = next(t for t in hub.fake.tables if t["name"] == "orders_eu")

        [posted] = hub.fake.reports
        assert (posted["run_id"], posted["vend_ids"]) == (vend["run_id"], [vend_id])
        assert posted["inputs"] == [
            {"namespace_id": hub.sales["id"], "table_id": hub.orders["id"], "table_name": "orders"}
        ]
        assert posted["outputs"] == [
            {"namespace_id": hub.sales["id"], "table_id": new_table["id"], "table_name": "orders_eu"}
        ]
        assert (posted["flow_version"], posted["success"]) == (None, True)

        with get_db_context() as db:
            assert db.query(CatalogTable).filter_by(name="orders_eu").count() == 0
        row = run_row(flow_path)
        node_results = json.dumps([r.model_dump(mode="json") for r in run_info.node_step_result])
        for text in (flow_path.read_text(), row.flow_snapshot or "", row.node_results_json or "", node_results):
            assert MINIO_ACCESS_KEY not in text and MINIO_SECRET_KEY not in text
        held = store_contents(store)
        assert held["flowfile"] == before["flowfile"], "the suite's keys alone, as before the run"
        assert set(held) == {"flowfile", session.SERVICE} and list(held[session.SERVICE]) == ["session:1"]
        assert MINIO_SECRET_KEY not in json.dumps(held)
    finally:
        graph.close_flow()


@pytest.mark.parametrize(
    "mode, rows_out",
    # In Performance only the write counted, and nothing says how many rows the filter read.
    [("Performance", {1: None, 2: 3, 3: 3}), ("Development", {1: 5, 2: None, 3: 3})],
)
def test_a_run_reports_the_rows_it_knows(hub, location, tmp_path, mode, rows_out):
    graph = build_flow(hub, location, tmp_path / "hub_tables.yaml")
    graph.flow_settings.execution_mode = mode
    try:
        run_info = run(graph)

        assert run_info.success is True, [(r.node_id, r.error) for r in run_info.node_step_result]
        [posted] = hub.fake.reports
        assert {node["node_id"]: node["rows_out"] for node in posted["nodes"]} == rows_out
    finally:
        graph.close_flow()


def test_without_a_hub_session_the_flow_ends_before_anything_runs_and_nothing_is_reported(
    hub, location, tmp_path, backend_calls
):
    flow_path = tmp_path / "hub_tables.yaml"
    graph = build_flow(hub, location, flow_path)
    session.clear(1)
    hub.fake.calls.clear()
    try:
        run_info = run(graph)

        assert run_info.success is False
        errors = {r.node_id: r.error for r in run_info.node_step_result}
        assert errors == {1: NOT_CONNECTED, 3: NOT_CONNECTED}
        assert all(r.compute_error == "Refusal" for r in run_info.node_step_result)
        assert hub.fake.vend_calls == [] and hub.fake.registered == [] and hub.fake.reports == []
        assert backend_calls == [], "nothing reached a worker"
        assert hub.fake.calls == []
        assert graph.hub_vend is None and graph.last_hub_vend is None
        assert run_row(flow_path).success is False
    finally:
        graph.close_flow()
