"""A run over tables on the hub asks the hub once for their credential, uses it for the run and keeps nothing.

The hub is a fake behind the hub client's seam and no object storage is reached: the reader's Delta scan is recorded
and answered with rows, the writer's operation is recorded and answered with a Delta write's report. A run without a
hub session, or one the hub refuses, ends before any node starts, each node on a hub table failed with the sentence.
"""

import polars as pl
import pytest

from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogTable
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.payloads import CatalogTableIdentity, DeltaWritePayload, unpack
from flowfile_core.flowfile.execution.protocol import OperationKind, OpResult
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.hub_client import client, session
from flowfile_core.hub_client.client import HubRefused
from flowfile_core.schemas import input_schema
from shared.cloud_credential_provider import PROVIDER_CREDENTIAL_KEYS, EncryptedCredentialProvider
from tests.conftest import protocol_states
from tests.flowfile.execution.graph_helpers import add_filter_on, add_manual_input, add_promise, connect, make_graph
from tests.hub_client import conftest as hub_conftest
from tests.hub_client.conftest import HUB_URL, FakeHubWithCatalog

store = hub_conftest.store
flag = protocol_states()

RECONNECT = "This flow uses tables on the hub, but this Flowfile is not connected to it; connect to the hub again (Compute → Hub)"
OPTIONS = {
    "aws_region": "eu-west-1",
    "aws_access_key_id": "AKIAHUBVENDED",
    "aws_secret_access_key": "vended-secret-key",
    "endpoint_url": "http://minio.example.test:9000",
    "aws_allow_http": "true",
}
COLUMNS = [{"name": "id", "dtype": "Int64"}, {"name": "name", "dtype": "String"}]
ROWS = {"id": [1, 2], "name": ["ada", "bob"]}


@pytest.fixture
def hub(monkeypatch) -> FakeHubWithCatalog:
    fake = FakeHubWithCatalog()
    monkeypatch.setattr(client, "_module_http", fake.http())
    sales = fake.seed_catalog("sales", storage_uri="s3://acme/sales", storage_connection_name="acme-s3")
    raw = fake.seed_schema(sales, "raw")
    curated = fake.seed_schema(sales, "curated")
    scratch = fake.seed_catalog("scratch")
    notes = fake.seed_schema(scratch, "notes")
    fake.seed_table(
        raw, "orders", file_path="s3://acme/sales/orders_0a1b2c3d", is_remote_storage=True, schema_columns=COLUMNS
    )
    fake.seed_table(notes, "memo", file_path="/srv/hub/catalog_tables/memo_1", schema_columns=COLUMNS)
    fake.vend_options["s3://acme/sales"] = dict(OPTIONS)
    fake.grants.update({raw["id"]: "use", curated["id"]: "manage", notes["id"]: "manage"})
    fake.ids = {"raw": raw["id"], "curated": curated["id"], "notes": notes["id"]}
    fake.ids.update({t["name"]: t["id"] for t in fake.tables})
    return fake


@pytest.fixture
def connected(store, hub) -> FakeHubWithCatalog:
    tokens = client.sign_in(HUB_URL, "ana", "s3cret")
    session.save(1, {"hub_url": HUB_URL, "refresh_token": tokens["refresh_token"], "device_id": 1})
    hub.calls.clear()
    return hub


@pytest.fixture
def scans(monkeypatch) -> list[tuple[str, dict]]:
    """Every Delta scan as ``(path, kwargs)``, answered with ``ROWS``."""
    recorded: list[tuple[str, dict]] = []

    def scan_delta(path, **kwargs):
        recorded.append((path, kwargs))
        return pl.LazyFrame(ROWS)

    monkeypatch.setattr(pl, "scan_delta", scan_delta)
    return recorded


@pytest.fixture
def carrying_scans(monkeypatch, tmp_path) -> list[tuple[str, dict]]:
    """Every Delta scan as ``(path, kwargs)``, answered with a scan of ``ROWS`` whose plan holds the given provider.

    So the plan carries the credential as a real scan of a hub table does, as does every plan composed on it.
    """
    rows = tmp_path / "rows.parquet"
    pl.DataFrame(ROWS).write_parquet(rows)
    recorded: list[tuple[str, dict]] = []

    def scan_delta(path, **kwargs):
        recorded.append((path, kwargs))
        return pl.scan_parquet(rows, credential_provider=kwargs["credential_provider"])

    monkeypatch.setattr(pl, "scan_delta", scan_delta)
    return recorded


@pytest.fixture
def operations(monkeypatch) -> list[tuple[FlowNode, OperationKind, dict]]:
    """Every node operation as ``(node, kind, payload)``, answered with the report of a Delta write of ``ROWS``."""
    recorded = []

    def run_op(self, backend, op, payload, *, job_id):
        recorded.append((self, op, payload))
        write = unpack(payload, DeltaWritePayload)
        return OpResult(
            refs={},
            metadata={
                "table_path": f"{write.table.base_uri}/{write.table.table_name}",
                "version": 0,
                "schema": COLUMNS,
                "row_count": 2,
                "column_count": 2,
                "size_bytes": 1234,
            },
        )

    monkeypatch.setattr(FlowNode, "run_op", run_op)
    return recorded


def add_hub_reader(graph, node_id: int, hub: FakeHubWithCatalog, table: str = "orders", namespace: str = "raw"):
    add_promise(graph, node_id, "catalog_reader")
    graph.add_catalog_reader(
        input_schema.NodeCatalogReader(
            flow_id=graph.flow_id,
            node_id=node_id,
            user_id=1,
            hub_table_id=hub.ids[table],
            hub_namespace_id=hub.ids[namespace],
        )
    )


def add_hub_writer(graph, node_id: int, depending_on_id: int, namespace_id: int, name: str, mode: str = "overwrite"):
    add_promise(graph, node_id, "catalog_writer")
    graph.add_catalog_writer(
        input_schema.NodeCatalogWriter(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=depending_on_id,
            user_id=1,
            catalog_write_settings=input_schema.CatalogWriteSettings(
                table_name=name, hub_namespace_id=namespace_id, write_mode=mode
            ),
        )
    )
    connect(graph, depending_on_id, node_id)


def assert_ended_before_any_node(run_info, graph, refused: dict[int, str]) -> None:
    errors = {result.node_id: result.error for result in run_info.node_step_result}
    assert errors == refused
    assert all(result.compute_error == "Refusal" for result in run_info.node_step_result)
    assert run_info.success is False and run_info.nodes_completed == 0
    assert graph.hub_vend is None and graph.last_hub_vend is None
    assert graph.flow_settings.is_running is False


def test_a_hub_reader_predicts_the_hub_s_schema_and_reads_only_in_a_run(connected):
    graph = make_graph(8701, execution_location="local")
    add_hub_reader(graph, 1, connected)

    node = graph.get_node(1)
    assert [(c.column_name, c.data_type) for c in node.schema] == [("id", "Int64"), ("name", "String")]
    assert connected.vend_calls == []
    with pytest.raises(ValueError, match="^Run the flow to read a table on the hub$"):
        node.function()


def test_a_hub_reader_of_a_table_on_the_hub_s_disk_is_refused_when_added(connected):
    graph = make_graph(8702, execution_location="local")

    with pytest.raises(ValueError, match="^The hub keeps the tables of scratch.notes on its own disk; this Flowfile"):
        add_hub_reader(graph, 1, connected, table="memo", namespace="notes")


def test_a_run_without_a_hub_session_ends_before_any_node_with_the_reconnect_sentence(store, hub, scans):
    graph = make_graph(8703, execution_location="local")
    add_manual_input(graph, 1, [{"id": 1}])
    add_hub_reader(graph, 2, hub)
    assert graph.get_node(2).results.errors == RECONNECT

    run_info = graph.run_graph()

    assert_ended_before_any_node(run_info, graph, {2: RECONNECT})
    assert hub.vend_calls == [] and scans == []
    assert not any(path in ("/hub/vend", "/catalog/namespaces") for _, path in hub.calls)


def test_a_run_writing_to_a_namespace_on_the_hub_s_disk_ends_with_its_sentence(connected, operations):
    graph = make_graph(8704, execution_location="local")
    add_manual_input(graph, 1, [{"id": 1}])
    add_hub_writer(graph, 2, 1, connected.ids["notes"], "memo_out")

    run_info = graph.run_graph()

    sentence = "The hub keeps the tables of scratch.notes on its own disk; this Flowfile cannot read them"
    assert_ended_before_any_node(run_info, graph, {2: sentence})
    assert connected.vend_calls == [] and operations == []


def test_a_run_the_hub_refuses_ends_with_the_hub_s_reason(connected, operations):
    graph = make_graph(8705, execution_location="local")
    add_manual_input(graph, 1, [{"id": 1}])
    add_hub_writer(graph, 2, 1, connected.ids["raw"], "orders_copy")

    run_info = graph.run_graph()

    sentence = (
        f"The hub did not give this run access to its tables: no manage grant on namespace {connected.ids['raw']}"
    )
    assert_ended_before_any_node(run_info, graph, {2: sentence})
    assert len(connected.vend_calls) == 1 and operations == []


def test_a_run_reading_and_writing_hub_tables_vends_once_and_keeps_nothing(
    connected, scans, operations, monkeypatch, flag
):
    graph = make_graph(8706 + 100 * flag, execution_location="local")
    add_hub_reader(graph, 1, connected)
    add_hub_writer(graph, 2, 1, connected.ids["curated"], "orders_out")
    held: list = []
    recording = FlowNode.run_op

    def holding(self, backend, op, payload, *, job_id):
        held.append(graph.hub_vend)
        return recording(self, backend, op, payload, job_id=job_id)

    monkeypatch.setattr(FlowNode, "run_op", holding)

    run_info = graph.run_graph()

    assert run_info.success is True, [result.error for result in run_info.node_step_result]
    [vend_call] = connected.vend_calls
    assert vend_call["read_namespace_ids"] == [connected.ids["raw"]]
    assert vend_call["write_namespace_ids"] == [connected.ids["curated"]]
    [held_vend] = held
    assert held_vend is not None and vend_call["run_id"] == held_vend.run_id

    [(path, kwargs)] = scans
    assert path == "s3://acme/sales/orders_0a1b2c3d"
    assert isinstance(kwargs["credential_provider"], EncryptedCredentialProvider)
    assert not PROVIDER_CREDENTIAL_KEYS & kwargs["storage_options"].keys()
    assert "vended-secret-key" not in repr(kwargs)

    [(node, op, payload)] = operations
    assert node.node_id == 2 and op is OperationKind.delta_write
    write = unpack(payload, DeltaWritePayload)
    assert write.path is None
    assert isinstance(write.table, CatalogTableIdentity)
    assert write.table.table_id is None and write.table.base_uri == "s3://acme/sales"
    assert write.table.table_name.startswith("orders_out_")
    assert write.connection.aws_secret_access_key.startswith("$ffsec$1$1$")
    assert write.connection.aws_access_key_id == "AKIAHUBVENDED"
    assert write.connection.endpoint_url == "http://minio.example.test:9000"
    assert write.connection.aws_allow_unsafe_html is True

    [registered] = connected.registered
    assert registered == {
        "name": "orders_out",
        "table_path": f"s3://acme/sales/{write.table.table_name}",
        "namespace_id": connected.ids["curated"],
        "description": None,
        "storage_format": "delta",
        "schema_columns": COLUMNS,
        "row_count": 2,
        "column_count": 2,
        "size_bytes": 1234,
    }
    with get_db_context() as db:
        assert db.query(CatalogTable).filter_by(name="orders_out").count() == 0
    assert graph.hub_vend is None
    assert graph.last_hub_vend == (vend_call["run_id"], 1)


def test_an_existing_hub_table_is_written_in_place_and_not_registered_again(connected, operations):
    connected.seed_table(
        {"id": connected.ids["curated"], "name": "curated"},
        "orders_out",
        file_path="s3://acme/sales/orders_out_feedbeef",
    )
    graph = make_graph(8707, execution_location="local")
    add_manual_input(graph, 1, [{"id": 1, "name": "ada"}])
    add_hub_writer(graph, 2, 1, connected.ids["curated"], "orders_out")

    run_info = graph.run_graph()

    assert run_info.success is True, [result.error for result in run_info.node_step_result]
    [(_, _, payload)] = operations
    assert unpack(payload, DeltaWritePayload).table.table_name == "orders_out_feedbeef"
    assert connected.registered == []


def test_an_error_write_onto_an_existing_hub_table_is_refused_before_the_write(connected, operations):
    connected.seed_table(
        {"id": connected.ids["curated"], "name": "curated"}, "orders_out", file_path="s3://acme/sales/x"
    )
    graph = make_graph(8708, execution_location="local")
    add_manual_input(graph, 1, [{"id": 1}])
    add_hub_writer(graph, 2, 1, connected.ids["curated"], "orders_out", mode="error")

    run_info = graph.run_graph()

    errors = {result.node_id: result.error for result in run_info.node_step_result}
    assert "Catalog table 'orders_out' already exists" in errors[2]
    assert operations == [] and connected.registered == []


def test_a_failed_registration_fails_the_writer_naming_where_the_table_was_written(connected, operations, monkeypatch):
    def refuse(url, token, body, **kwargs):
        raise HubRefused(403, "Access denied: table_path must be a table directory in the catalog's storage")

    monkeypatch.setattr(client, "register_table_from_data", refuse)
    graph = make_graph(8709, execution_location="local")
    add_manual_input(graph, 1, [{"id": 1}])
    add_hub_writer(graph, 2, 1, connected.ids["curated"], "orders_out")

    run_info = graph.run_graph()

    errors = {result.node_id: result.error for result in run_info.node_step_result}
    assert errors[2].startswith("The table was written to s3://acme/sales/orders_out_")
    assert errors[2].endswith(
        "but could not be registered on the hub: Access denied: table_path must be a table directory in the catalog's "
        "storage"
    )


def test_a_flow_with_no_hub_table_asks_the_hub_nothing(connected):
    graph = make_graph(8710, execution_location="local")
    add_manual_input(graph, 1, [{"id": 1}])
    graph.last_hub_vend = ("an earlier run", 3)

    run_info = graph.run_graph()

    assert run_info.success is True
    assert connected.calls == []
    assert graph.hub_vend is None and graph.last_hub_vend is None


@pytest.mark.parametrize("mode", ["Performance", "Development"])
def test_after_a_run_over_a_hub_table_no_node_result_holds_its_credential(
    connected, carrying_scans, operations, monkeypatch, flag, mode
):
    flow_id = 8711 + 100 * flag + 10 * (mode == "Development")
    graph = make_graph(flow_id, execution_location="local", execution_mode=mode)
    add_hub_reader(graph, 1, connected)
    add_hub_writer(graph, 2, 1, connected.ids["curated"], "orders_out")
    add_filter_on(graph, 3, 1, "[id] > 1")
    during: list = []
    recording = FlowNode.run_op

    def holding(self, backend, op, payload, *, job_id):
        during.append(graph.get_node(1).results.result_ref)
        return recording(self, backend, op, payload, job_id=job_id)

    monkeypatch.setattr(FlowNode, "run_op", holding)

    run_info = graph.run_graph()

    assert run_info.success is True, [result.error for result in run_info.node_step_result]
    [(_, kwargs)] = carrying_scans
    ciphertext = kwargs["credential_provider"].encrypted_credentials.encode()
    [reader_ref] = during
    for node in graph.nodes:
        plans = [node.results.resulting_data, *node._named_outputs.values()]
        assert not any(ciphertext in data.data_frame.lazy().serialize() for data in plans if data is not None)
        ref = node.results.result_ref
        assert ref is None or ref.state == "materialised"
    reader = graph.get_node(1)
    if reader_ref.state == "deferred":
        assert reader.results.result_ref is None and not reader.node_stats.has_run_with_current_setup
        assert registry.backend(reader_ref.backend_id).exists(reader_ref) is False
    else:
        assert reader.results.result_ref == reader_ref and reader.node_stats.has_run_with_current_setup
    assert mode == "Development" or reader_ref.state == "deferred"
    assert graph.hub_vend is None
