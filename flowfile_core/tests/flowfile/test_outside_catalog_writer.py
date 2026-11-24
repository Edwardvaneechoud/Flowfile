"""The Catalog Writer on a schema of a catalog kept elsewhere (Apache Polaris), on the worker in this process and on the
session's worker.

The node names its target as it names one of Flowfile's own, ``catalog.schema`` and a table name, and the write is
step 2's ``iceberg_write``: the compute that writes asks the catalog for the table's keys, so neither core's payload
nor the saved flow holds one. Only Overwrite and Append write; every other choice is refused with one sentence before
anything is written. After a write the table has its lineage row, naming the flow as its writer.

Every test that needs Polaris skips unless it runs: ``python -m test_utils.iceberg_rest start``. This process runs
with no AWS identity (``clean_aws``).
"""

import contextlib
import json
import uuid

import pytest
from fastapi.testclient import TestClient

from flowfile_core.schemas import input_schema
from shared.iceberg_utils import storage_properties
from test_utils.iceberg_rest import fixtures
from test_utils.s3.aws_profiles import isolate_aws
from tests.flowfile.conftest import add_test_manual_input
from tests.flowfile.test_flowfile import add_node_promise_on_type, create_graph, handle_run_info

SECRET_KEY_NAMES = ["s3.secret-access-key", "s3.session-token"]


@pytest.fixture(autouse=True)
def clean_aws(monkeypatch, tmp_path):
    """No ambient AWS key or profile, and a closed endpoint for anything that falls back to the environment."""
    aws = tmp_path / "aws"
    aws.mkdir()
    isolate_aws(monkeypatch, aws)


class _Values(list):
    """Secret values a test searches for; its ``repr`` holds none of them."""

    def __repr__(self) -> str:
        return f"<{len(self)} secret values>"


@pytest.fixture(scope="module")
def principal():
    if not fixtures.signed_catalog_ready():
        pytest.skip(fixtures.SIGNED_SKIP_REASON)
    made = fixtures.new_principal()
    yield made
    fixtures.drop_principal(made.name)


@pytest.fixture(scope="module")
def signed(principal):
    return fixtures.signed_catalog(principal)


@pytest.fixture(scope="module")
def schema(signed):
    """A namespace of this module's own on Polaris; dropped with its tables after the module."""
    name = f"writer_{uuid.uuid4().hex[:8]}"
    signed.create_namespace(name)
    yield name
    for identifier in signed.list_tables(name):
        with contextlib.suppress(Exception):
            signed.drop_table(identifier)
    with contextlib.suppress(Exception):
        signed.drop_namespace(name)


@pytest.fixture(scope="module")
def polaris(principal, schema):
    """Polaris added to Flowfile's catalog with a client id and secret, by user 1: ``(name, catalog id, schema id)``."""
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import CatalogNamespace
    from flowfile_core.schemas.catalog_schema import ExternalCatalogCreate

    name = f"polaris_{uuid.uuid4().hex[:6]}"
    body = ExternalCatalogCreate(
        name=name, uri=fixtures.SIGNED_CATALOG_URI, warehouse=fixtures.SIGNED_WAREHOUSE,
        sign_in="client_credentials", client_id=principal.client_id, client_secret=principal.client_secret,
        scope=fixtures.SIGNED_SCOPE,
    )
    with get_db_context() as db:
        CatalogService(SQLAlchemyCatalogRepository(db)).create_external_catalog(body, 1)
        catalog = db.query(CatalogNamespace).filter_by(name=name, parent_id=None).one()
        schema_id = db.query(CatalogNamespace).filter_by(parent_id=catalog.id, name=schema).one().id
        catalog_id = catalog.id
    yield name, catalog_id, schema_id
    with get_db_context() as db:
        row = db.query(CatalogNamespace).filter_by(name=name, parent_id=None).first()
        if row is not None:
            CatalogService(SQLAlchemyCatalogRepository(db)).delete_external_catalog(row.id)


def _rows(start: int, count: int) -> list[dict]:
    return [{"id": i, "grp": i % 10, "name": f"name-{i}"} for i in range(start, start + count)]


def _writer_flow(execution_location: str, rows: list[dict], namespace: str | None, table: str, **settings):
    """A manual input of ``rows`` into a Catalog Writer on ``namespace`` (``catalog.schema``), table ``table``.

    Named as the writer's drawer saves it: by the name alone, no namespace id; ``settings`` may give one instead.
    """
    graph = create_graph(execution_location=execution_location)
    add_test_manual_input(graph, rows, node_id=1)
    add_node_promise_on_type(graph, "catalog_writer", 2)
    from flowfile_core.flowfile.flow_graph import add_connection

    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_catalog_writer(input_schema.NodeCatalogWriter(
        flow_id=graph.flow_id, node_id=2, user_id=1, depending_on_id=1,
        catalog_write_settings=input_schema.CatalogWriteSettings(
            table_name=table, namespace_full_name=namespace, **settings,
        ),
    ))
    return graph


def _seen_by_pyiceberg(signed, identifier: str) -> list[int]:
    """The ids of the table's rows as PyIceberg reads them, asking the catalog itself."""
    return sorted(signed.load_table(identifier).scan().to_arrow().column("id").to_pylist())


def _step(run, node_id: int = 2) -> tuple[bool, str]:
    (step,) = [step for step in run.node_step_result if step.node_id == node_id]
    return step.success, step.error or ""


def test_append_creates_the_table_when_missing_and_then_appends(polaris, schema, signed, execution_location):
    name, _, _ = polaris
    table = f"appended_{uuid.uuid4().hex[:6]}"

    handle_run_info(_writer_flow(execution_location, _rows(0, 3), f"{name}.{schema}", table,
                                 write_mode="append").run_graph())
    created = _seen_by_pyiceberg(signed, f"{schema}.{table}")
    handle_run_info(_writer_flow(execution_location, _rows(3, 2), f"{name}.{schema}", table,
                                 write_mode="append").run_graph())

    assert created == [0, 1, 2]
    assert _seen_by_pyiceberg(signed, f"{schema}.{table}") == [0, 1, 2, 3, 4]


def test_overwrite_replaces_the_rows(polaris, schema, signed, execution_location):
    name, _, _ = polaris
    table = f"replaced_{uuid.uuid4().hex[:6]}"

    handle_run_info(_writer_flow(execution_location, _rows(0, 5), f"{name}.{schema}", table).run_graph())
    handle_run_info(_writer_flow(execution_location, _rows(10, 2), f"{name}.{schema}", table).run_graph())

    assert _seen_by_pyiceberg(signed, f"{schema}.{table}") == [10, 11]


def test_the_drawers_name_only_shape_and_a_schema_row_id_both_write(polaris, schema, signed):
    """The drawer saves ``namespace_id`` null, the name, ``source`` local; the Python API may send the row's id."""
    name, _, schema_id = polaris
    by_name, by_id = f"by_name_{uuid.uuid4().hex[:6]}", f"by_id_{uuid.uuid4().hex[:6]}"
    drawer = {"namespace_id": None, "source": "local", "write_mode": "append", "track_changes": False,
              "partition_by": [], "merge_keys": []}

    handle_run_info(_writer_flow("local", _rows(0, 3), f"{name}.{schema}", by_name, **drawer).run_graph())
    handle_run_info(_writer_flow("local", _rows(0, 2), None, by_id, namespace_id=schema_id,
                                 write_mode="overwrite").run_graph())

    assert _seen_by_pyiceberg(signed, f"{schema}.{by_name}") == [0, 1, 2]
    assert _seen_by_pyiceberg(signed, f"{schema}.{by_id}") == [0, 1]


REFUSED = {
    "upsert": ({"write_mode": "upsert", "merge_keys": ["id"]}, "Upsert"),
    "update": ({"write_mode": "update", "merge_keys": ["id"]}, "Update"),
    "delete": ({"write_mode": "delete", "merge_keys": ["id"]}, "Delete"),
    "scd2": ({"write_mode": "scd2", "merge_keys": ["id"]}, "SCD2"),
    "error if exists": ({"write_mode": "error"}, "Error if exists"),
    "virtual": ({"write_mode": "virtual"}, "Virtual Table"),
    "track changes": ({"write_mode": "append", "track_changes": True}, "Track changes"),
    "partition by": ({"write_mode": "append", "partition_by": ["grp"]}, "Partition by"),
}


@pytest.mark.parametrize("case", list(REFUSED))
def test_each_choice_but_overwrite_and_append_is_refused_before_anything_is_written(polaris, schema, signed, case):
    from flowfile_core.catalog.services.external_catalogs import outside_write_refusal

    name, _, _ = polaris
    table = f"refused_{uuid.uuid4().hex[:6]}"
    settings, choice = REFUSED[case]
    run = _writer_flow("local", _rows(0, 3), f"{name}.{schema}", table, **settings).run_graph()

    sentence = f"{name} keeps this table. Flowfile can Append to it or Overwrite it; {choice} is not available there."
    assert _step(run) == (False, sentence)
    worded_by_one_function = outside_write_refusal(
        name, table=table, write_mode=settings["write_mode"], track_changes=settings.get("track_changes", False),
        partition_by=settings.get("partition_by", []),
    )
    assert worded_by_one_function == sentence
    assert not signed.table_exists(f"{schema}.{table}")


def test_neither_the_payload_core_sends_nor_the_saved_flow_holds_a_key_or_the_sign_in(
    principal, signed, polaris, schema, monkeypatch, tmp_path
):
    from flowfile_core.flowfile.execution.backends.worker import _iceberg_write_body, _json_body
    from flowfile_core.flowfile.execution.protocol import OperationKind
    from flowfile_core.flowfile.flow_node.flow_node import FlowNode

    name, _, _ = polaris
    table = f"no_keys_{uuid.uuid4().hex[:6]}"
    sent = []
    run_op = FlowNode.run_op

    def recording(self, backend, kind, payload, **kwargs):
        sent.append((kind, payload))
        return run_op(self, backend, kind, payload, **kwargs)

    monkeypatch.setattr(FlowNode, "run_op", recording)
    graph = _writer_flow("local", _rows(0, 3), f"{name}.{schema}", table, write_mode="append")
    handle_run_info(graph.run_graph())
    saved = tmp_path / "outside_writer.yaml"
    graph.save_flow(str(saved))

    keys = fixtures.key_values(storage_properties(signed.load_table(f"{schema}.{table}")))
    token, _ = fixtures.fetch_token(principal)
    forms = _Values([principal.client_secret, principal.credential, token, *keys])
    writes = [payload["payload"] for kind, payload in sent if kind == OperationKind.iceberg_write]
    assert len(writes) == 1
    body = json.dumps(_json_body(_iceberg_write_body(writes[0])))
    assert [index for index, value in enumerate(forms) if value in body] == []
    assert [key for key in SECRET_KEY_NAMES if key in body] == []
    assert writes[0].table.token.startswith("$ffsec$1$") and writes[0].mode == "append"
    text = saved.read_text()
    assert [index for index, value in enumerate(forms) if value in text] == []
    holds_no_address_and_no_ciphertext = fixtures.SIGNED_CATALOG_URI not in text and "$ffsec$" not in text
    assert holds_no_address_and_no_ciphertext
    assert f"namespace_full_name: {name}.{schema}" in text


@pytest.fixture
def writer_registration(tmp_path):
    """A flow registration of this test's own: ``(registration id, name, path)``."""
    from flowfile_core.catalog import SQLAlchemyCatalogRepository
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import FlowRegistration

    name = f"outside_writer_{uuid.uuid4().hex[:6]}"
    path = tmp_path / f"{name}.yaml"
    with get_db_context() as db:
        registration = FlowRegistration(name=name, flow_path=str(path), owner_id=1)
        db.add(registration)
        db.commit()
        registration_id = registration.id
    yield registration_id, name, path
    with get_db_context() as db:
        SQLAlchemyCatalogRepository(db).delete_flow(registration_id)


def _api() -> TestClient:
    from flowfile_core import main

    client = TestClient(main.app)
    token = client.post("/auth/token").json()["access_token"]
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


def test_the_saved_flow_produces_the_table_and_the_tables_detail_names_the_flow(
    polaris, schema, writer_registration, execution_location
):
    name, catalog_id, schema_id = polaris
    registration_id, flow_name, path = writer_registration
    table = f"lineage_{uuid.uuid4().hex[:6]}"
    graph = _writer_flow(execution_location, _rows(0, 4), f"{name}.{schema}", table, write_mode="append")
    graph.flow_settings.source_registration_id = registration_id

    handle_run_info(graph.run_graph())
    graph.save_flow(str(path))
    api = _api()
    flow = api.get(f"/catalog/flows/{registration_id}").json()
    detail = api.get(f"/catalog/external/{catalog_id}/schemas/{schema_id}/table", params={"name": table}).json()

    assert [(t["name"], t["namespace_id"]) for t in flow["tables_produced"]] == [(table, schema_id)]
    assert detail["written_by"] == {"id": registration_id, "name": flow_name}
    listed = [t["name"] for t in api.get("/catalog/tables").json() if t["namespace_id"] == schema_id]
    assert listed == []


def test_the_run_lists_the_new_table_at_once(polaris, schema):
    """A write drops the schema's kept table names, so the tree lists a table the run just made."""
    name, catalog_id, schema_id = polaris
    api = _api()
    before = api.get(f"/catalog/external/{catalog_id}/schemas/{schema_id}/tables").json()
    table = f"listed_{uuid.uuid4().hex[:6]}"

    handle_run_info(_writer_flow("local", _rows(0, 2), f"{name}.{schema}", table, write_mode="append").run_graph())
    after = api.get(f"/catalog/external/{catalog_id}/schemas/{schema_id}/tables").json()

    assert table not in [t["name"] for t in before["tables"]]
    assert table in [t["name"] for t in after["tables"]] and after["cached"] is False


def test_a_writer_on_a_delta_table_saves_and_writes_as_before(tmp_path):
    """A writer on a schema of Flowfile's own keeps its settings as given and writes a Delta table with a row."""
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import CatalogNamespace, CatalogTable
    from flowfile_core.flowfile.flow_graph import add_connection

    with get_db_context() as db:
        catalog = CatalogNamespace(name=f"own_{uuid.uuid4().hex[:6]}", level=0, owner_id=1)
        db.add(catalog)
        db.commit()
        own_schema = CatalogNamespace(name="sch", level=1, parent_id=catalog.id, owner_id=1)
        db.add(own_schema)
        db.commit()
        ids = catalog.id, own_schema.id, catalog.name
    try:
        graph = create_graph(execution_location="local")
        add_test_manual_input(graph, [{"id": 1}, {"id": 2}], node_id=1)
        add_node_promise_on_type(graph, "catalog_writer", 2)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
        given = input_schema.NodeCatalogWriter(
            flow_id=graph.flow_id, node_id=2, user_id=1, depending_on_id=1,
            catalog_write_settings=input_schema.CatalogWriteSettings(
                table_name="kept_here", namespace_full_name=f"{ids[2]}.sch",
            ),
        )
        expected = given.model_dump()
        graph.add_catalog_writer(given)
        handle_run_info(graph.run_graph())

        assert graph.get_node(2).setting_input.model_dump() == expected
        with get_db_context() as db:
            row = db.query(CatalogTable).filter_by(namespace_id=ids[1], name="kept_here").one()
            assert (row.storage_format, row.row_count) == ("delta", 2)
    finally:
        with get_db_context() as db:
            service = CatalogService(SQLAlchemyCatalogRepository(db))
            for row in db.query(CatalogTable).filter_by(namespace_id=ids[1]).all():
                service.delete_table(row.id, delete_file=True)
            service.delete_namespace(ids[1])
            service.delete_namespace(ids[0])


@pytest.mark.parametrize("write_mode", ["append", "virtual"])
def test_a_writer_whose_schema_resolves_nowhere_is_refused_and_writes_no_table(write_mode):
    """A writer saved on ``polaris / sales`` once ``polaris`` is gone (removed, renamed, another install) writes nothing.

    It used to write a Delta table into no namespace at all. Needs no catalog running.
    """
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import CatalogTable

    table = f"gone_{uuid.uuid4().hex[:6]}"
    run = _writer_flow("local", _rows(0, 2), "polaris_gone.sales", table, namespace_id=None, source="local",
                       write_mode=write_mode).run_graph()

    assert _step(run) == (False, "There is no catalog schema called polaris_gone.sales here; pick the schema again.")
    with get_db_context() as db:
        assert db.query(CatalogTable).filter_by(name=table).count() == 0
