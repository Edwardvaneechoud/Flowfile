"""The cloud storage reader and writer on an Iceberg table kept by a REST catalog, on the worker in this process and
on the session's worker.

The flow names the catalog's address and the table; the process that runs the read or the write asks the catalog for
the table's keys itself, so neither the flow nor the plan core ships holds one. Every test that needs the catalog skips
unless it runs: ``python -m test_utils.iceberg_rest start``.

This process runs with no AWS identity (``clean_aws``). The session's worker is started by the conftest before any test
and keeps the environment it was started with; start the suite with no AWS identity to keep it out of that process too.
"""

import contextlib
import json
import uuid

import polars as pl
import pytest

from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.schemas import cloud_storage_schemas as cloud_ss
from flowfile_core.schemas import input_schema, transform_schema
from shared.iceberg_utils import storage_properties
from test_utils.iceberg_rest import fixtures
from test_utils.s3.aws_profiles import isolate_aws
from tests.flowfile.test_flowfile import add_manual_input, add_node_promise_on_type, create_graph, handle_run_info

pytestmark = pytest.mark.iceberg_rest

SECRET_KEY_NAMES = [b"s3.secret-access-key", b"s3.session-token"]


@pytest.fixture(autouse=True)
def clean_aws(monkeypatch, tmp_path):
    """No ambient AWS key or profile, and a closed endpoint for anything that falls back to the environment."""
    aws = tmp_path / "aws"
    aws.mkdir()
    isolate_aws(monkeypatch, aws)


@pytest.fixture(scope="module")
def rest_catalog():
    if not fixtures.catalog_ready():
        pytest.skip(fixtures.SKIP_REASON)
    from pyiceberg.catalog import load_catalog

    return load_catalog("core_tests", **fixtures.catalog_properties())


@pytest.fixture(scope="module")
def namespace(rest_catalog):
    name = f"core_{uuid.uuid4().hex[:8]}"
    rest_catalog.create_namespace(name)
    yield name
    for identifier in rest_catalog.list_tables(name):
        with contextlib.suppress(Exception):
            rest_catalog.purge_table(identifier)
    with contextlib.suppress(Exception):
        rest_catalog.drop_namespace(name)


def _orders(rows: int, start: int = 0) -> pl.DataFrame:
    return (
        pl.DataFrame({"id": pl.int_range(start, start + rows, dtype=pl.Int64, eager=True)})
        .with_columns(grp=pl.col("id") % 10, val=pl.col("id").cast(pl.Float64) * 0.5,
                      name=pl.format("name-{}", pl.col("id")))
    )


@pytest.fixture
def orders(rest_catalog, namespace):
    """A table of 1000 rows another tool (PyIceberg) made; its name is unique to the test."""
    identifier = f"{namespace}.orders_{uuid.uuid4().hex[:6]}"
    arrow = _orders(1000).to_arrow(compat_level=pl.CompatLevel.oldest())
    rest_catalog.create_table(identifier, schema=arrow.schema).append(arrow)
    return identifier


def _catalog(token_secret: str | None = None) -> cloud_ss.IcebergCatalogSettings:
    return cloud_ss.IcebergCatalogSettings(uri=fixtures.CATALOG_URI, warehouse=fixtures.WAREHOUSE,
                                           token_secret=token_secret)


def _add_reader(graph, table: str, node_id: int = 1, token_secret: str | None = None) -> None:
    add_node_promise_on_type(graph, "cloud_storage_reader", node_id)
    graph.add_cloud_storage_reader(input_schema.NodeCloudStorageReader(
        flow_id=graph.flow_id, node_id=node_id, user_id=1,
        cloud_storage_settings=cloud_ss.CloudStorageReadSettings(
            resource_path=table, file_format="iceberg", iceberg_catalog=_catalog(token_secret)),
    ))


def _add_filter(graph, node_id: int, depends_on: int, formula: str) -> None:
    add_node_promise_on_type(graph, "filter", node_id)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(depends_on, node_id))
    graph.add_filter(input_schema.NodeFilter(
        flow_id=graph.flow_id, node_id=node_id, depending_on_id=depends_on,
        filter_input=transform_schema.FilterInput(advanced_filter=formula, filter_type="advanced"),
    ))


def _handed_keys(rest_catalog, table: str) -> list[str]:
    return fixtures.key_values(storage_properties(rest_catalog.load_table(table)))


def test_a_reader_node_reads_a_catalogs_table_with_the_credential_the_catalog_hands_the_worker(
    rest_catalog, orders, execution_location
):
    graph = create_graph(execution_location=execution_location)
    _add_reader(graph, orders)
    _add_filter(graph, 2, 1, "[grp] = 3")
    add_node_promise_on_type(graph, "select", 3)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))
    graph.add_select(input_schema.NodeSelect(
        flow_id=graph.flow_id, node_id=3, keep_missing=False,
        select_input=[transform_schema.SelectInput(old_name="id"), transform_schema.SelectInput(old_name="val")],
    ))

    assert [column.column_name for column in graph.get_node(1).schema] == ["id", "grp", "val", "name"]
    handle_run_info(graph.run_graph())

    result = graph.get_node(3).get_resulting_data().collect()
    assert result.columns == ["id", "val"]
    kept = [i for i in range(1000) if i % 10 == 3]
    assert (result.height, result["id"].sum()) == (len(kept), sum(kept))


def test_the_plan_core_ships_names_the_table_and_holds_no_key(rest_catalog, orders, execution_location):
    graph = create_graph(execution_location=execution_location)
    _add_reader(graph, orders)
    blob = graph.get_node(1).get_resulting_data().data_frame.serialize()

    keys = _handed_keys(rest_catalog, orders)
    leaked = [index for index, value in enumerate(keys) if value.encode() in blob]
    assert leaked == []
    assert [name for name in SECRET_KEY_NAMES if name in blob] == []
    names_table_and_catalog = orders.encode() in blob and fixtures.CATALOG_URI.encode() in blob
    assert names_table_and_catalog


def test_a_token_travels_as_ciphertext_and_the_read_still_succeeds(rest_catalog, orders, execution_location):
    """The token's stored ciphertext is all the plan holds, and the read succeeds with it.

    This does not prove that a catalog which checks sign-ins accepts the token: the test catalog asks for none.
    """
    from flowfile_core.auth.models import SecretInput
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.secret_manager.secret_manager import get_encrypted_secret, store_secret

    name, clear = f"iceberg_token_{uuid.uuid4().hex[:6]}", f"bearer-{uuid.uuid4().hex}"
    with get_db_context() as db:
        store_secret(db, SecretInput(name=name, value=clear), 1)
    graph = create_graph(execution_location=execution_location)
    _add_reader(graph, orders, token_secret=name)

    blob = graph.get_node(1).get_resulting_data().data_frame.serialize()
    clear_in_plan = clear.encode() in blob
    ciphertext_in_plan = get_encrypted_secret(1, name).encode() in blob
    assert not clear_in_plan
    assert ciphertext_in_plan
    handle_run_info(graph.run_graph())
    assert graph.get_node(1).get_resulting_data().collect().height == 1000


def test_a_multi_user_server_refuses_a_catalog_named_by_its_address(monkeypatch):
    from flowfile_core.flowfile.flow_graph.cloud import ICEBERG_ON_A_SERVER

    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    graph = create_graph(execution_location="local")
    _add_reader(graph, "demo.orders")
    run = graph.run_graph()

    (step,) = [step for step in run.node_step_result if step.node_id == 1]
    assert not step.success and ICEBERG_ON_A_SERVER in step.error


def _add_writer(graph, table: str, node_id: int, depends_on: int, write_mode: str = "append") -> None:
    add_node_promise_on_type(graph, "cloud_storage_writer", node_id)
    graph.add_cloud_storage_writer(input_schema.NodeCloudStorageWriter(
        flow_id=graph.flow_id, node_id=node_id, user_id=1, depending_on_id=depends_on,
        cloud_storage_settings=cloud_ss.CloudStorageWriteSettings(
            resource_path=table, file_format="iceberg", write_mode=write_mode, iceberg_catalog=_catalog()),
    ))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(depends_on, node_id))


def _rows(rest_catalog, table: str) -> int:
    return rest_catalog.load_table(table).scan().to_arrow().num_rows


def test_a_writer_node_creates_a_table_then_appends_to_it(rest_catalog, namespace, execution_location):
    table = f"{namespace}.written_{execution_location}_{uuid.uuid4().hex[:6]}"
    graph = create_graph(execution_location=execution_location)
    add_manual_input(graph, data=[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}])
    _add_writer(graph, table, 2, 1)

    handle_run_info(graph.run_graph())
    assert _rows(rest_catalog, table) == 3
    graph.get_node(2).reset(True)
    handle_run_info(graph.run_graph())
    assert _rows(rest_catalog, table) == 6


def test_a_flow_reads_one_table_and_writes_another(rest_catalog, namespace, orders, execution_location):
    target = f"{orders}_grp3_{execution_location}"
    graph = create_graph(execution_location=execution_location)
    _add_reader(graph, orders)
    _add_filter(graph, 2, 1, "[grp] = 3")
    _add_writer(graph, target, 3, 2, write_mode="overwrite")

    handle_run_info(graph.run_graph())

    written = rest_catalog.load_table(target).scan().to_arrow()
    assert written.num_rows == 100 and set(written.column("grp").to_pylist()) == {3}
    assert written.schema.names == ["id", "grp", "val", "name"]


def test_the_write_request_holds_the_tables_name_and_no_key(rest_catalog, orders):
    """The body core posts to the worker names the table and its catalog; a token is only its stored ciphertext."""
    from flowfile_core.flowfile.execution.backends.worker import _iceberg_write_body, _json_body
    from flowfile_core.flowfile.execution.payloads import IcebergWritePayload
    from shared.iceberg_utils import IcebergTableRef

    ciphertext = "$ffsec$1$1$not-a-real-token"
    write = IcebergWritePayload(
        plan=_orders(10).lazy(), table=IcebergTableRef(fixtures.CATALOG_URI, orders, fixtures.WAREHOUSE, ciphertext),
        mode="append", flow_id=1, node_id=3,
    )
    sent = json.dumps(_json_body(_iceberg_write_body(write)))

    keys = _handed_keys(rest_catalog, orders)
    assert [index for index, value in enumerate(keys) if value in sent] == []
    assert [name for name in SECRET_KEY_NAMES if name.decode() in sent] == []
    names_table_catalog_and_ciphertext = orders in sent and fixtures.CATALOG_URI in sent and ciphertext in sent
    assert names_table_catalog_and_ciphertext


def test_a_version_3_table_fails_the_writer_node_with_the_sentence(rest_catalog, namespace):
    table = f"{namespace}.v3_{uuid.uuid4().hex[:6]}"
    arrow = _orders(10).to_arrow(compat_level=pl.CompatLevel.oldest())
    rest_catalog.create_table(table, schema=arrow.schema, properties={"format-version": "3"})
    graph = create_graph(execution_location="local")
    add_manual_input(graph, data=_orders(10).to_dicts())
    _add_writer(graph, table, 2, 1)

    run = graph.run_graph()

    (step,) = [step for step in run.node_step_result if step.node_id == 2]
    assert not step.success
    assert "is an Iceberg version 3 table, which this Flowfile reads and cannot write yet" in step.error
    assert rest_catalog.load_table(table).current_snapshot() is None


def test_a_rerun_reads_again_only_when_the_table_has_a_new_commit(
    rest_catalog, namespace, orders, execution_location, monkeypatch
):
    """Each run asks the catalog whether the table changed: an unchanged table keeps the reader's result, a commit by
    another engine (rows appended, then a column added) makes the reader read it again."""
    from shared import iceberg_utils

    loads = []
    load = iceberg_utils.load_table
    monkeypatch.setattr(iceberg_utils, "load_table", lambda ref: (loads.append(ref.table), load(ref))[1])
    graph = create_graph(execution_location=execution_location)
    _add_reader(graph, orders)
    _add_filter(graph, 2, 1, "[grp] = 3")

    handle_run_info(graph.run_graph())
    first = graph.get_node(1).hash
    assert graph.get_node(2).get_resulting_data().collect().height == 100

    before = len(loads)
    handle_run_info(graph.run_graph())
    assert len(loads) - before == 1, "an unchanged table should cost one catalog load in core"
    assert graph.get_node(1).hash == first
    assert graph.get_node(2).get_resulting_data().collect().height == 100

    rest_catalog.load_table(orders).append(_orders(1000, 1000).to_arrow(compat_level=pl.CompatLevel.oldest()))
    handle_run_info(graph.run_graph())
    appended = graph.get_node(1).hash
    assert appended != first
    assert graph.get_node(2).get_resulting_data().collect().height == 200

    from pyiceberg.types import StringType

    with rest_catalog.load_table(orders).update_schema() as update:
        update.add_column("note", StringType())
    handle_run_info(graph.run_graph())
    assert graph.get_node(1).hash != appended
    assert graph.get_node(2).get_resulting_data().columns == ["id", "grp", "val", "name", "note"]


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
def signed_orders(signed):
    """A namespace on Polaris holding ``orders``, 1000 rows; ``(namespace, "namespace.orders")``."""
    from shared.iceberg_utils import write_table

    name = f"flow_{uuid.uuid4().hex[:8]}"
    signed.create_namespace(name)
    arrow = _orders(1).to_arrow(compat_level=pl.CompatLevel.oldest())
    table = signed.create_table(f"{name}.orders", schema=arrow.schema)
    write_table(table, _orders(1000).lazy(), "append")
    yield name, f"{name}.orders"
    for identifier in signed.list_tables(name):
        with contextlib.suppress(Exception):
            signed.drop_table(identifier)
    with contextlib.suppress(Exception):
        signed.drop_namespace(name)


def _add_external(body: dict) -> str:
    """Add a catalog kept elsewhere through the catalog service, as user 1; returns its name."""
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.schemas.catalog_schema import ExternalCatalogCreate

    with get_db_context() as db:
        CatalogService(SQLAlchemyCatalogRepository(db)).create_external_catalog(ExternalCatalogCreate(**body), 1)
    return body["name"]


def _remove_external(name: str) -> None:
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import CatalogNamespace

    with get_db_context() as db:
        row = db.query(CatalogNamespace).filter_by(name=name, parent_id=None).first()
        if row is not None:
            CatalogService(SQLAlchemyCatalogRepository(db)).delete_external_catalog(row.id)


@pytest.fixture(scope="module")
def polaris(principal, signed_orders):
    name = _add_external({
        "name": f"polaris_{uuid.uuid4().hex[:6]}", "uri": fixtures.SIGNED_CATALOG_URI,
        "warehouse": fixtures.SIGNED_WAREHOUSE, "sign_in": "client_credentials",
        "client_id": principal.client_id, "client_secret": principal.client_secret, "scope": fixtures.SIGNED_SCOPE,
    })
    yield name
    _remove_external(name)


def _named(catalog: str) -> cloud_ss.IcebergCatalogSettings:
    return cloud_ss.IcebergCatalogSettings(catalog=catalog)


def _add_named_reader(graph, table: str, catalog: str, node_id: int = 1, user_id: int = 1) -> None:
    add_node_promise_on_type(graph, "cloud_storage_reader", node_id)
    graph.add_cloud_storage_reader(input_schema.NodeCloudStorageReader(
        flow_id=graph.flow_id, node_id=node_id, user_id=user_id,
        cloud_storage_settings=cloud_ss.CloudStorageReadSettings(
            resource_path=table, file_format="iceberg", iceberg_catalog=_named(catalog)),
    ))


def _add_named_writer(graph, table: str, catalog: str, node_id: int, depends_on: int, user_id: int = 1) -> None:
    add_node_promise_on_type(graph, "cloud_storage_writer", node_id)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(depends_on, node_id))
    graph.add_cloud_storage_writer(input_schema.NodeCloudStorageWriter(
        flow_id=graph.flow_id, node_id=node_id, user_id=user_id, depending_on_id=depends_on,
        cloud_storage_settings=cloud_ss.CloudStorageWriteSettings(
            resource_path=table, file_format="iceberg", write_mode="overwrite", iceberg_catalog=_named(catalog)),
    ))


def _signed_rows(signed, identifier: str) -> tuple[int, int]:
    from shared.iceberg_utils import scan_table, table_access

    counted = scan_table(table_access(signed.load_table(identifier))).select(pl.len(), pl.col("id").sum()).collect()
    return int(counted.item(0, 0)), int(counted.item(0, 1) or 0)


def _forms(principal, *tokens) -> _Values:
    return _Values([principal.client_secret, principal.credential, *tokens])


def test_a_reader_and_a_writer_that_name_the_catalog_read_and_write_its_tables(
    signed, signed_orders, polaris, execution_location
):
    namespace, orders = signed_orders
    target = f"{namespace}.grp3_{execution_location}"
    graph = create_graph(execution_location=execution_location)
    _add_named_reader(graph, orders, polaris)
    _add_filter(graph, 2, 1, "[grp] = 3")
    _add_named_writer(graph, target, polaris, 3, 2)

    assert [column.column_name for column in graph.get_node(1).schema] == ["id", "grp", "val", "name"]
    handle_run_info(graph.run_graph())

    kept = [i for i in range(1000) if i % 10 == 3]
    assert _signed_rows(signed, target) == (len(kept), sum(kept))


def test_the_plan_and_the_write_request_hold_ciphertext_and_no_part_of_the_sign_in(
    principal, signed_orders, polaris, monkeypatch
):
    from flowfile_core.flowfile.flow_node.flow_node import FlowNode

    namespace, orders = signed_orders
    sent = []
    run_op = FlowNode.run_op

    def recording(self, backend, kind, payload, **kwargs):
        sent.append(payload)
        return run_op(self, backend, kind, payload, **kwargs)

    monkeypatch.setattr(FlowNode, "run_op", recording)
    graph = create_graph(execution_location="local")
    _add_named_reader(graph, orders, polaris)
    _add_named_writer(graph, f"{namespace}.plan_check", polaris, 2, 1)
    blob = graph.get_node(1).get_resulting_data().data_frame.serialize()
    handle_run_info(graph.run_graph())

    token, _ = fixtures.fetch_token(principal)
    forms = _forms(principal, token)
    in_plan = [index for index, value in enumerate(forms) if value.encode() in blob]
    assert in_plan == []
    carried = [payload.get("payload") for payload in sent if isinstance(payload, dict)]
    tables = [item.table for item in carried if hasattr(getattr(item, "table", None), "token")]
    assert len(tables) == 1
    request = json.dumps({"uri": tables[0].catalog_uri, "table": tables[0].table, "token": tables[0].token})
    in_request = [index for index, value in enumerate(forms) if value in request]
    assert in_request == []
    ciphertext = tables[0].token.startswith("$ffsec$1$")
    assert ciphertext and tables[0].token.encode() in blob


def _skip_when_tokens_live_long():
    if fixtures.token_seconds() > 30:
        pytest.skip(f"the catalog's tokens live {fixtures.token_seconds()} seconds; start it with the default lifetime")


def test_a_flow_run_after_the_token_ran_out_still_works_with_a_client_id_and_secret(signed_orders, polaris):
    import time

    _skip_when_tokens_live_long()
    _, orders = signed_orders
    graph = create_graph(execution_location="local")
    _add_named_reader(graph, orders, polaris)
    time.sleep(fixtures.token_seconds() + 1)

    handle_run_info(graph.run_graph())

    assert graph.get_node(1).get_resulting_data().collect().height == 1000


def test_a_flow_with_a_pasted_token_that_ran_out_fails_with_the_sentence(principal, signed_orders):
    import time

    _skip_when_tokens_live_long()
    _, orders = signed_orders
    token, lifetime = fixtures.fetch_token(principal)
    name = _add_external({
        "name": f"pasted_{uuid.uuid4().hex[:6]}", "uri": fixtures.SIGNED_CATALOG_URI,
        "warehouse": fixtures.SIGNED_WAREHOUSE, "sign_in": "token", "token": token,
    })
    try:
        graph = create_graph(execution_location="local")
        _add_named_reader(graph, orders, name)
        time.sleep(lifetime + 1)
        run = graph.run_graph()
    finally:
        _remove_external(name)

    (step,) = [step for step in run.node_step_result if step.node_id == 1]
    said = step.error or ""
    assert not step.success
    assert "refused the sign-in: it is wrong or has run out" in said
    leaked = [index for index, value in enumerate(_forms(principal, token)) if value in said]
    assert leaked == []


def test_a_write_that_outlasts_the_token_commits_with_a_client_id_and_secret(signed, signed_orders, polaris):
    _skip_when_tokens_live_long()
    namespace, orders = signed_orders
    target = f"{namespace}.slow_copy"
    seconds = fixtures.token_seconds() + 3
    graph = create_graph(execution_location="local")
    _add_named_reader(graph, orders, polaris)
    add_node_promise_on_type(graph, "polars_code", 2)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_polars_code(input_schema.NodePolarsCode(
        flow_id=graph.flow_id, node_id=2, depending_on_ids=[1],
        polars_code_input=transform_schema.PolarsCodeInput(
            polars_code=f"output_df = input_df.map_batches(lambda batch: (time.sleep({seconds}), batch)[1])"),
    ))
    _add_named_writer(graph, target, polaris, 3, 2)

    handle_run_info(graph.run_graph())

    assert _signed_rows(signed, target) == (1000, sum(range(1000)))


def test_an_unknown_catalog_name_fails_the_node_with_the_sentence():
    graph = create_graph(execution_location="local")
    _add_named_reader(graph, "sales.orders", "no_such_catalog")
    run = graph.run_graph()

    (step,) = [step for step in run.node_step_result if step.node_id == 1]
    assert not step.success
    assert "No catalog named 'no_such_catalog' is kept elsewhere in Flowfile's catalog." in (step.error or "")
