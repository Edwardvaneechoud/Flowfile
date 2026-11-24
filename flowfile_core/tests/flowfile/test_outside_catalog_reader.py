"""The Catalog Reader on a table of a catalog kept elsewhere (Apache Polaris), on the worker in this process and on the
session's worker.

The node names the table as it names one of Flowfile's own, ``catalog.schema.table``. Its columns come from the other
catalog's description before anything runs, and no storage key is asked for to get them; the plan core ships holds
the table's name and the sign-in's ciphertext only; each run asks the catalog whether the table has a new commit.

Every test that needs Polaris skips unless it runs: ``python -m test_utils.iceberg_rest start``. This process runs
with no AWS identity (``clean_aws``).
"""

import contextlib
import uuid

import polars as pl
import pytest

from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.schemas import input_schema, transform_schema
from shared.iceberg_utils import storage_properties
from test_utils.iceberg_rest import fixtures
from test_utils.s3.aws_profiles import isolate_aws
from tests.flowfile.test_flowfile import add_node_promise_on_type, create_graph, handle_run_info

SECRET_KEY_NAMES = [b"s3.secret-access-key", b"s3.session-token"]


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


def _orders(rows: int, start: int = 0) -> pl.LazyFrame:
    return (
        pl.LazyFrame()
        .select(pl.int_range(start, start + rows, dtype=pl.Int64).alias("id"))
        .with_columns(grp=pl.col("id") % 10, val=pl.col("id").cast(pl.Float64) * 0.5,
                      name=pl.format("name-{}", pl.col("id")))
    )


@pytest.fixture(scope="module")
def schema(signed):
    """A namespace of this module's own on Polaris; dropped with its tables after the module."""
    name = f"reader_{uuid.uuid4().hex[:8]}"
    signed.create_namespace(name)
    yield name
    for identifier in signed.list_tables(name):
        with contextlib.suppress(Exception):
            signed.drop_table(identifier)
    with contextlib.suppress(Exception):
        signed.drop_namespace(name)


@pytest.fixture(scope="module")
def polaris(principal, schema):
    """Polaris added to Flowfile's catalog with a client id and secret, by user 1; its name."""
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
    yield name
    with get_db_context() as db:
        row = db.query(CatalogNamespace).filter_by(name=name, parent_id=None).first()
        if row is not None:
            CatalogService(SQLAlchemyCatalogRepository(db)).delete_external_catalog(row.id)


@pytest.fixture
def orders(signed, schema):
    """``orders_<hex>``, 1000 rows another engine (PyIceberg) wrote; its name in the schema."""
    from shared.iceberg_utils import write_table

    table = f"orders_{uuid.uuid4().hex[:6]}"
    frame = _orders(1000)
    made = signed.create_table(f"{schema}.{table}",
                               schema=frame.collect().to_arrow(compat_level=pl.CompatLevel.oldest()).schema)
    write_table(made, frame, "append")
    return table


def _add_reader(graph, reference: str, node_id: int = 1, **fields) -> None:
    add_node_promise_on_type(graph, "catalog_reader", node_id)
    graph.add_catalog_reader(input_schema.NodeCatalogReader(
        flow_id=graph.flow_id, node_id=node_id, user_id=1, catalog_full_table_name=reference, **fields,
    ))


def _add_filter(graph, node_id: int, depends_on: int, formula: str) -> None:
    add_node_promise_on_type(graph, "filter", node_id)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(depends_on, node_id))
    graph.add_filter(input_schema.NodeFilter(
        flow_id=graph.flow_id, node_id=node_id, depending_on_id=depends_on,
        filter_input=transform_schema.FilterInput(advanced_filter=formula, filter_type="advanced"),
    ))


def _asked_for_keys(monkeypatch) -> list[bool]:
    """Whether each catalog client this process builds from now on asks the catalog to hand out storage keys."""
    from shared import iceberg_utils

    asked: list[bool] = []
    build = iceberg_utils._catalog

    def recording(*args, **kwargs):
        asked.append(kwargs.get("keys", True))
        return build(*args, **kwargs)

    monkeypatch.setattr(iceberg_utils, "_catalog", recording)
    return asked


def test_the_columns_are_there_before_any_run_and_no_storage_key_was_asked_for(
    polaris, schema, orders, execution_location, monkeypatch
):
    asked = _asked_for_keys(monkeypatch)
    graph = create_graph(execution_location=execution_location)
    _add_reader(graph, f"{polaris}.{schema}.{orders}")
    _add_filter(graph, 2, 1, "[grp] = 3")

    columns = [column.column_name for column in graph.get_node(1).schema]
    downstream = [column.column_name for column in graph.get_node(2).schema]

    assert columns == ["id", "grp", "val", "name"]
    assert downstream == ["id", "grp", "val", "name"]
    assert asked and True not in asked, f"catalog clients asked for keys: {asked}"
    assert not graph.get_node(1).node_stats.has_run_with_current_setup


def test_a_run_reads_the_table_with_the_catalogs_credential(polaris, schema, orders, execution_location):
    graph = create_graph(execution_location=execution_location)
    _add_reader(graph, f"{polaris}.{schema}.{orders}")
    _add_filter(graph, 2, 1, "[grp] = 3")

    handle_run_info(graph.run_graph())

    result = graph.get_node(2).get_resulting_data().collect()
    kept = [i for i in range(1000) if i % 10 == 3]
    assert (result.height, result["id"].sum()) == (len(kept), sum(kept))
    assert result.columns == ["id", "grp", "val", "name"]


def test_the_plan_core_ships_holds_no_key_and_no_part_of_the_sign_in(
    principal, signed, polaris, schema, orders, execution_location
):
    graph = create_graph(execution_location=execution_location)
    _add_reader(graph, f"{polaris}.{schema}.{orders}")

    blob = graph.get_node(1).get_resulting_data().data_frame.serialize()

    keys = _Values(fixtures.key_values(storage_properties(signed.load_table(f"{schema}.{orders}"))))
    token, _ = fixtures.fetch_token(principal)
    forms = _Values([principal.client_secret, principal.credential, token, *keys])
    assert [index for index, value in enumerate(forms) if value.encode() in blob] == []
    assert [name for name in SECRET_KEY_NAMES if name in blob] == []
    names_the_table = f"{schema}.{orders}".encode() in blob and fixtures.SIGNED_CATALOG_URI.encode() in blob
    assert names_the_table and b"$ffsec$1$" in blob


def test_a_new_commit_by_another_engine_is_read_on_the_next_run(signed, polaris, schema, orders, execution_location):
    from shared.iceberg_utils import write_table

    graph = create_graph(execution_location=execution_location)
    _add_reader(graph, f"{polaris}.{schema}.{orders}")
    _add_filter(graph, 2, 1, "[grp] = 3")

    handle_run_info(graph.run_graph())
    first = graph.get_node(1).hash
    handle_run_info(graph.run_graph())
    unchanged = graph.get_node(1).hash
    write_table(signed.load_table(f"{schema}.{orders}"), _orders(1000, 1000), "append")
    handle_run_info(graph.run_graph())

    assert unchanged == first
    assert graph.get_node(1).hash != first
    assert graph.get_node(2).get_resulting_data().collect().height == 200


@pytest.mark.parametrize(
    ("fields", "sentence"),
    [
        ({"cdc_mode": "since_version", "cdc_from_version": 0},
         "keeps this table. Flowfile can read it only as it is now; reading its changes is not available there."),
        ({"delta_version": 1},
         "keeps this table. Flowfile can read it only as it is now; reading it at a version is not available there."),
    ],
    ids=["changes", "version"],
)
def test_change_reads_and_a_version_are_refused_with_a_sentence(polaris, schema, orders, fields, sentence):
    reference = f"{polaris}.{schema}.{orders}"
    graph = create_graph(execution_location="local")
    _add_reader(graph, reference, **fields)

    run = graph.run_graph()

    (step,) = [step for step in run.node_step_result if step.node_id == 1]
    assert not step.success
    assert step.error == f"{polaris} {sentence}"


def test_a_name_only_reader_on_a_schema_kept_elsewhere_reads_the_table(polaris, schema, orders):
    """A reader that names the table by its schema's row and the table's name, as the drawer's picker does."""
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import CatalogNamespace

    with get_db_context() as db:
        catalog = db.query(CatalogNamespace).filter_by(name=polaris, parent_id=None).one()
        schema_id = db.query(CatalogNamespace).filter_by(name=schema, parent_id=catalog.id).one().id
    graph = create_graph(execution_location="local")
    add_node_promise_on_type(graph, "catalog_reader", 1)
    settings = input_schema.NodeCatalogReader(
        flow_id=graph.flow_id, node_id=1, user_id=1, catalog_table_name=orders, catalog_namespace_id=schema_id,
    )
    graph.add_catalog_reader(settings)

    handle_run_info(graph.run_graph())

    assert graph.get_node(1).get_resulting_data().collect().height == 1000
    assert graph.get_node(1).setting_input.catalog_table_id is None


def test_an_outside_readers_saved_settings_hold_what_it_was_given(polaris, schema, orders):
    """The name wins: core writes no table id, namespace id or other field into the node."""
    graph = create_graph(execution_location="local")
    given = input_schema.NodeCatalogReader(
        flow_id=graph.flow_id, node_id=1, user_id=1, catalog_full_table_name=f"{polaris}.{schema}.{orders}",
    )
    expected = given.model_dump()
    add_node_promise_on_type(graph, "catalog_reader", 1)
    graph.add_catalog_reader(given)

    assert graph.get_node(1).setting_input.model_dump() == expected


@pytest.fixture
def delta_table():
    """A table of Flowfile's own, written by a flow into a catalog and schema of this test; ``(schema id, id)``."""
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import CatalogNamespace, CatalogTable
    from tests.flowfile.conftest import add_test_catalog_writer, add_test_manual_input, run_test_graph

    with get_db_context() as db:
        catalog = CatalogNamespace(name=f"own_{uuid.uuid4().hex[:6]}", level=0, owner_id=1)
        db.add(catalog)
        db.commit()
        schema = CatalogNamespace(name="sch", level=1, parent_id=catalog.id, owner_id=1)
        db.add(schema)
        db.commit()
        ids = catalog.id, schema.id
    graph = create_graph(flow_id=7, execution_location="local")
    add_test_manual_input(graph, [{"id": 1}, {"id": 2}], node_id=1)
    add_test_catalog_writer(graph, 2, 1, "kept_here", ids[1])
    run_test_graph(graph)
    with get_db_context() as db:
        table_id = db.query(CatalogTable).filter_by(namespace_id=ids[1], name="kept_here").one().id
    yield ids[1], table_id
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        service.delete_table(table_id, delete_file=True)
        service.delete_namespace(ids[1])
        service.delete_namespace(ids[0])


def test_a_delta_readers_saved_settings_are_unchanged(delta_table):
    """A reader of Flowfile's own table back-fills the table id as before and nothing else."""
    schema_id, table_id = delta_table
    graph = create_graph(execution_location="local")
    given = input_schema.NodeCatalogReader(
        flow_id=graph.flow_id, node_id=1, user_id=1, catalog_table_name="kept_here", catalog_namespace_id=schema_id,
    )
    expected = {**given.model_dump(), "catalog_table_id": table_id}
    add_node_promise_on_type(graph, "catalog_reader", 1)
    graph.add_catalog_reader(given)

    assert graph.get_node(1).setting_input.model_dump() == expected
    handle_run_info(graph.run_graph())
    assert graph.get_node(1).get_resulting_data().collect().height == 2


def test_a_notebook_kernels_lookup_answers_the_columns_and_no_secret(principal, polaris, schema, orders):
    """A kernel holds no database and no secret: core's answer to its ``catalog_table`` lookup carries the columns."""
    import json

    from flowfile_core.notebook import lookup

    reference = f"{polaris}.{schema}.{orders}"
    answer = lookup.answer("catalog_table", {"node_id": 1, "catalog_full_table_name": reference}, 1)

    assert answer["kept_elsewhere"] == reference and answer["authorized"] is True
    assert [entry["name"] for entry in json.loads(answer["schema_json"])] == ["id", "grp", "val", "name"]
    text = json.dumps(answer)
    carries_ciphertext = "$ffsec$" in text
    assert not carries_ciphertext
    assert [index for index, value in enumerate(_Values([principal.client_secret])) if value in text] == []


@pytest.fixture
def producer_flow(tmp_path):
    """A registered flow of this test's own, open in the handler, and the virtual tables it made: ``(graph, path)``."""
    from flowfile_core import flow_file_handler
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import CatalogTable, FlowRegistration

    name = f"virtual_over_outside_{uuid.uuid4().hex[:6]}"
    path = tmp_path / f"{name}.yaml"
    with get_db_context() as db:
        registration = FlowRegistration(name=name, flow_path=str(path), owner_id=1)
        db.add(registration)
        db.commit()
        registration_id = registration.id
    internal_id = flow_file_handler.add_flow(name=name, flow_path=str(path), user_id=1)
    graph = flow_file_handler.get_flow(internal_id)
    graph.flow_settings.source_registration_id = registration_id
    yield graph, path
    flow_file_handler.delete_flow(internal_id)
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        for row in db.query(CatalogTable).filter_by(source_registration_id=registration_id).all():
            service.delete_table(row.id, delete_file=False)
        SQLAlchemyCatalogRepository(db).delete_flow(registration_id)


def test_a_virtual_table_over_a_table_kept_elsewhere_is_read_again_after_another_engines_commit(
    signed, polaris, schema, orders, producer_flow
):
    """The table kept elsewhere has no version Flowfile records, so the virtual table records none of its sources."""
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import CatalogTable
    from shared.iceberg_utils import write_table

    graph, path = producer_flow
    graph.flow_settings.execution_location = "local"
    virtual = f"over_outside_{uuid.uuid4().hex[:6]}"
    _add_reader(graph, f"{polaris}.{schema}.{orders}")
    add_node_promise_on_type(graph, "catalog_writer", 2)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_catalog_writer(input_schema.NodeCatalogWriter(
        flow_id=graph.flow_id, node_id=2, user_id=1, depending_on_id=1,
        catalog_write_settings=input_schema.CatalogWriteSettings(table_name=virtual, write_mode="virtual"),
    ))
    graph.save_flow(str(path))
    handle_run_info(graph.run_graph())
    with get_db_context() as db:
        row = db.query(CatalogTable).filter_by(name=virtual).one()
        table_id, versions = row.id, row.source_table_versions
    consumer = create_graph(execution_location="local")
    add_node_promise_on_type(consumer, "catalog_reader", 1)
    consumer.add_catalog_reader(input_schema.NodeCatalogReader(
        flow_id=consumer.flow_id, node_id=1, user_id=1, catalog_table_id=table_id, catalog_table_name=virtual,
    ))

    handle_run_info(consumer.run_graph())
    before = consumer.get_node(1).get_resulting_data().collect().height
    write_table(signed.load_table(f"{schema}.{orders}"), _orders(1000, 1000), "append")
    handle_run_info(consumer.run_graph())
    after = consumer.get_node(1).get_resulting_data().collect().height

    assert versions is None
    assert (before, after) == (1000, 2000)
