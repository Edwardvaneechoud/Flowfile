"""``ff.read_from_cloud_storage`` and ``ff.write_to_cloud_storage`` on an Iceberg table kept by a REST catalog.

The catalog tests skip unless it runs: ``python -m test_utils.iceberg_rest start``. This process, where the frame's
reads and writes run, has no AWS identity (``clean_aws``).
"""

import contextlib
import uuid

import polars as pl
import pytest

import flowfile_frame as ff
from test_utils.iceberg_rest import fixtures
from test_utils.iceberg_rest.pytest_fixtures import added_catalog, clean_aws, namespace_on, principal, signed

CATALOG = {"catalog_uri": fixtures.CATALOG_URI, "catalog_warehouse": fixtures.WAREHOUSE}


@pytest.fixture(scope="module")
def namespace():
    if not fixtures.catalog_ready():
        pytest.skip(fixtures.SKIP_REASON)
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog("frame_tests", **fixtures.catalog_properties())
    name = f"frame_{uuid.uuid4().hex[:8]}"
    catalog.create_namespace(name)
    arrow = (
        pl.DataFrame({"id": list(range(1000))})
        .with_columns(grp=pl.col("id") % 10, name=pl.format("name-{}", pl.col("id")))
        .to_arrow(compat_level=pl.CompatLevel.oldest())
    )
    catalog.create_table(f"{name}.orders", schema=arrow.schema).append(arrow)
    yield name
    for identifier in catalog.list_tables(name):
        with contextlib.suppress(Exception):
            catalog.purge_table(identifier)
    with contextlib.suppress(Exception):
        catalog.drop_namespace(name)


def test_the_catalog_keywords_need_the_iceberg_format():
    with pytest.raises(ValueError, match="need file_format='iceberg'"):
        ff.read_from_cloud_storage("s3://bucket/x.parquet", file_format="parquet", catalog_uri=fixtures.CATALOG_URI)
    with pytest.raises(ValueError, match="needs catalog_uri"):
        ff.read_from_cloud_storage("demo.orders", file_format="iceberg")


@pytest.mark.iceberg_rest
def test_a_frame_reads_a_catalogs_table(namespace):
    orders = ff.read_from_cloud_storage(f"{namespace}.orders", file_format="iceberg", **CATALOG)

    assert orders.columns == ["id", "grp", "name"]
    result = orders.filter(ff.col("grp") == 3).collect()
    kept = [i for i in range(1000) if i % 10 == 3]
    assert (result.height, result["id"].sum()) == (len(kept), sum(kept))


@pytest.mark.iceberg_rest
def test_a_frame_writes_a_new_table_and_reads_it_back(namespace):
    orders = ff.read_from_cloud_storage(f"{namespace}.orders", file_format="iceberg", **CATALOG)

    ff.write_to_cloud_storage(orders.filter(ff.col("grp") == 3), f"{namespace}.orders_grp3", file_format="iceberg",
                              **CATALOG)
    ff.write_to_cloud_storage(orders.filter(ff.col("grp") == 4), f"{namespace}.orders_grp3", file_format="iceberg",
                              write_mode="append", **CATALOG)

    back = ff.read_from_cloud_storage(f"{namespace}.orders_grp3", file_format="iceberg", **CATALOG).collect()
    assert sorted(back["grp"].unique().to_list()) == [3, 4]
    assert back.height == 200


def test_a_named_catalog_takes_no_address_and_no_other_format():
    with pytest.raises(ValueError, match="not both"):
        ff.read_from_cloud_storage("sales.orders", file_format="iceberg", catalog="polaris",
                                   catalog_uri=fixtures.CATALOG_URI)
    with pytest.raises(ValueError, match="need file_format='iceberg'"):
        ff.read_from_cloud_storage("s3://bucket/x.parquet", file_format="parquet", catalog="polaris")


@pytest.fixture(scope="module")
def polaris(principal, signed):
    """Polaris added to Flowfile's catalog with a client id and secret, ``sales.orders`` 1000 rows on it."""
    from shared.iceberg_utils import write_table

    with namespace_on(signed, "frame") as schema:
        frame = pl.LazyFrame({"id": list(range(1000))}).with_columns(grp=pl.col("id") % 10)
        table = signed.create_table(f"{schema}.orders", schema=frame.collect().to_arrow().schema)
        write_table(table, frame, "append")
        with added_catalog(principal) as (name, _, _):
            yield name, schema, signed


@pytest.mark.iceberg_rest
def test_two_python_lines_read_and_write_a_table_by_the_catalogs_name(polaris):
    name, schema, signed = polaris

    orders = ff.read_from_cloud_storage(f"{schema}.orders", file_format="iceberg", catalog=name)
    ff.write_to_cloud_storage(orders.filter(ff.col("grp") == 3), f"{schema}.orders_grp3", file_format="iceberg",
                              catalog=name)

    assert orders.columns == ["id", "grp"]
    from shared.iceberg_utils import scan_table, table_access

    back = scan_table(table_access(signed.load_table(f"{schema}.orders_grp3"))).collect()
    assert (back.height, sorted(back["grp"].unique().to_list())) == (100, [3])


@pytest.mark.iceberg_rest
def test_read_catalog_table_reads_a_table_of_a_catalog_kept_elsewhere(polaris):
    name, schema, _ = polaris

    orders = ff.read_catalog_table("orders", namespace_full_name=f"{name}.{schema}")

    assert orders.columns == ["id", "grp"]
    result = orders.filter(ff.col("grp") == 3).collect()
    kept = [i for i in range(1000) if i % 10 == 3]
    assert (result.height, result["id"].sum()) == (len(kept), sum(kept))


@pytest.mark.iceberg_rest
def test_write_catalog_table_writes_a_table_of_a_catalog_kept_elsewhere_and_read_catalog_table_reads_it_back(polaris):
    name, schema, signed = polaris

    orders = ff.read_catalog_table("orders", namespace_full_name=f"{name}.{schema}")
    written = ff.write_catalog_table(orders.filter(ff.col("grp") == 3), "orders_grp3_frame",
                                     namespace_full_name=f"{name}.{schema}", write_mode="overwrite")
    run = written.flow_graph.run_graph()
    back = ff.read_catalog_table("orders_grp3_frame", namespace_full_name=f"{name}.{schema}").collect()

    assert run.success, [step.error for step in run.node_step_result if not step.success]
    assert (back.height, sorted(back["grp"].unique().to_list())) == (100, [3])
    assert signed.load_table(f"{schema}.orders_grp3_frame").scan().to_arrow().num_rows == 100


def _qualified(tables) -> list[str]:
    return [table.qualified_name for table in tables]


@pytest.mark.iceberg_rest
def test_list_tables_lists_a_schema_kept_elsewhere_as_its_catalog_has_it_now(polaris):
    import pyarrow as pa

    name, schema, signed = polaris
    first = _qualified(ff.get_catalog(name).get_schema(schema).list_tables())
    signed.create_table(f"{schema}.later_frame", schema=pa.schema([("id", pa.int64())]))

    second = _qualified(ff.get_catalog(name).get_schema(schema).list_tables())
    flat = _qualified(ff.get_catalog(name).list_tables())

    assert f"{name}.{schema}.orders" in first and f"{name}.{schema}.later_frame" not in first
    assert f"{name}.{schema}.later_frame" in second
    assert set(second) <= set(flat)
    assert {table.table_type for table in ff.get_catalog(name).get_schema(schema).list_tables()} == {"virtual"}


@pytest.mark.iceberg_rest
def test_a_kernel_sessions_list_tables_is_core_s_answer_for_the_session_user(polaris):
    from flowfile_core.notebook import lookup
    from flowfile_frame import _metadata

    name, schema, _ = polaris
    asked: list[str] = []

    def transport(payload):
        asked.append(payload["kind"])
        return {"result": lookup.answer(payload["kind"], payload["args"], 1)}

    with _metadata.installed(1, transport):
        in_session = ff.get_catalog(name).get_schema(schema).list_tables()

    assert "tables" in asked
    assert [(t.id, t.qualified_name) for t in in_session] == [
        (t.id, t.qualified_name) for t in ff.get_catalog(name).get_schema(schema).list_tables()
    ]
