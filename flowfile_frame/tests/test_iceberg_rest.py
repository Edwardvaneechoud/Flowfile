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
from test_utils.s3.aws_profiles import isolate_aws

CATALOG = {"catalog_uri": fixtures.CATALOG_URI, "catalog_warehouse": fixtures.WAREHOUSE}


@pytest.fixture(autouse=True)
def clean_aws(monkeypatch, tmp_path):
    """No ambient AWS key or profile, and a closed endpoint for anything that falls back to the environment."""
    aws = tmp_path / "aws"
    aws.mkdir()
    isolate_aws(monkeypatch, aws)


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
def polaris():
    """Polaris added to Flowfile's catalog with a client id and secret, ``sales.orders`` 1000 rows on it."""
    if not fixtures.signed_catalog_ready():
        pytest.skip(fixtures.SIGNED_SKIP_REASON)
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import CatalogNamespace
    from flowfile_core.schemas.catalog_schema import ExternalCatalogCreate
    from shared.iceberg_utils import write_table

    principal = fixtures.new_principal()
    signed = fixtures.signed_catalog(principal)
    schema = f"frame_{uuid.uuid4().hex[:8]}"
    signed.create_namespace(schema)
    frame = pl.LazyFrame({"id": list(range(1000))}).with_columns(grp=pl.col("id") % 10)
    table = signed.create_table(f"{schema}.orders", schema=frame.collect().to_arrow().schema)
    write_table(table, frame, "append")
    name = f"polaris_{uuid.uuid4().hex[:6]}"
    body = ExternalCatalogCreate(
        name=name, uri=fixtures.SIGNED_CATALOG_URI, warehouse=fixtures.SIGNED_WAREHOUSE,
        sign_in="client_credentials", client_id=principal.client_id, client_secret=principal.client_secret,
        scope=fixtures.SIGNED_SCOPE,
    )
    with get_db_context() as db:
        CatalogService(SQLAlchemyCatalogRepository(db)).create_external_catalog(body, 1)
    yield name, schema, signed
    with get_db_context() as db:
        row = db.query(CatalogNamespace).filter_by(name=name, parent_id=None).first()
        CatalogService(SQLAlchemyCatalogRepository(db)).delete_external_catalog(row.id)
    for identifier in signed.list_tables(schema):
        with contextlib.suppress(Exception):
            signed.drop_table(identifier)
    with contextlib.suppress(Exception):
        signed.drop_namespace(schema)
    fixtures.drop_principal(principal.name)


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
