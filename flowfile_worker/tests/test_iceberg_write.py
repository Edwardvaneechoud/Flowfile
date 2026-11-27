"""``POST /v1/ops/iceberg_write``: the worker asks the table's catalog for its keys and writes the plan's rows.

The catalog tests skip unless it runs: ``python -m test_utils.iceberg_rest start``. This process runs with no AWS
identity (``clean_aws``); the write's child inherits that environment.
"""

import contextlib
import uuid

import polars as pl
import pytest
from fastapi.testclient import TestClient

from flowfile_worker import main
from flowfile_worker.protocol_routes import OpOutputs
from test_utils.iceberg_rest import fixtures
from test_utils.iceberg_rest.pytest_fixtures import clean_aws
from tests.conftest import V1_HEADERS

client = TestClient(main.app, headers=V1_HEADERS)


@pytest.fixture(scope="module")
def namespace():
    if not fixtures.catalog_ready():
        pytest.skip(fixtures.SKIP_REASON)
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog("worker_tests", **fixtures.catalog_properties())
    name = f"worker_{uuid.uuid4().hex[:8]}"
    catalog.create_namespace(name)
    yield catalog, name
    for identifier in catalog.list_tables(name):
        with contextlib.suppress(Exception):
            catalog.purge_table(identifier)
    with contextlib.suppress(Exception):
        catalog.drop_namespace(name)


def _body(table: str, plan: pl.LazyFrame, **extra) -> dict:
    import base64

    return {
        "operation": base64.b64encode(plan.serialize()).decode(),
        "catalog_uri": fixtures.CATALOG_URI,
        "table": table,
        "warehouse": fixtures.WAREHOUSE,
        "mode": "append",
        "flowfile_flow_id": 1,
        "flowfile_node_id": 1,
        **extra,
    }


@pytest.mark.iceberg_rest
def test_the_worker_creates_the_table_and_writes_the_plans_rows(namespace):
    catalog, name = namespace
    plan = pl.LazyFrame().select(pl.int_range(0, 300, dtype=pl.Int64).alias("id")).with_columns(grp=pl.col("id") % 3)

    response = client.post("/v1/ops/iceberg_write", params={"job_id": uuid.uuid4().hex},
                           json=_body(f"{name}.written", plan))

    assert response.status_code == 200, fixtures.mask(response.text[:500])
    assert OpOutputs.model_validate_json(response.text).outputs == {}
    rows = catalog.load_table(f"{name}.written").scan().to_arrow()
    assert (rows.num_rows, sum(rows.column("id").to_pylist())) == (300, sum(range(300)))


def test_a_body_with_an_unknown_field_is_refused():
    plan = pl.LazyFrame({"id": [1]})
    response = client.post("/v1/ops/iceberg_write", params={"job_id": uuid.uuid4().hex},
                           json=_body("demo.orders", plan, storage_key="nope"))

    assert response.status_code == 422
