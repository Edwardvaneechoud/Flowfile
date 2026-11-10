"""A table in the hub's catalog read outside a flow run, through each route that reads one: the Catalog page's
preview, the SQL editor, a visual on a query and a notebook cell's read.

Every test goes through the HTTP route against the fake hub served over loopback, with the real ``vend.request`` and
the hub's table ``orders`` written to MinIO (the fixtures of ``test_hub_tables_e2e``), and asserts on what the route
answers, so the tests stand whatever module behind the routes answers them. The fake checks what is vended against
the namespace ids it holds, so a vend asked for the wrong schema, or by a name the hub does not list, fails here.

The module skips when MinIO is not available, and every test skips without the session worker, which reads the rows.
"""

import shutil
import uuid

import polars as pl
import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth.jwt import get_internal_token
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import Kernel
from shared.storage_config import storage
from tests import test_catalog_table_target as local
from tests.flowfile.execution import test_hub_tables_e2e as e2e
from tests.test_catalog_cloud_virtual import requires_minio

pytestmark = [requires_minio, pytest.mark.usefixtures("running_worker")]
serve_fake = e2e.serve_fake
store = e2e.store
hub = e2e.hub


@pytest.fixture
def api() -> TestClient:
    """A client signed in as user 1, the person ``hub`` connects."""
    client = TestClient(main.app)
    token = client.post("/auth/token").json()["access_token"]
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


def _vended_reads(fake) -> list[list[int]]:
    return [call["read_namespace_ids"] for call in fake.vend_calls]


def test_the_catalog_page_previews_a_hub_table(api, hub):
    resp = api.get(f"/catalog/tables/{-hub.orders['id']}/preview", params={"limit": 3})

    assert resp.status_code == 200, resp.text
    preview = resp.json()
    assert preview["columns"] == ["id", "country", "amount"]
    assert preview["rows"] == [[1, "NL", 10], [2, "US", 20], [3, "DE", 30]]
    assert preview["total_rows"] == 5
    assert _vended_reads(hub.fake) == [[hub.sales["id"]]]


def test_the_catalog_page_lists_the_versions_of_a_hub_table_and_previews_one(api, hub):
    pl.DataFrame({"id": [6], "country": ["FR"], "amount": [60]}).write_delta(
        hub.orders["file_path"], mode="append", storage_options=e2e.MINIO_OPTIONS
    )

    history = api.get(f"/catalog/tables/{-hub.orders['id']}/history")
    first = api.get(f"/catalog/tables/{-hub.orders['id']}/preview", params={"version": 0})

    assert history.status_code == 200, history.text
    assert history.json()["current_version"] == 1
    assert [entry["version"] for entry in history.json()["history"]] == [1, 0]
    assert first.status_code == 200, first.text
    assert first.json()["total_rows"] == 5
    assert _vended_reads(hub.fake) == [[hub.sales["id"]], [hub.sales["id"]]]


def test_a_hub_table_this_person_is_not_connected_for_answers_the_sentence(api, hub):
    e2e.session.clear(1)

    resp = api.get(f"/catalog/tables/{-hub.orders['id']}/preview")

    assert resp.status_code == 422, resp.text
    assert "not connected" in resp.json()["detail"]
    assert hub.fake.vend_calls == []


@pytest.fixture(params=["hub", "local"])
def orders(request, hub) -> tuple[int, str, list[list[int]]]:
    """``orders`` on object storage, in the hub's catalog or in a local catalog on MinIO: the id of its row in the
    tree, the name a query reads it by, and the reads the hub vends for one read of it."""
    if request.param == "hub":
        yield -hub.orders["id"], hub.orders["qualified_name"], [[hub.sales["id"]]]
        return
    local._ensure_connection()
    prefix = f"local_{uuid.uuid4().hex[:8]}"
    path = f"s3://{e2e._BUCKET}/{prefix}/orders"
    pl.DataFrame(e2e.ORDERS).write_delta(path, storage_options=e2e.MINIO_OPTIONS)
    table_id = local._table(local._schema(f"s3://{e2e._BUCKET}/{prefix}"), "orders", path)
    yield table_id, "orders", []
    minio = e2e.get_minio_client()
    for obj in minio.list_objects_v2(Bucket=e2e._BUCKET, Prefix=f"{prefix}/").get("Contents", []):
        minio.delete_object(Bucket=e2e._BUCKET, Key=obj["Key"])


def test_the_sql_editor_queries_a_table_on_object_storage(api, hub, orders):
    _, name, vended = orders
    query = f"SELECT country, amount FROM {name} WHERE amount > 15 ORDER BY amount"

    resp = api.post("/catalog/sql/execute", json={"query": query, "max_rows": 100})

    assert resp.status_code == 200, resp.text
    result = resp.json()
    assert result["error"] is None
    assert result["rows"] == [["US", 20], ["DE", 30], ["NL", 40], ["BR", 50]]
    assert _vended_reads(hub.fake) == vended


def test_a_visual_charts_a_query_over_a_table_on_object_storage(api, hub, orders, running_worker):
    _, name, vended = orders
    query = f"SELECT country, amount FROM {name}"

    resp = api.post("/catalog/visualizations/fields", json={"source": {"source_type": "sql", "sql_query": query}})

    assert resp.status_code == 200, resp.text
    answer = resp.json()
    assert answer["error"] is None
    assert {field["fid"] for field in answer["fields"]} == {"country", "amount"}
    assert _vended_reads(hub.fake) == vended


def test_a_visual_charts_a_table_on_object_storage(api, hub, orders, running_worker):
    table_id, _, vended = orders

    resp = api.post("/catalog/visualizations/fields", json={"source": {"source_type": "table", "table_id": table_id}})

    assert resp.status_code == 200, resp.text
    answer = resp.json()
    assert answer["error"] is None
    assert {field["fid"] for field in answer["fields"]} == {"id", "country", "amount"}
    assert _vended_reads(hub.fake) == vended


def test_the_sql_editor_refuses_a_query_over_object_storage_and_this_machines_disk(api, hub):
    path = storage.catalog_tables_directory / f"on_disk_{uuid.uuid4().hex[:8]}"
    pl.DataFrame({"id": [1]}).write_delta(path)
    local._table(local._schema(), "on_disk", str(path))
    query = f"SELECT * FROM {hub.orders['qualified_name']} JOIN on_disk USING (id)"
    try:
        resp = api.post("/catalog/sql/execute", json={"query": query, "max_rows": 100})
    finally:
        shutil.rmtree(path)

    assert "one or the other" in resp.json()["error"]
    assert hub.fake.vend_calls == []


@pytest.fixture
def kernel_of_user_1():
    """A kernel row of user 1, as a notebook cell's requests name it."""
    kernel_id = f"hub-reads-{uuid.uuid4().hex[:8]}"
    with get_db_context() as db:
        db.add(Kernel(id=kernel_id, name="notebook", user_id=1))
        db.commit()
    yield kernel_id
    with get_db_context() as db:
        db.query(Kernel).filter(Kernel.id == kernel_id).delete()
        db.commit()


def test_a_notebook_cell_reads_a_hub_table(hub, kernel_of_user_1, running_worker):
    kernel = TestClient(main.app)

    resp = kernel.post(
        f"/catalog/tables/{-hub.orders['id']}/materialize",
        headers={"X-Internal-Token": get_internal_token(), "X-Kernel-Id": kernel_of_user_1},
    )

    assert resp.status_code == 200, resp.text
    result = resp.json()
    assert result["row_count"] == 5
    assert pl.read_ipc(result["path"]).sort("id").to_dict(as_series=False) == e2e.ORDERS
    assert _vended_reads(hub.fake) == [[hub.sales["id"]]]
