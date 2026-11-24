"""A catalog kept elsewhere on a multi-user server: only an administrator adds one, it is private until shared, a grant
on the catalog reaches its schemas (also ones a later sync adds), and neither Use nor Manage changes how it is reached.

The cases that add a catalog need Apache Polaris (``python -m test_utils.iceberg_rest start``) and skip without it.
"""

import uuid

import pytest

from test_utils.iceberg_rest import fixtures

SIGNED = fixtures.SIGNED_CATALOG_URI


@pytest.fixture(autouse=True)
def clean_aws(monkeypatch, tmp_path):
    from test_utils.s3.aws_profiles import isolate_aws

    aws_dir = tmp_path / "aws"
    aws_dir.mkdir()
    isolate_aws(monkeypatch, aws_dir)


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


def _body(principal, name: str) -> dict:
    return {
        "name": name,
        "uri": SIGNED,
        "warehouse": fixtures.SIGNED_WAREHOUSE,
        "sign_in": "client_credentials",
        "client_id": principal.client_id,
        "client_secret": principal.client_secret,
        "scope": fixtures.SIGNED_SCOPE,
    }


@pytest.fixture
def team(users, group_factory):
    return group_factory("elsewhere-team", users["admin"].id, {users["bob"].id: "member"})


@pytest.fixture
def made_namespaces(signed):
    made: list[str] = []
    yield made
    for name in made:
        if signed.namespace_exists(name):
            for identifier in signed.list_tables(name):
                signed.drop_table(identifier)
            signed.drop_namespace(name)


@pytest.fixture
def polaris(client_for, principal, signed, made_namespaces):
    """A Polaris catalog an administrator added, with two schemas of its own; ``(id, {schema name: id})``."""
    for _ in range(2):
        name = f"team_{uuid.uuid4().hex[:8]}"
        signed.create_namespace(name)
        made_namespaces.append(name)
    admin = client_for("admin")
    added = admin.post("/catalog/external", json=_body(principal, f"polaris_{uuid.uuid4().hex[:6]}"))
    assert added.status_code == 201, added.json().get("detail")
    catalog_id = added.json()["id"]
    node = next(n for n in admin.get("/catalog/namespaces/tree").json() if n["id"] == catalog_id)
    schemas = {child["name"]: child["id"] for child in node["children"] if child["name"] in made_namespaces}
    yield catalog_id, schemas
    admin.delete(f"/catalog/external/{catalog_id}")


def _tree(client, catalog_id: int) -> dict | None:
    return next((n for n in client.get("/catalog/namespaces/tree").json() if n["id"] == catalog_id), None)


def test_only_an_administrator_adds_or_tests_a_catalog_kept_elsewhere(client_for):
    alice = client_for("alice")
    body = {"uri": "http://127.0.0.1:9/api/catalog", "warehouse": "signed"}

    tested = alice.post("/catalog/external/test", json=body)
    added = alice.post("/catalog/external", json={**body, "name": "not_hers"})

    assert (tested.status_code, added.status_code) == (403, 403)


@pytest.mark.iceberg_rest
def test_it_is_private_until_shared_and_a_grant_on_the_catalog_shows_its_schemas(
    users, client_for, polaris, team, grant_factory, signed, made_namespaces
):
    catalog_id, schemas = polaris
    bob = client_for("bob")
    before = _tree(bob, catalog_id)

    grant_factory("catalog_namespace", catalog_id, team, permission="use", granted_by=users["admin"].id)
    shared = _tree(bob, catalog_id)
    later = f"later_{uuid.uuid4().hex[:8]}"
    signed.create_namespace(later)
    made_namespaces.append(later)
    synced = client_for("admin").post(f"/catalog/external/{catalog_id}/sync", params={"force": True})
    after = _tree(bob, catalog_id)

    assert before is None
    assert set(schemas) <= {child["name"] for child in shared["children"]}
    assert synced.json()["added"] == [later]
    assert later in {child["name"] for child in after["children"]}
    assert after["external"]["uri"] == SIGNED


@pytest.mark.iceberg_rest
def test_a_grant_on_one_schema_shows_that_schema_only(users, client_for, polaris, team, grant_factory):
    catalog_id, schemas = polaris
    one, other = sorted(schemas)
    grant_factory("catalog_namespace", schemas[one], team, permission="use", granted_by=users["admin"].id)

    node = _tree(client_for("bob"), catalog_id)

    names = {child["name"] for child in node["children"]}
    assert one in names and other not in names


@pytest.mark.iceberg_rest
def test_a_manager_cannot_change_the_address_or_the_sign_in(users, client_for, polaris, team, grant_factory, principal):
    catalog_id, _ = polaris
    grant_factory("catalog_namespace", catalog_id, team, permission="manage", granted_by=users["admin"].id)
    bob = client_for("bob")

    moved = bob.put(f"/catalog/external/{catalog_id}", json={"uri": "https://elsewhere.example/api/catalog"})
    resigned = bob.put(f"/catalog/external/{catalog_id}", json={"client_secret": "a-new-one"})
    removed = bob.delete(f"/catalog/external/{catalog_id}")

    assert (moved.status_code, resigned.status_code, removed.status_code) == (403, 403, 403)


@pytest.mark.iceberg_rest
def test_someone_with_use_may_refresh_and_someone_without_may_not(users, client_for, polaris, team, grant_factory):
    catalog_id, _ = polaris
    grant_factory("catalog_namespace", catalog_id, team, permission="use", granted_by=users["admin"].id)

    bob = client_for("bob").post(f"/catalog/external/{catalog_id}/sync", params={"force": True})
    carol = client_for("carol").post(f"/catalog/external/{catalog_id}/sync", params={"force": True})

    assert (bob.status_code, carol.status_code) == (200, 403)


@pytest.mark.iceberg_rest
def test_tables_of_a_schema_are_listed_for_someone_with_use_and_refused_without(
    users, client_for, polaris, team, grant_factory, signed
):
    import pyarrow as pa

    catalog_id, schemas = polaris
    name = sorted(schemas)[0]
    signed.create_table(f"{name}.orders", schema=pa.schema([("id", pa.int64())]))
    grant_factory("catalog_namespace", catalog_id, team, permission="use", granted_by=users["admin"].id)
    path = f"/catalog/external/{catalog_id}/schemas/{schemas[name]}"

    listed = client_for("bob").get(f"{path}/tables")
    described = client_for("bob").get(f"{path}/table", params={"name": "orders"})
    refused = client_for("carol").get(f"{path}/tables")

    assert [t["name"] for t in listed.json()["tables"]] == ["orders"]
    assert [c["name"] for c in described.json()["columns"]] == ["id"]
    assert refused.status_code == 403


@pytest.fixture
def seeded(signed, polaris):
    """``orders`` (10 rows) in the first of the catalog's schemas; ``(schema name, schema id)``."""
    import polars as pl

    from shared.iceberg_utils import write_table
    from shared.tests.iceberg.table_child import rows_frame

    catalog_id, schemas = polaris
    name = sorted(schemas)[0]
    frame = rows_frame(10)
    table = signed.create_table(
        f"{name}.orders", schema=frame.collect().to_arrow(compat_level=pl.CompatLevel.oldest()).schema
    )
    write_table(table, frame, "append")
    return name, schemas[name]


def _catalog_name(client_for, catalog_id: int) -> str:
    return _tree(client_for("admin"), catalog_id)["name"]


def _run(user_id: int, catalog: str, schema: str, write_to: str | None = None, address: bool = False):
    from flowfile_core.flowfile.flow_graph import add_connection
    from flowfile_core.schemas import cloud_storage_schemas as cloud_ss
    from flowfile_core.schemas import input_schema
    from tests.flowfile.test_flowfile import add_node_promise_on_type, create_graph

    named = (
        cloud_ss.IcebergCatalogSettings(uri=SIGNED, warehouse=fixtures.SIGNED_WAREHOUSE)
        if address
        else cloud_ss.IcebergCatalogSettings(catalog=catalog)
    )
    graph = create_graph(execution_location="local")
    add_node_promise_on_type(graph, "cloud_storage_reader", 1)
    graph.add_cloud_storage_reader(input_schema.NodeCloudStorageReader(
        flow_id=graph.flow_id, node_id=1, user_id=user_id,
        cloud_storage_settings=cloud_ss.CloudStorageReadSettings(
            resource_path=f"{schema}.orders", file_format="iceberg", iceberg_catalog=named),
    ))
    if write_to:
        add_node_promise_on_type(graph, "cloud_storage_writer", 2)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
        graph.add_cloud_storage_writer(input_schema.NodeCloudStorageWriter(
            flow_id=graph.flow_id, node_id=2, user_id=user_id, depending_on_id=1,
            cloud_storage_settings=cloud_ss.CloudStorageWriteSettings(
                resource_path=f"{schema}.{write_to}", file_format="iceberg", write_mode="overwrite",
                iceberg_catalog=named),
        ))
    run = graph.run_graph()
    return {step.node_id: (step.success, step.error or "") for step in run.node_step_result}


@pytest.mark.iceberg_rest
def test_a_multi_user_server_runs_a_node_that_names_a_shared_catalog_and_still_refuses_a_typed_address(
    users, client_for, polaris, seeded, team, grant_factory
):
    from flowfile_core.flowfile.flow_graph.cloud import ICEBERG_ON_A_SERVER

    catalog_id, _ = polaris
    schema, _ = seeded
    grant_factory("catalog_namespace", catalog_id, team, permission="use", granted_by=users["admin"].id)
    name = _catalog_name(client_for, catalog_id)

    named = _run(users["bob"].id, name, schema)
    typed = _run(users["bob"].id, name, schema, address=True)

    assert named[1] == (True, "")
    assert not typed[1][0] and ICEBERG_ON_A_SERVER in typed[1][1]


@pytest.mark.iceberg_rest
def test_a_node_is_refused_for_someone_the_schema_is_not_shared_with(users, client_for, polaris, seeded):
    catalog_id, _ = polaris
    schema, _ = seeded
    name = _catalog_name(client_for, catalog_id)

    refused = _run(users["carol"].id, name, schema)

    assert refused[1] == (False, f"You have no access to {name}.{schema}; ask its owner to share it.")


@pytest.mark.iceberg_rest
def test_use_reads_and_does_not_write_and_manage_writes(users, client_for, polaris, seeded, team, grant_factory):
    catalog_id, _ = polaris
    schema, schema_id = seeded
    name = _catalog_name(client_for, catalog_id)
    grant_factory("catalog_namespace", catalog_id, team, permission="use", granted_by=users["admin"].id)

    with_use = _run(users["bob"].id, name, schema, write_to="bob_copy")
    grant_factory("catalog_namespace", schema_id, team, permission="manage", granted_by=users["admin"].id)
    with_manage = _run(users["bob"].id, name, schema, write_to="bob_copy")

    assert with_use[1] == (True, "")
    assert with_use[2] == (False, f"You have no access to {name}.{schema}; ask its owner to share it to write it.")
    assert with_manage[2] == (True, "")
