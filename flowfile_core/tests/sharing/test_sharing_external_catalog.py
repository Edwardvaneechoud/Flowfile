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


def _catalog_reader(user_id: int, reference: str):
    """A flow of one Catalog Reader on ``reference`` for ``user_id``: its columns before a run and its run's step."""
    from flowfile_core.schemas import input_schema
    from tests.flowfile.test_flowfile import add_node_promise_on_type, create_graph

    graph = create_graph(execution_location="local")
    add_node_promise_on_type(graph, "catalog_reader", 1)
    graph.add_catalog_reader(input_schema.NodeCatalogReader(
        flow_id=graph.flow_id, node_id=1, user_id=user_id, catalog_full_table_name=reference,
    ))
    columns = [column.column_name for column in graph.get_node(1).schema]
    run = graph.run_graph()
    (step,) = [step for step in run.node_step_result if step.node_id == 1]
    return columns, (step.success, step.error or ""), graph


@pytest.mark.iceberg_rest
def test_a_catalog_reader_reads_a_shared_catalogs_table_for_someone_with_use(
    users, client_for, polaris, seeded, team, grant_factory
):
    catalog_id, _ = polaris
    schema, _ = seeded
    grant_factory("catalog_namespace", catalog_id, team, permission="use", granted_by=users["admin"].id)
    name = _catalog_name(client_for, catalog_id)

    columns, step, graph = _catalog_reader(users["bob"].id, f"{name}.{schema}.orders")

    assert columns == ["id", "grp", "val", "name"]
    assert step == (True, "")
    assert graph.get_node(1).get_resulting_data().collect().height == 10


@pytest.mark.iceberg_rest
def test_a_catalog_reader_is_refused_and_shown_no_columns_for_someone_the_schema_is_not_shared_with(
    users, client_for, polaris, seeded
):
    catalog_id, _ = polaris
    schema, _ = seeded
    name = _catalog_name(client_for, catalog_id)

    columns, step, _ = _catalog_reader(users["carol"].id, f"{name}.{schema}.orders")

    assert columns == []
    assert step == (False, f"You have no access to {name}.{schema}; ask its owner to share it.")


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
    assert with_use[2] == (False, f"You may read {name}.{schema} but not write it; ask its owner to let you write it.")
    assert with_manage[2] == (True, "")


def _catalog_writer(user_id: int, namespace: str, table: str):
    """A flow of two rows into a Catalog Writer (Append) on ``namespace`` for ``user_id``; the writer's step."""
    from flowfile_core.flowfile.flow_graph import add_connection
    from flowfile_core.schemas import input_schema
    from tests.flowfile.conftest import add_test_manual_input
    from tests.flowfile.test_flowfile import add_node_promise_on_type, create_graph

    graph = create_graph(execution_location="local")
    add_test_manual_input(graph, [{"id": 1}, {"id": 2}], node_id=1)
    add_node_promise_on_type(graph, "catalog_writer", 2)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_catalog_writer(input_schema.NodeCatalogWriter(
        flow_id=graph.flow_id, node_id=2, user_id=user_id, depending_on_id=1,
        catalog_write_settings=input_schema.CatalogWriteSettings(
            table_name=table, namespace_full_name=namespace, write_mode="append"),
    ))
    run = graph.run_graph()
    (step,) = [step for step in run.node_step_result if step.node_id == 2]
    return step.success, step.error or ""


@pytest.mark.iceberg_rest
def test_a_catalog_writer_is_refused_for_someone_who_may_only_read_the_schema_and_writes_with_manage(
    users, client_for, polaris, seeded, team, grant_factory, signed
):
    catalog_id, _ = polaris
    schema, schema_id = seeded
    name = _catalog_name(client_for, catalog_id)
    grant_factory("catalog_namespace", catalog_id, team, permission="use", granted_by=users["admin"].id)

    with_use = _catalog_writer(users["bob"].id, f"{name}.{schema}", "bob_written")
    grant_factory("catalog_namespace", schema_id, team, permission="manage", granted_by=users["admin"].id)
    with_manage = _catalog_writer(users["bob"].id, f"{name}.{schema}", "bob_written")

    assert with_use == (False, f"You may read {name}.{schema} but not write it; ask its owner to let you write it.")
    assert with_manage == (True, "")
    assert signed.load_table(f"{schema}.bob_written").scan().to_arrow().num_rows == 2


@pytest.mark.iceberg_rest
def test_the_writer_settings_refuse_someone_who_may_only_read_the_schema(
    users, client_for, polaris, seeded, team, grant_factory
):
    from flowfile_core import flow_file_handler

    catalog_id, _ = polaris
    schema, schema_id = seeded
    name = _catalog_name(client_for, catalog_id)
    grant_factory("catalog_namespace", catalog_id, team, permission="use", granted_by=users["admin"].id)
    bob = client_for("bob")
    flow_id = bob.post("/editor/create_flow/", params={"persist": False, "register_in_catalog": False}).json()
    try:
        bob.post("/editor/add_node",
                 params={"flow_id": flow_id, "node_id": 1, "node_type": "catalog_writer", "pos_x": 0, "pos_y": 0})
        payload = {"flow_id": flow_id, "node_id": 1,
                   "catalog_write_settings": {"table_name": "w", "namespace_id": schema_id, "write_mode": "append"}}
        refused = bob.post("/update_settings/", json=payload, params={"node_type": "catalog_writer"})
        grant_factory("catalog_namespace", schema_id, team, permission="manage", granted_by=users["admin"].id)
        payload["catalog_write_settings"]["table_name"] = "w2"
        saved = bob.post("/update_settings/", json=payload, params={"node_type": "catalog_writer"})
    finally:
        flow_file_handler.delete_flow(flow_id)

    assert (refused.status_code, saved.status_code) == (403, 200), saved.text
    the_runs_sentence = f"You may read {name}.{schema} but not write it; ask its owner to let you write it."
    assert refused.json()["detail"] == the_runs_sentence


@pytest.mark.iceberg_rest
def test_the_writer_settings_check_access_before_the_mode_so_a_stranger_learns_nothing_of_the_catalog(
    users, client_for, polaris, seeded
):
    """Someone with no access to the schema gets the ordinary 403 for a refused mode too, never the catalog's name."""
    from flowfile_core import flow_file_handler

    catalog_id, _ = polaris
    _, schema_id = seeded
    name = _catalog_name(client_for, catalog_id)
    carol = client_for("carol")
    flow_id = carol.post("/editor/create_flow/", params={"persist": False, "register_in_catalog": False}).json()
    try:
        carol.post("/editor/add_node",
                   params={"flow_id": flow_id, "node_id": 1, "node_type": "catalog_writer", "pos_x": 0, "pos_y": 0})
        payload = {"flow_id": flow_id, "node_id": 1, "catalog_write_settings": {
            "table_name": "w", "namespace_id": schema_id, "write_mode": "upsert", "merge_keys": ["id"]}}
        refused = carol.post("/update_settings/", json=payload, params={"node_type": "catalog_writer"})
    finally:
        flow_file_handler.delete_flow(flow_id)

    assert refused.status_code == 403
    assert refused.json()["detail"] == "You do not have write access to the selected catalog namespace"
    assert name not in refused.text


@pytest.mark.iceberg_rest
def test_a_kernels_lookup_for_someone_without_use_is_not_authorized_and_asks_the_other_catalog_nothing(
    users, client_for, polaris, seeded, monkeypatch
):
    from flowfile_core.notebook import lookup
    from shared import iceberg_utils

    catalog_id, _ = polaris
    schema, _ = seeded
    name = _catalog_name(client_for, catalog_id)
    asked: list[object] = []
    build = iceberg_utils._catalog

    def recording(*args, **kwargs):
        asked.append(kwargs.get("keys"))
        return build(*args, **kwargs)

    monkeypatch.setattr(iceberg_utils, "_catalog", recording)

    answer = lookup.answer(
        "catalog_table", {"node_id": 1, "catalog_full_table_name": f"{name}.{schema}.orders"}, users["carol"].id
    )

    assert answer["kept_elsewhere"] == f"{name}.{schema}.orders"
    assert (answer["authorized"], answer["schema_json"]) == (False, None)
    assert asked == []


@pytest.fixture
def kernel_of():
    """A TestClient signed as a running kernel of a user, with that kernel's own token; teardown removes the rows."""
    from fastapi.testclient import TestClient

    from flowfile_core import main
    from flowfile_core.auth.jwt import create_kernel_token
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import Kernel

    made: list[str] = []

    def _client(user_id: int) -> TestClient:
        kernel_id, token_id = f"ext-share-{uuid.uuid4().hex[:8]}", uuid.uuid4().hex
        with get_db_context() as db:
            db.add(Kernel(id=kernel_id, name="Outside catalog", user_id=user_id, token_id=token_id))
            db.commit()
        made.append(kernel_id)
        client = TestClient(main.app)
        client.headers = {
            "X-Internal-Token": create_kernel_token(user_id, kernel_id, token_id),
            "X-Kernel-Id": kernel_id,
        }
        return client

    yield _client
    with get_db_context() as db:
        db.query(Kernel).filter(Kernel.id.in_(made)).delete(synchronize_session=False)
        db.commit()


@pytest.mark.iceberg_rest
def test_a_kernel_whose_owner_has_no_use_gets_the_access_sentence_and_the_other_catalog_is_asked_nothing(
    users, client_for, polaris, seeded, team, grant_factory, kernel_of, monkeypatch
):
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import CatalogTable
    from shared import iceberg_utils

    catalog_id, _ = polaris
    schema, schema_id = seeded
    name = _catalog_name(client_for, catalog_id)
    asked: list[object] = []
    build = iceberg_utils._catalog

    def recording(*args, **kwargs):
        asked.append(kwargs.get("keys"))
        return build(*args, **kwargs)

    monkeypatch.setattr(iceberg_utils, "_catalog", recording)
    carol = kernel_of(users["carol"].id)

    by_name = carol.get("/catalog/tables/resolve", params={"q": f"{name}.{schema}.orders"})
    in_schema = carol.get("/catalog/tables/resolve", params={"q": "orders", "namespace_id": schema_id})

    said = f"You have no access to {name}.{schema}; ask its owner to share it."
    assert (by_name.status_code, by_name.json()["detail"]) == (422, said)
    assert (in_schema.status_code, in_schema.json()["detail"]) == (422, said)
    assert asked == []
    with get_db_context() as db:
        assert db.query(CatalogTable).filter_by(namespace_id=schema_id, name="orders").count() == 0

    grant_factory("catalog_namespace", catalog_id, team, permission="use", granted_by=users["admin"].id)
    bob = kernel_of(users["bob"].id).get("/catalog/tables/resolve", params={"q": f"{name}.{schema}.orders"})

    assert (bob.status_code, bob.json()["detail"]) == (422, SHARED_SERVER.format(f"{name}.{schema}.orders"))
    assert asked == []
    with get_db_context() as db:
        assert db.query(CatalogTable).filter_by(namespace_id=schema_id, name="orders").count() == 0


@pytest.mark.iceberg_rest
def test_a_kernel_lists_an_outside_schemas_tables_only_with_use_and_without_it_the_other_catalog_is_asked_nothing(
    users, client_for, polaris, seeded, team, grant_factory, kernel_of, monkeypatch
):
    """Listing names is no read: someone with Use lists them on a shared server too; a stranger gets the access
    sentence before the other catalog is asked anything, and their own sign-in's listing is what it was."""
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import CatalogTable
    from flowfile_core.notebook import lookup
    from shared import iceberg_utils

    catalog_id, _ = polaris
    schema, schema_id = seeded
    name = _catalog_name(client_for, catalog_id)
    grant_factory("catalog_namespace", catalog_id, team, permission="use", granted_by=users["admin"].id)
    asked: list[object] = []
    build = iceberg_utils._catalog

    def recording(*args, **kwargs):
        asked.append(kwargs.get("keys"))
        return build(*args, **kwargs)

    monkeypatch.setattr(iceberg_utils, "_catalog", recording)
    params = {"namespace_id": schema_id}

    carol = kernel_of(users["carol"].id).get("/catalog/tables", params=params)
    with pytest.raises(ValueError) as refused:
        lookup.answer("tables", params, users["carol"].id)
    carol_person = client_for("carol").get("/catalog/tables", params=params)

    said = f"You have no access to {name}.{schema}; ask its owner to share it."
    assert (carol.status_code, carol.json()["detail"]) == (422, said)
    assert str(refused.value) == said
    assert (carol_person.status_code, carol_person.json()) == (200, [])
    assert asked == []
    with get_db_context() as db:
        assert db.query(CatalogTable).filter_by(namespace_id=schema_id, name="orders").count() == 0

    bob = kernel_of(users["bob"].id).get("/catalog/tables", params=params)

    assert bob.status_code == 200, bob.text
    assert [row["qualified_name"] for row in bob.json()] == [f"{name}.{schema}.orders"]
    assert asked == [False]


@pytest.mark.iceberg_rest
def test_a_second_catalog_renamed_onto_a_shared_ones_name_lists_nothing_for_someone_without_use_on_it(
    users, client_for, polaris, seeded, principal, team, grant_factory, kernel_of, monkeypatch
):
    """Two catalogs kept elsewhere can carry one name (a catalog renamed onto a taken name): Use is checked on the
    catalog whose schema is listed, not on the first one of that name the person may reach."""
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import CatalogNamespace, CatalogTable
    from flowfile_core.notebook import lookup
    from shared import iceberg_utils

    catalog_id, _ = polaris
    schema, schema_id = seeded
    name = _catalog_name(client_for, catalog_id)
    grant_factory("catalog_namespace", schema_id, team, permission="use", granted_by=users["admin"].id)
    admin = client_for("admin")
    added = admin.post("/catalog/external", json=_body(principal, f"other_{uuid.uuid4().hex[:6]}"))
    assert added.status_code == 201, added.json().get("detail")
    other_id = added.json()["id"]
    try:
        renamed = admin.put(f"/catalog/namespaces/{other_id}", json={"name": name})
        assert renamed.status_code == 200, renamed.text
        with get_db_context() as db:
            other_schema = db.query(CatalogNamespace).filter_by(parent_id=other_id, name=schema).one().id
        asked: list[object] = []
        build = iceberg_utils._catalog

        def recording(*args, **kwargs):
            asked.append(kwargs.get("keys"))
            return build(*args, **kwargs)

        monkeypatch.setattr(iceberg_utils, "_catalog", recording)
        bob = kernel_of(users["bob"].id)

        listed = bob.get("/catalog/tables", params={"namespace_id": other_schema})
        with pytest.raises(ValueError) as refused:
            lookup.answer("tables", {"namespace_id": other_schema}, users["bob"].id)

        said = f"You have no access to {name}.{schema}; ask its owner to share it."
        assert (listed.status_code, listed.json().get("detail")) == (422, said)
        assert str(refused.value) == said
        assert asked == []
        with get_db_context() as db:
            assert db.query(CatalogTable).filter_by(namespace_id=other_schema).count() == 0

        shared = bob.get("/catalog/tables", params={"namespace_id": schema_id})
        assert shared.status_code == 200, shared.text
        assert [row["qualified_name"] for row in shared.json()] == [f"{name}.{schema}.orders"]
    finally:
        admin.delete(f"/catalog/external/{other_id}")


@pytest.mark.iceberg_rest
def test_a_listing_names_the_same_reader_and_writer_flows_as_the_tables_panel_for_someone_with_use(
    users, client_for, polaris, seeded, team, grant_factory, kernel_of, tmp_path
):
    """Both name an administrator's flow that reads and writes the table to someone with Use on the schema who has no
    access to that flow: the listing shows no more and no less than the panel."""
    from flowfile_core.catalog import SQLAlchemyCatalogRepository
    from flowfile_core.catalog.services.external_catalogs import OutsideTable, ensure_table_row
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import CatalogTable, FlowRegistration

    catalog_id, _ = polaris
    schema, schema_id = seeded
    name = _catalog_name(client_for, catalog_id)
    grant_factory("catalog_namespace", catalog_id, team, permission="use", granted_by=users["admin"].id)
    flow_name = f"admins_flow_{uuid.uuid4().hex[:6]}"
    with get_db_context() as db:
        flow = FlowRegistration(name=flow_name, flow_path=str(tmp_path / f"{flow_name}.yaml"), owner_id=users["admin"].id)
        db.add(flow)
        db.commit()
        flow_id = flow.id
        row = ensure_table_row(db, OutsideTable(name, schema, "orders", schema_id), written_by=flow_id)
        SQLAlchemyCatalogRepository(db).replace_read_links(flow_id, [row.id])
        db.commit()
    try:
        panel = client_for("bob").get(
            f"/catalog/external/{catalog_id}/schemas/{schema_id}/table", params={"name": "orders"}
        )
        listed = kernel_of(users["bob"].id).get("/catalog/tables", params={"namespace_id": schema_id})

        assert (panel.status_code, listed.status_code) == (200, 200), (panel.text, listed.text)
        (entry,) = [table for table in listed.json() if table["name"] == "orders"]
        named = {"id": flow_id, "name": flow_name}
        assert panel.json()["read_by_flows"] == entry["read_by_flows"] == [named]
        assert panel.json()["written_by"] == named
        assert (entry["source_registration_id"], entry["source_registration_name"]) == (flow_id, flow_name)
    finally:
        with get_db_context() as db:
            for table in db.query(CatalogTable).filter_by(source_registration_id=flow_id).all():
                table.source_registration_id = None
            db.commit()
            SQLAlchemyCatalogRepository(db).delete_flow(flow_id)


SHARED_SERVER = "On a shared server {} is read in a flow, not in this notebook."


def _hidden_row(catalog: str, schema: str, schema_id: int) -> int:
    """The hidden row of ``orders``, made as a flow's node makes it (``ensure_table_row``); its id."""
    from flowfile_core.catalog.services.external_catalogs import OutsideTable, ensure_table_row
    from flowfile_core.database.connection import get_db_context

    with get_db_context() as db:
        return ensure_table_row(db, OutsideTable(catalog, schema, "orders", schema_id)).id


def _snapshots(row_id: int) -> list:
    from shared.storage_config import storage

    return list(storage.shared_virtual_results_directory.glob(f"fvt-{row_id}-*.arrow"))


@pytest.mark.iceberg_rest
def test_on_a_shared_server_no_kernel_reads_a_table_kept_elsewhere_and_no_snapshot_is_written(
    users, client_for, polaris, seeded, team, grant_factory, kernel_of, monkeypatch
):
    """Use is checked first: a stranger gets the access sentence, someone with Use the shared-server sentence, by name,
    in the schema and by the hidden row's id, from a kernel and from their own sign-in; the other catalog is asked
    nothing and no snapshot file is written."""
    from shared import iceberg_utils

    catalog_id, _ = polaris
    schema, schema_id = seeded
    name = _catalog_name(client_for, catalog_id)
    grant_factory("catalog_namespace", catalog_id, team, permission="use", granted_by=users["admin"].id)
    row_id = _hidden_row(name, schema, schema_id)
    asked: list[object] = []
    build = iceberg_utils._catalog

    def recording(*args, **kwargs):
        asked.append(kwargs.get("keys"))
        return build(*args, **kwargs)

    monkeypatch.setattr(iceberg_utils, "_catalog", recording)
    callers = {
        "bob's kernel": kernel_of(users["bob"].id),
        "carol's kernel": kernel_of(users["carol"].id),
        "bob": client_for("bob"),
        "carol": client_for("carol"),
    }

    def said(response) -> tuple[int, str]:
        return response.status_code, response.json().get("detail")

    answers = {
        who: (
            said(client.get("/catalog/tables/resolve", params={"q": f"{name}.{schema}.orders"})),
            said(client.get("/catalog/tables/resolve", params={"q": "orders", "namespace_id": schema_id})),
            said(client.post(f"/catalog/tables/{row_id}/materialize")),
        )
        for who, client in callers.items()
    }

    shared = (422, SHARED_SERVER.format(f"{name}.{schema}.orders"))
    no_access = (422, f"You have no access to {name}.{schema}; ask its owner to share it.")
    not_found = (404, "Catalog table not found")
    not_yours = (403, "Not authorized to access this catalog_table")
    assert answers == {
        "bob's kernel": (shared, shared, shared),
        "carol's kernel": (no_access, no_access, not_yours),
        "bob": (not_found, not_found, shared),
        "carol": (not_found, not_found, not_yours),
    }
    assert asked == []
    assert _snapshots(row_id) == []


@pytest.mark.iceberg_rest
def test_the_snapshot_is_checked_as_the_person_it_is_for(users, client_for, polaris, seeded, monkeypatch):
    """Past the catalog service's own check (an unrestricted service), the snapshot still checks Use as the user it
    is written for: someone without Use gets the access sentence, not the shared-server one, and nothing is asked."""
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
    from flowfile_core.database.connection import get_db_context
    from shared import iceberg_utils

    catalog_id, _ = polaris
    schema, schema_id = seeded
    name = _catalog_name(client_for, catalog_id)
    row_id = _hidden_row(name, schema, schema_id)
    asked: list[object] = []
    monkeypatch.setattr(iceberg_utils, "_catalog", lambda *args, **kwargs: asked.append(kwargs.get("keys")))

    with get_db_context() as db, pytest.raises(ValueError) as refused:
        CatalogService(SQLAlchemyCatalogRepository(db)).materialize_table(row_id, user_id=users["carol"].id)

    assert str(refused.value) == f"You have no access to {name}.{schema}; ask its owner to share it."
    assert asked == []
    assert _snapshots(row_id) == []


def _sign_in_of(catalog_id: int) -> tuple[str | None, int | None]:
    """The catalog's sign-in secret as its row names it: the secret's name (``None`` when no row has the id) and id."""
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import ExternalCatalog, Secret

    with get_db_context() as db:
        row = db.query(ExternalCatalog).filter_by(namespace_id=catalog_id).one()
        secret = db.get(Secret, row.secret_id) if row.secret_id is not None else None
        return (secret.name if secret is not None else None), row.secret_id


def _ciphertext(secret_id: int) -> str | None:
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import Secret

    with get_db_context() as db:
        secret = db.get(Secret, secret_id)
        return secret.encrypted_value if secret is not None else None


def _ref_token(catalog_name: str) -> str | None:
    from flowfile_core.catalog.services.external_catalogs import table_ref_for_run
    from flowfile_core.database.connection import get_db_context

    with get_db_context() as db:
        return table_ref_for_run(db, catalog_name, "any_schema.any_table", None, writes=False).token



def _sign_in_again(catalog_name: str) -> str:
    return f"Flowfile no longer holds the sign-in for {catalog_name}; enter it again with Edit on the catalog."


@pytest.mark.iceberg_rest
def test_another_persons_secret_on_a_dangling_sign_in_id_is_never_sent_overwritten_or_deleted(
    users, client_for, polaris, principal
):
    """The catalog's sign-in deleted before that was refused leaves its id to the next secret anyone stores: Bob's,
    even named as the catalog's sign-in is, belongs to no catalog of the administrator's."""
    from flowfile_core.catalog.services.external_catalogs import sign_in_name
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import Secret

    catalog_id, _ = polaris
    admin, bob = client_for("admin"), client_for("bob")
    catalog = _catalog_name(client_for, catalog_id)
    _, secret_id = _sign_in_of(catalog_id)
    with get_db_context() as db:
        db.delete(db.get(Secret, secret_id))
        db.commit()
    stored = bob.post("/secrets/secrets", json={"name": sign_in_name(catalog), "value": "bobs-value"})
    bobs_id = stored.json()["id"]
    if bobs_id != secret_id:
        pytest.skip("this catalog database gave the next secret a new id, so no id is left dangling to take")
    bobs = _ciphertext(bobs_id)

    with pytest.raises(ValueError) as refused:
        _ref_token(catalog)
    edited = admin.put(f"/catalog/external/{catalog_id}", json={"client_secret": principal.client_secret})
    unchanged = _ciphertext(bobs_id) == bobs
    _, new_id = _sign_in_of(catalog_id)
    removed = admin.delete(f"/catalog/external/{catalog_id}")
    survived = _ciphertext(bobs_id) is not None
    bob_deletes = bob.delete(f"/secrets/secrets/{sign_in_name(catalog)}")

    assert str(refused.value) == _sign_in_again(catalog)
    assert edited.status_code == 200, edited.json().get("detail")
    assert new_id != bobs_id and unchanged is True
    assert removed.status_code == 204 and survived
    assert bob_deletes.status_code == 204


@pytest.mark.iceberg_rest
def test_deleting_the_owners_account_leaves_the_catalog_needing_its_sign_in_and_no_other_secret_takes_its_place(
    users, client_for, principal
):
    from flowfile_core.catalog import SQLAlchemyCatalogRepository
    from flowfile_core.catalog.services.external_catalogs import ExternalCatalogService
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import Secret
    from flowfile_core.schemas.catalog_schema import ExternalCatalogCreate

    name = f"alices_{uuid.uuid4().hex[:6]}"
    with get_db_context() as db:
        catalog_id = ExternalCatalogService(SQLAlchemyCatalogRepository(db)).create(
            ExternalCatalogCreate(**_body(principal, name)), users["alice"].id
        ).id
    try:
        _, secret_id = _sign_in_of(catalog_id)
        removed = client_for("admin").delete(f"/auth/users/{users['alice'].id}")
        after_removal = _sign_in_of(catalog_id)
        with get_db_context() as db:
            alices_secret = db.get(Secret, secret_id)
        stored = client_for("bob").post("/secrets/secrets", json={"name": "bobs_own", "value": "bobs-value"})

        with pytest.raises(ValueError) as refused:
            _ref_token(name)

        assert removed.status_code == 200
        assert alices_secret is None and after_removal == (None, None)
        assert stored.status_code == 200 and _sign_in_of(catalog_id) == (None, None)
        assert str(refused.value) == _sign_in_again(name)
    finally:
        with get_db_context() as db:
            ExternalCatalogService(SQLAlchemyCatalogRepository(db)).delete(catalog_id)
