import json
from datetime import UTC
from importlib import import_module
from types import SimpleNamespace

import httpx
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import MetaData, Table, create_engine, select
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import get_db
from flowfile_core.database.models import Secret, SharePointConnection, User
from flowfile_core.main import app
from flowfile_core.secret_manager.secret_manager import decrypt_secret


@pytest.fixture
def saved_connection(monkeypatch):
    from cryptography.fernet import Fernet

    from flowfile_core.secret_manager import secret_manager

    worker_secrets = import_module("flowfile_worker.secrets")

    key = Fernet.generate_key().decode()
    monkeypatch.setattr(secret_manager, "get_master_key", lambda: key)
    monkeypatch.setattr(worker_secrets, "get_master_key", lambda: key)
    engine = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
    User.__table__.create(engine)
    Secret.__table__.create(engine)
    secret_value = "fixture-client-secret"

    def database():
        with Session(engine) as db:
            yield db

    monkeypatch.setitem(app.dependency_overrides, get_db, database)
    monkeypatch.setitem(app.dependency_overrides, get_current_active_user, lambda: SimpleNamespace(id=1))
    try:
        migration = import_module("flowfile_core.alembic.versions.046_sharepoint_connections")
        with engine.begin() as connection, Operations.context(MigrationContext.configure(connection)):
            migration.upgrade()
            import_module("flowfile_core.alembic.versions.047_sharepoint_connection_ids").upgrade()
            import_module("flowfile_core.alembic.versions.048_sharepoint_project_identity").upgrade()
            import_module("flowfile_core.alembic.versions.049_sharepoint_excel_preview").upgrade()
        with Session(engine) as db:
            db.add(User(id=1, username="owner"))
            db.commit()
        response = TestClient(app).post(
            "/sharepoint_connections",
            json={
                "connection_name": "test SharePoint",
                "tenant_id": "a01b7819-6a0a-47c9-a6ff-e1563529ac33",
                "client_id": "45d666c4-a4d2-4aad-905a-15443f348233",
                "client_secret": secret_value,
                "default_site_url": "https://example.sharepoint.com/sites/Finance",
            },
        )
        assert response.status_code == 201, response.text
        assert response.json()["connection_name"] == "test SharePoint"
        with Session(engine) as db:
            connection = db.get(SharePointConnection, response.json()["id"])
            secret = db.scalar(select(Secret).where(Secret.id == connection.credential_secret_id))
            assert secret.encrypted_value.startswith("$ffsec$1$1$")
            assert decrypt_secret(secret.encrypted_value).get_secret_value() == secret_value
            assert secret.encrypted_value not in response.text
        assert secret_value not in response.text
        yield response.json()["id"]
    finally:
        engine.dispose()


def test_project_sharepoint_binding_does_not_use_another_owners_connection(saved_connection):
    from contextlib import contextmanager

    from flowfile_core.project.importer import _bind_project_sharepoint_nodes

    with contextmanager(app.dependency_overrides[get_db])() as db:
        connection = db.get(SharePointConnection, saved_connection)
        settings = {"connection_id": saved_connection, "sharepoint_connection_uuid": connection.connection_uuid}
        flow = {"nodes": [{"type": "sharepoint_reader", "setting_input": settings}]}
        _bind_project_sharepoint_nodes(db, flow, 2)
        assert settings["connection_id"] is None


@pytest.mark.parametrize("delegated", [False, True])
def test_project_sharepoint_binding_retains_only_usable_local_settings(saved_connection, monkeypatch, delegated):
    from contextlib import contextmanager

    from flowfile_core.auth import sharing
    from flowfile_core.project.importer import _bind_project_sharepoint_nodes

    with contextmanager(app.dependency_overrides[get_db])() as db:
        connection = db.get(SharePointConnection, saved_connection)
        if delegated:
            connection.auth_method = "delegated"
        monkeypatch.setattr(sharing, "browser_sign_in_allowed", lambda: True)
        local = {"nodes": [{"id": 1, "type": "sharepoint_reader", "setting_input": {
            "connection_id": saved_connection,
        }}]}
        settings = {"connection_id": 9999}
        incoming = {"nodes": [{"id": 1, "type": "sharepoint_reader", "setting_input": settings}]}
        _bind_project_sharepoint_nodes(db, incoming, connection.user_id, local)
        assert settings["connection_id"] == saved_connection
        _bind_project_sharepoint_nodes(db, incoming, connection.user_id)
        assert settings["connection_id"] is None
        _bind_project_sharepoint_nodes(db, incoming, connection.user_id + 1, local)
        assert settings["connection_id"] is None


def test_sqlite_upgrade_preserves_saved_connections_and_does_not_reuse_ids(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'catalog.db'}")
    table = SharePointConnection.__table__
    saved = {
        "connection_name": "Existing SharePoint connection",
        "tenant_id": "tenant",
        "client_id": "client",
        "default_site_url": "https://example.sharepoint.com/sites/Finance",
        "auth_method": "client_secret",
        "credential_secret_id": 11,
        "user_id": 7,
    }
    try:
        with engine.begin() as db:
            db.exec_driver_sql("PRAGMA foreign_keys=ON")
            User.__table__.create(db)
            Secret.__table__.create(db)
            db.execute(User.__table__.insert().values(id=7, username="owner"))
            db.execute(Secret.__table__.insert().values(id=11, user_id=7, encrypted_value="opaque-fixture"))
            with Operations.context(MigrationContext.configure(db)):
                import_module("flowfile_core.alembic.versions.046_sharepoint_connections").upgrade()
            old_table = Table(table.name, MetaData(), autoload_with=db)
            db.execute(old_table.insert().values(id=42, **saved))

        with engine.begin() as db, Operations.context(MigrationContext.configure(db)):
            import_module("flowfile_core.alembic.versions.047_sharepoint_connection_ids").upgrade()
            db.execute(old_table.insert().values(id=99, **saved))
            db.execute(old_table.delete().where(old_table.c.id == 99))
            import_module("flowfile_core.alembic.versions.048_sharepoint_project_identity").upgrade()
            import_module("flowfile_core.alembic.versions.049_sharepoint_excel_preview").upgrade()
            cursor_migration = import_module("flowfile_core.alembic.versions.050_sharepoint_delta_cursor")
            assert cursor_migration.down_revision == "049"
            cursor_migration.upgrade()
            cursor_table = Table("sharepoint_delta_cursors", MetaData(), autoload_with=db)
            db.execute(cursor_table.insert().values(scope="a" * 64, state="fixture-state"))
            assert db.execute(select(cursor_table.c.state)).scalar_one() == "fixture-state"
            cursor_migration.downgrade()
            row = dict(db.execute(select(table)).mappings().one())
            assert row.pop("connection_uuid")
            assert row == {"id": 42, "excel_column_preview": False, **saved}
            assert db.execute(select(Secret.encrypted_value).where(Secret.id == 11)).scalar_one() == "opaque-fixture"

        with engine.begin() as db:
            db.execute(table.delete().where(table.c.id == 42))
            assert db.execute(table.insert().values(**saved)).inserted_primary_key[0] > 99
            assert db.exec_driver_sql("PRAGMA foreign_key_check").all() == []
    finally:
        engine.dispose()


def test_create_sharepoint_connection_encrypts_secret(saved_connection):
    assert saved_connection > 0


def test_lists_only_owned_sharepoint_connections(saved_connection, monkeypatch):
    client = TestClient(app)
    response = client.get("/sharepoint_connections")
    assert response.status_code == 200, response.text
    assert response.json() == [
        {
            "id": saved_connection,
            "connection_name": "test SharePoint",
            "tenant_id": "a01b7819-6a0a-47c9-a6ff-e1563529ac33",
            "client_id": "45d666c4-a4d2-4aad-905a-15443f348233",
            "auth_method": "client_secret",
            "excel_column_preview": False,
            "is_owner": True,
            "can_manage": True,
            "default_site_url": "https://example.sharepoint.com/sites/Finance",
        }
    ]
    monkeypatch.setitem(app.dependency_overrides, get_current_active_user, lambda: SimpleNamespace(id=2))
    response = client.get("/sharepoint_connections")
    assert response.status_code == 200
    assert response.json() == []


def test_group_grant_allows_using_saved_sharepoint_connection(saved_connection, monkeypatch):
    from flowfile_core.database.models import ResourceGrant, SharePointConnection, UserGroup, UserGroupMembership
    from flowfile_core.routes.sharepoint_connections import build_read_settings

    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    db_session = app.dependency_overrides[get_db]()
    db = next(db_session)
    try:
        for model in (UserGroup, UserGroupMembership, ResourceGrant):
            model.__table__.create(db.get_bind())
        db.add_all([User(id=2, username="member"), User(id=3, username="outsider")])
        db.add(UserGroup(id=1, name="Finance", created_by=1))
        db.add(UserGroupMembership(user_id=2, group_id=1))
        db.commit()
        monkeypatch.setitem(
            app.dependency_overrides,
            get_current_active_user,
            lambda: SimpleNamespace(id=1, is_admin=False),
        )
        client = TestClient(app)
        grant = client.post(
            "/shares",
            json={
                "resource_type": "sharepoint_connection",
                "resource_id": saved_connection,
                "group_id": 1,
                "permission": "use",
            },
        )
        assert grant.is_success, grant.text
        secret_id = db.get(SharePointConnection, saved_connection).credential_secret_id
        denied_secret = client.post(
            "/shares",
            json={
                "resource_type": "secret",
                "resource_id": secret_id,
                "group_id": 1,
                "permission": "use",
            },
        )
        assert denied_secret.status_code == 404
        monkeypatch.setitem(app.dependency_overrides, get_current_active_user, lambda: SimpleNamespace(id=2))
        listed = TestClient(app).get("/sharepoint_connections").json()
        assert [row["id"] for row in listed] == [saved_connection]
        assert listed[0]["is_owner"] is False
        assert listed[0]["can_manage"] is False
        assert "fixture-client-secret" not in str(listed)
        wire = build_read_settings(db, 2, saved_connection, "drive", "file.csv")
        assert decrypt_secret(wire.client_secret_encrypted, 1).get_secret_value() == "fixture-client-secret"
        with pytest.raises(HTTPException) as denied:
            build_read_settings(db, 3, saved_connection, "drive", "file.csv")
        assert denied.value.status_code == 404
        path = f"/sharepoint_connections/{saved_connection}"
        metadata = {"connection_name": "Updated"}
        assert client.patch(path, json={**metadata, "default_site_url": None}).status_code == 404
        assert client.delete(path).status_code == 404

        actor = app.dependency_overrides[get_current_active_user]
        monkeypatch.setitem(
            app.dependency_overrides, get_current_active_user, lambda: SimpleNamespace(id=1, is_admin=False)
        )
        promoted = client.patch(f"/shares/{grant.json()['id']}", json={"permission": "manage"})
        assert promoted.status_code == 200, promoted.text
        monkeypatch.setitem(app.dependency_overrides, get_current_active_user, actor)
        assert client.get("/sharepoint_connections").json()[0]["can_manage"] is True
        assert client.patch(path, json={**metadata, "client_id": "not editable"}).status_code == 422
        assert client.patch(path, json={**metadata, "default_site_url": "https://example.org"}).status_code == 422
        for site in (None, "https://example.sharepoint.com/sites/Other"):
            changed = client.patch(path, json={**metadata, "default_site_url": site})
            assert changed.status_code == 422
            assert "new connection" in changed.json()["detail"]
        edited = client.patch(path, json=metadata)
        assert edited.status_code == 200, edited.text
        assert edited.json()["id"] == saved_connection
        assert edited.json()["connection_name"] == "Updated"
        assert edited.json()["default_site_url"] == "https://example.sharepoint.com/sites/Finance"
        db.expire_all()
        assert db.get(Secret, secret_id).encrypted_value == wire.client_secret_encrypted

        assert client.delete(path).status_code == 204
        db.expire_all()
        assert db.get(SharePointConnection, saved_connection) is None
        assert db.get(Secret, secret_id) is None
        assert (
            not db.query(ResourceGrant)
            .filter_by(resource_type="sharepoint_connection", resource_id=saved_connection)
            .count()
        )
        assert client.get("/sharepoint_connections").json() == []
        monkeypatch.setitem(app.dependency_overrides, get_current_active_user, lambda: SimpleNamespace(id=1))
        replacement = client.post(
            "/sharepoint_connections",
            json={
                **edited.json(),
                "client_secret": "fixture-replacement",
            },
        )
        assert replacement.status_code == 201, replacement.text
        assert replacement.json()["id"] != saved_connection
    finally:
        db_session.close()


def test_opening_saved_flow_does_not_grant_sharepoint_data(saved_connection, monkeypatch, tmp_path):
    from contextlib import contextmanager
    from datetime import datetime

    from flowfile_core.configs import settings
    from flowfile_core.database.models import ResourceGrant, UserGroup, UserGroupMembership
    from flowfile_core.flowfile.flow_graph.builders import connectors
    from flowfile_core.flowfile.flow_graph.connections import add_connection
    from flowfile_core.flowfile.handler import FlowfileHandler
    from flowfile_core.flowfile.share.transform import build_share_envelope
    from flowfile_core.routes import routes
    from flowfile_core.run_flow_cli import _run_flow
    from flowfile_core.schemas.input_schema import NodeConnection, NodeSharePointReader, NodeSort
    from flowfile_core.schemas.schemas import FlowSettings
    from flowfile_worker import spawner
    from flowfile_worker.external_sources import sharepoint_source
    from shared import run_completion
    from shared.models import FlowRun

    requests = []

    def microsoft(request):
        requests.append(request.url.host)
        if request.method == "POST":
            return httpx.Response(200, json={"access_token": "fixture-token"})
        return httpx.Response(200, content=b"sku,quantity\nPRIVATE-QA-ROW,42\n")

    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    monkeypatch.setattr(spawner, "in_process", True)
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", True)
    monkeypatch.setattr(connectors, "get_db_context", contextmanager(app.dependency_overrides[get_db]))
    monkeypatch.setattr(
        sharepoint_source, "_http_client", lambda: httpx.Client(transport=httpx.MockTransport(microsoft))
    )
    handler = FlowfileHandler()
    monkeypatch.setattr(routes, "flow_file_handler", handler)
    with next(app.dependency_overrides[get_db]()) as db:
        for model in (UserGroup, UserGroupMembership, ResourceGrant):
            model.__table__.create(db.get_bind())
        db.add_all([User(id=2, username="member"), User(id=3, username="outsider")])
        db.add(UserGroup(id=1, name="Finance", created_by=1))
        db.add(UserGroupMembership(user_id=2, group_id=1))
        db.commit()
        actor = SimpleNamespace(id=1, is_admin=False)
        monkeypatch.setitem(app.dependency_overrides, get_current_active_user, lambda: actor)
        client = TestClient(app)
        grant = client.post(
            "/shares",
            json={
                "resource_type": "sharepoint_connection",
                "resource_id": saved_connection,
                "group_id": 1,
                "permission": "use",
            },
        )
        assert grant.is_success, grant.text
        owner_graph = handler.register_flow(
            FlowSettings(
                flow_id=116, name="Shared file", path="", execution_location="local", execution_mode="Development"
            ),
            user_id=1,
        )
        owner_graph.add_sharepoint_reader(
            NodeSharePointReader(
                flow_id=116,
                node_id=1,
                user_id=1,
                connection_id=saved_connection,
                drive_id="test-drive",
                file_path="sales.csv",
            )
        )
        owner_graph.add_sort(NodeSort(flow_id=116, node_id=2, sort_input=[]))
        add_connection(owner_graph, NodeConnection.create_from_simple_input(1, 2))
        path = tmp_path / "shared.yaml"
        owner_graph.save_flow(str(path))
        saved_text = path.read_text()
        assert "fixture-client-secret" not in saved_text
        assert "$ffsec$" not in saved_text
        assert "client_secret_encrypted" not in saved_text
        shared_flow = build_share_envelope(owner_graph).envelope["flow"]
        shared_node, downstream = shared_flow["nodes"]
        assert shared_node["type"] == "sharepoint_reader__unsupported"
        assert downstream["input_ids"] == [shared_node["id"]]
        assert shared_node["setting_input"]["is_placeholder"] is True
        assert shared_node["setting_input"]["original_type"] == "sharepoint_reader"
        assert "SharePoint connections" in shared_node["setting_input"]["reason"]
        for sensitive in ("connection_id", "test-drive", "sales.csv", "$ffsec$", "fixture-client-secret"):
            assert sensitive not in str(shared_flow)

        FlowRun.__table__.create(db.get_bind(), checkfirst=True)
        scheduled = FlowRun(
            flow_name="SharePoint scheduled",
            flow_path=str(path),
            user_id=2,
            run_type="scheduled",
            started_at=datetime.now(UTC),
        )
        db.add(scheduled)
        db.commit()
        run_id = scheduled.id
        monkeypatch.setattr(run_completion, "get_catalog_engine", lambda _: db.get_bind())
        completions = []
        monkeypatch.setattr("flowfile_core.run_flow_cli._complete_run", lambda _, **fields: completions.append(fields))
        monkeypatch.setattr("flowfile_core.run_flow_cli.core_telemetry.install_headless", lambda: None)
        monkeypatch.setattr("flowfile_core.run_flow_cli.core_telemetry.flush", lambda _: None)
        with monkeypatch.context() as cli:
            from flowfile_core.flowfile.execution import registry

            cli.setattr(registry, "set_default_override", lambda _: None)
            assert _run_flow(str(path), run_id, "local") == 0
        assert completions[-1]["success"] is True
        assert "$ffsec$" not in completions[-1]["node_results_json"]

        def preview(flow_id, node_id=1):
            return client.get("/node/data", params={"flow_id": flow_id, "node_id": node_id})

        def run(graph):
            graph.flow_settings.execution_location = "local"
            return graph.run_graph().node_step_result[0]

        assert run(owner_graph).success
        assert "PRIVATE-QA-ROW" in preview(116).text
        actor.id = 3
        assert preview(116).status_code == 404
        outsider_id = handler.import_flow(path, user_id=3)
        assert outsider_id != 116
        before = len(requests)
        outsider = handler.get_flow(outsider_id, 3)
        assert "PRIVATE-QA-ROW" not in preview(outsider_id).text
        assert not run(outsider).success
        assert "PRIVATE-QA-ROW" not in preview(outsider_id).text
        assert len(requests) == before

        actor.id = 2
        member_id = handler.import_flow(path, user_id=2)
        member = handler.get_flow(member_id, 2)
        assert run(member).success
        assert "PRIVATE-QA-ROW" in preview(member_id).text
        assert "PRIVATE-QA-ROW" in preview(member_id, 2).text
        db.query(ResourceGrant).delete()
        db.commit()
        before = len(requests)
        with monkeypatch.context() as cli:
            cli.setattr(registry, "set_default_override", lambda _: None)
            assert _run_flow(str(path), run_id, "local") == 1
        assert completions[-1]["success"] is False
        assert "$ffsec$" not in completions[-1]["node_results_json"]
        assert len(requests) == before
        assert preview(member_id).status_code == 404
        assert preview(member_id, 2).status_code == 404
        assert client.get("/node/data/export", params={"flow_id": member_id, "node_id": 2}).status_code == 404
        assert not run(member).success
        assert preview(member_id).status_code == 404
        assert len(requests) == before
        reopened_id = handler.import_flow(path, user_id=2)
        reopened = handler.get_flow(reopened_id, 2)
        assert not run(reopened).success
        assert "PRIVATE-QA-ROW" not in preview(reopened_id).text
        assert len(requests) == before


def test_notebook_cannot_request_cached_sharepoint_rows_after_connection_deletion(
    saved_connection, monkeypatch, tmp_path
):
    from contextlib import contextmanager
    from pathlib import Path

    import polars as pl

    import flowfile_core
    from flowfile_core.auth.jwt import get_user_or_internal_service
    from flowfile_core.configs import settings
    from flowfile_core.database.models import ResourceGrant
    from flowfile_core.flowfile.flow_graph.builders import connectors
    from flowfile_core.flowfile.flow_graph.connections import add_connection
    from flowfile_core.flowfile.handler import FlowfileHandler
    from flowfile_core.notebook import kernel_runner
    from flowfile_core.schemas.input_schema import NodeConnection, NodeSharePointReader, NodeSort
    from flowfile_core.schemas.schemas import FlowSettings
    from flowfile_worker import spawner
    from flowfile_worker.external_sources import sharepoint_source

    calls = []

    def microsoft(request):
        calls.append(request.url.host)
        if request.method == "POST":
            return httpx.Response(200, json={"access_token": "fixture-token"})
        return httpx.Response(200, content=b"sku,quantity\nPRIVATE-QA-ROW,42\n")

    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    monkeypatch.setattr(spawner, "in_process", True)
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", True)
    monkeypatch.setattr(connectors, "get_db_context", contextmanager(app.dependency_overrides[get_db]))
    with connectors.get_db_context() as db:
        ResourceGrant.__table__.create(db.get_bind())
    monkeypatch.setattr(
        sharepoint_source, "_http_client", lambda: httpx.Client(transport=httpx.MockTransport(microsoft))
    )
    handler = FlowfileHandler()
    monkeypatch.setattr(flowfile_core, "flow_file_handler", handler)
    graph = handler.register_flow(
        FlowSettings(flow_id=117, name="Notebook", path="", execution_location="local", execution_mode="Development"),
        user_id=1,
    )
    graph.add_sharepoint_reader(
        NodeSharePointReader(
            flow_id=graph.flow_id,
            node_id=1,
            user_id=1,
            connection_id=saved_connection,
            drive_id="test-drive",
            file_path="sales.csv",
        )
    )
    graph.add_sort(NodeSort(flow_id=117, node_id=2, sort_input=[]))
    add_connection(graph, NodeConnection.create_from_simple_input(1, 2))
    assert all(step.success for step in graph.run_graph().node_step_result)

    kernel_id = "sharepoint-notebook"
    monkeypatch.setitem(kernel_runner._sessions, graph.flow_id, {kernel_id: None})
    monkeypatch.setattr(kernel_runner, "_results", {})
    manager = SimpleNamespace(
        shared_volume_path=str(tmp_path),
        get_kernel_owner=lambda _: 1,
        to_kernel_path=lambda path: path,
    )
    monkeypatch.setattr(kernel_runner, "_manager", lambda: manager)
    monkeypatch.setitem(app.dependency_overrides, get_user_or_internal_service, lambda: SimpleNamespace(id=1))
    client = TestClient(app)

    def request_rows(node_id):
        return client.post(
            "/notebook/session/node_result",
            headers={"X-Kernel-Id": kernel_id},
            json={"flow_id": graph.flow_id, "node_id": node_id},
        )

    before = len(calls)
    response = request_rows(2)
    assert response.status_code == 200, response.text
    path = Path(response.json()["path"])
    assert pl.read_parquet(path).rows() == [("PRIVATE-QA-ROW", 42)]

    assert client.delete(f"/sharepoint_connections/{saved_connection}").status_code == 204
    for node_id in (1, 2):
        assert graph.get_node(node_id).results.resulting_data is not None
        response = request_rows(node_id)
        assert response.status_code == 404, response.text
        assert "path" not in response.json()
    assert list(tmp_path.rglob("*.parquet")) == [path]
    assert len(calls) == before


def test_sharepoint_credential_is_not_a_generic_secret(saved_connection):
    from flowfile_core.database.models import Secret, SharePointConnection

    with next(app.dependency_overrides[get_db]()) as db:
        connection = db.get(SharePointConnection, saved_connection)
        name = db.get(Secret, connection.credential_secret_id).name
    client = TestClient(app)
    assert client.get(f"/secrets/secrets/{name}").status_code == 404
    assert name not in {row["name"] for row in client.get("/secrets/secrets").json()}
    assert client.delete(f"/secrets/secrets/{name}").status_code == 404


@pytest.mark.parametrize("opt_in,consented", [(False, True), (True, False), (True, True)])
def test_delegated_excel_preview_requires_opt_in_and_consent(saved_connection, monkeypatch, opt_in, consented):
    from urllib.parse import parse_qs

    from flowfile_core.routes import sharepoint_connections

    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    scopes = []

    def microsoft(req):
        if req.url.path.endswith("/devicecode"):
            scopes.append(parse_qs(req.content.decode())["scope"][0])
            return httpx.Response(200, json={
                "device_code": "private-code", "user_code": "PUBLIC", "expires_in": 60,
                "verification_uri": "https://microsoft.com/devicelogin",
            })
        return httpx.Response(200, json={
            "refresh_token": "private-refresh", "scope": "Files.ReadWrite" if consented else "Files.Read.All",
        })

    monkeypatch.setattr(
        sharepoint_connections, "_http_client", lambda: httpx.Client(transport=httpx.MockTransport(microsoft))
    )
    client = TestClient(app)
    start = client.post("/sharepoint_connections/device/start", json={
        "connection_name": "Personal Excel", "tenant_id": "a01b7819-6a0a-47c9-a6ff-e1563529ac33",
        "client_id": "45d666c4-a4d2-4aad-905a-15443f348233", "excel_column_preview": opt_in,
    })

    def finish(response):
        poll = f"/sharepoint_connections/device/{response.json()['flow_id']}"
        finished = client.post(poll)
        assert finished.json() == {"status": "ready"}
        saved = client.post(f"{poll}/save")
        assert saved.status_code == 201 and "private-" not in saved.text
        return saved.json()

    assert start.is_success and "private-" not in start.text
    connection = finish(start)
    assert ("https://graph.microsoft.com/Files.ReadWrite" in scopes[0].split()) == opt_in
    assert connection["excel_column_preview"] == (opt_in and consented)
    connection_id = connection["id"]
    with next(app.dependency_overrides[get_db]()) as db:
        build = sharepoint_connections.build_read_settings
        args = (db, 1, connection_id, "drive", "sales.xlsx", "Chosen")
        wire = build(*args, schema_only=True)
        assert wire.excel_column_preview == (opt_in and consented)
        assert wire.sheet_name == "Chosen" and "private-refresh" not in wire.model_dump_json()
        assert not build(*args).excel_column_preview
    reconnected = finish(client.post(f"/sharepoint_connections/{connection_id}/reconnect"))
    assert reconnected["id"] == connection_id and not reconnected["excel_column_preview"]
    assert "Files.ReadWrite" not in scopes[-1]


def test_delegated_sign_in_keeps_tokens_server_side(saved_connection, monkeypatch):
    from urllib.parse import parse_qs

    from flowfile_core.routes import sharepoint_connections

    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    refresh_revoked = False
    transient_statuses = iter([None, 429, 503])
    clock = [100.0]
    monkeypatch.setattr(sharepoint_connections, "monotonic", lambda: clock[0])

    def microsoft(req):
        fields = parse_qs(req.content.decode())
        if req.url.path.endswith("/devicecode"):
            assert "offline_access" in fields["scope"][0]
            assert "Files.ReadWrite" not in fields["scope"][0]
            return httpx.Response(
                200,
                json={
                    "device_code": "private-device-code",
                    "user_code": "PUBLIC",
                    "verification_uri": "https://microsoft.com/devicelogin",
                    "expires_in": 900,
                    "interval": 5,
                },
            )
        if fields["grant_type"] == ["urn:ietf:params:oauth:grant-type:device_code"]:
            assert fields["device_code"] == ["private-device-code"]
            failure = next(transient_statuses, 200)
            if failure is None:
                raise httpx.ReadTimeout("timed out", request=req)
            if failure != 200:
                return httpx.Response(failure)
            return httpx.Response(200, json={"refresh_token": "private-refresh", "access_token": "private-access"})
        assert fields["grant_type"] == ["refresh_token"]
        assert fields["refresh_token"] == ["private-refresh"]
        if refresh_revoked:
            return httpx.Response(400, json={"error": "invalid_grant", "error_description": "private-provider-detail"})
        return httpx.Response(200, json={"access_token": "private-access"})

    monkeypatch.setattr(
        sharepoint_connections, "_http_client", lambda: httpx.Client(transport=httpx.MockTransport(microsoft))
    )
    client = TestClient(app)
    owner = app.dependency_overrides[get_current_active_user]
    payload = {
        "connection_name": "Personal",
        "tenant_id": "a01b7819-6a0a-47c9-a6ff-e1563529ac33",
        "client_id": "45d666c4-a4d2-4aad-905a-15443f348233",
    }
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    assert client.post("/sharepoint_connections/device/start", json=payload).status_code == 404
    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    started = client.post("/sharepoint_connections/device/start", json=payload)
    assert started.is_success, started.text
    assert "private-device-code" not in started.text
    assert started.json()["user_code"] == "PUBLIC"
    poll = f"/sharepoint_connections/device/{started.json()['flow_id']}"
    monkeypatch.setitem(app.dependency_overrides, get_current_active_user, lambda: SimpleNamespace(id=2))
    assert client.post(poll).status_code == 404
    monkeypatch.setitem(app.dependency_overrides, get_current_active_user, owner)
    for _ in range(3):
        assert client.post(poll).json() == {"status": "pending", "interval": 5}
        assert client.post(poll).json() == {"status": "pending", "interval": 5}
        clock[0] += 5
    finished = client.post(poll)
    assert finished.is_success, finished.text
    assert "private-" not in finished.text
    assert finished.json() == {"status": "ready"}
    assert "Personal" not in {row["connection_name"] for row in client.get("/sharepoint_connections").json()}
    saved = client.post(f"{poll}/save")
    assert saved.is_success, saved.text
    connection = saved.json()
    assert connection["auth_method"] == "delegated"
    assert client.post(poll).status_code == 404
    with monkeypatch.context() as other:
        other.setitem(app.dependency_overrides, get_current_active_user, lambda: SimpleNamespace(id=2))
        assert connection["id"] not in {row["id"] for row in client.get("/sharepoint_connections").json()}
        refused = client.post(
            "/sharepoint_connections/test", json={"connection_id": connection["id"], "drive_id": "drive"}
        )
        assert refused.status_code == 404
    retry = client.post("/sharepoint_connections/device/start", json=payload)
    cancel_path = f"/sharepoint_connections/device/{retry.json()['flow_id']}"
    assert client.post(cancel_path).json() == {"status": "ready"}
    monkeypatch.setitem(app.dependency_overrides, get_current_active_user, lambda: SimpleNamespace(id=2))
    assert client.delete(cancel_path).status_code == 404
    monkeypatch.setitem(app.dependency_overrides, get_current_active_user, owner)
    assert client.delete(cancel_path).status_code == 204
    assert client.post(f"{cancel_path}/save").status_code == 404
    with next(app.dependency_overrides[get_db]()) as db:
        wire = sharepoint_connections.build_read_settings(db, 1, connection["id"], "drive", "file.csv")
        assert wire.auth_method == "delegated"
        credential = decrypt_secret(wire.client_secret_encrypted, 1).get_secret_value()
        with sharepoint_connections._http_client() as upstream:
            assert (
                sharepoint_connections.acquire_token(
                    upstream, str(wire.tenant_id), str(wire.client_id), credential, auth_method=wire.auth_method
                )
                == "private-access"
            )
        refresh_revoked = True
        revoked = client.post(
            "/sharepoint_connections/test", json={"connection_id": connection["id"], "drive_id": "drive"}
        )
        assert revoked.status_code == 422
        assert "Sign in again" in revoked.json()["detail"]
        assert "private-" not in revoked.text
        reconnect_url = f"/sharepoint_connections/{connection['id']}/reconnect"
        with monkeypatch.context() as other:
            other.setitem(app.dependency_overrides, get_current_active_user, lambda: SimpleNamespace(id=2))
            assert client.post(reconnect_url).status_code == 404
        restart = client.post(reconnect_url)
        assert restart.status_code == 200
        attempt = f"/sharepoint_connections/device/{restart.json()['flow_id']}"
        assert client.post(attempt).json() == {"status": "ready"}
        assert client.delete(attempt).status_code == 204
        assert (
            sharepoint_connections.build_read_settings(
                db, 1, connection["id"], "drive", "file.csv"
            ).client_secret_encrypted
            == wire.client_secret_encrypted
        )
        restart = client.post(reconnect_url)
        attempt = f"/sharepoint_connections/device/{restart.json()['flow_id']}"
        assert client.post(attempt).json() == {"status": "ready"}
        with monkeypatch.context() as other:
            other.setitem(app.dependency_overrides, get_current_active_user, lambda: SimpleNamespace(id=2))
            assert client.post(f"{attempt}/save").status_code == 404
        assert client.post(f"{attempt}/save").json() == connection
        assert (
            sharepoint_connections.build_read_settings(
                db, 1, connection["id"], "drive", "file.csv"
            ).client_secret_encrypted
            != wire.client_secret_encrypted
        )
        monkeypatch.setenv("FLOWFILE_MODE", "docker")
        with pytest.raises(HTTPException):
            sharepoint_connections.build_read_settings(db, 1, connection["id"], "drive", "file.csv")


def test_abandoned_device_sign_in_is_removed_without_another_request(saved_connection, monkeypatch):
    from time import monotonic, sleep

    from flowfile_core.routes import sharepoint_connections

    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    transport = httpx.MockTransport(
        lambda req: httpx.Response(
            200,
            json={
                "device_code": "synthetic-device-code",
                "user_code": "PUBLIC",
                "expires_in": 1,
            },
        )
    )
    monkeypatch.setattr(sharepoint_connections, "_http_client", lambda: httpx.Client(transport=transport))
    client = TestClient(app)
    started = client.post(
        "/sharepoint_connections/device/start",
        json={
            "connection_name": "Abandoned",
            "tenant_id": "a01b7819-6a0a-47c9-a6ff-e1563529ac33",
            "client_id": "45d666c4-a4d2-4aad-905a-15443f348233",
        },
    )
    assert started.status_code == 200
    flow_id = started.json()["flow_id"]
    with sharepoint_connections._device_lock:
        assert flow_id in sharepoint_connections._device_flows
    try:
        deadline = monotonic() + 3
        while monotonic() < deadline:
            with sharepoint_connections._device_lock:
                if flow_id not in sharepoint_connections._device_flows:
                    break
            sleep(0.05)
        else:
            pytest.fail("Abandoned sign-in was retained after expiry")
    finally:
        client.delete(f"/sharepoint_connections/device/{flow_id}")


def test_admin_default_site_is_a_deployment_hint(saved_connection, monkeypatch):
    monkeypatch.setenv("FLOWFILE_SHAREPOINT_DEFAULT_SITE_URL", "https://example.sharepoint.com/sites/Finance")
    client = TestClient(app)
    response = client.get("/sharepoint_connections/browse_defaults")
    assert response.status_code == 200
    assert response.json() == {"site_url": "https://example.sharepoint.com/sites/Finance"}
    monkeypatch.delenv("FLOWFILE_SHAREPOINT_DEFAULT_SITE_URL")
    assert client.get("/sharepoint_connections/browse_defaults").json() == {"site_url": None}


def test_search_sites_returns_metadata_and_requires_connection_access(saved_connection, monkeypatch):
    from flowfile_core.routes import sharepoint_connections

    calls = []
    status = 200
    site = {"id": "site-id", "displayName": "Finance", "webUrl": "https://example.sharepoint.com/sites/Finance"}

    def upstream(req):
        calls.append(req)
        if req.method == "POST":
            return httpx.Response(200, json={"access_token": "fixture-token"})
        assert req.url.host == "graph.microsoft.com" and req.url.path == "/v1.0/sites"
        assert req.url.params["search"] == "Finance & Ops"
        assert req.url.params["$select"] == "id,displayName,webUrl"
        assert req.headers["Authorization"] == "Bearer fixture-token"
        if status != 200:
            return httpx.Response(status, json={"error": {"message": "fixture-client-secret"}})
        return httpx.Response(200, json={"value": [{**site, "private": "not metadata"}]})

    monkeypatch.setattr(
        sharepoint_connections, "_http_client", lambda: httpx.Client(transport=httpx.MockTransport(upstream))
    )
    client = TestClient(app)
    url = f"/sharepoint_connections/{saved_connection}/sites"
    params = {"search": "Finance & Ops"}
    response = client.get(url, params=params)
    assert response.status_code == 200, response.text
    assert response.json() == [site]

    status = 401
    response = client.get(url, params=params)
    assert response.status_code == 422
    assert isinstance(response.json()["detail"], str)
    assert "fixture-client-secret" not in response.text

    count = len(calls)
    monkeypatch.setitem(app.dependency_overrides, get_current_active_user, lambda: SimpleNamespace(id=2))
    assert client.get(url, params=params).status_code == 404
    assert len(calls) == count


def test_list_libraries_for_explicit_site(saved_connection, monkeypatch):
    from flowfile_core.routes import sharepoint_connections

    def upstream(req):
        if req.method == "POST":
            return httpx.Response(200, json={"access_token": "fixture-token"})
        assert req.url.host == "graph.microsoft.com"
        assert req.headers["Authorization"] == "Bearer fixture-token"
        if req.url.path == "/v1.0/sites/example.sharepoint.com:/sites/Finance":
            return httpx.Response(200, json={"id": "example.sharepoint.com,site-id,web-id"})
        if req.url.path.endswith("/lists"):
            return httpx.Response(200, json={"value": [{"id": "list-id", "name": "Inventory"}]})
        if req.url.path.endswith("/lists/list-id/columns"):
            return httpx.Response(200, json={"value": [
                {"name": "Quantity", "displayName": "Item quantity"}, {"name": "Internal", "hidden": True},
            ]})
        assert req.url.path == "/v1.0/sites/example.sharepoint.com,site-id,web-id/drives"
        return httpx.Response(200, json={"value": [{"id": "drive-id", "name": "Documents"}]})

    monkeypatch.setattr(
        sharepoint_connections, "_http_client", lambda: httpx.Client(transport=httpx.MockTransport(upstream))
    )
    client = TestClient(app)
    url = f"/sharepoint_connections/{saved_connection}/libraries"
    params = {"site_url": "https://example.sharepoint.com/sites/Finance"}
    response = client.get(url, params=params)
    assert response.status_code == 200, response.text
    assert response.json() == [{"id": "drive-id", "name": "Documents"}]
    assert client.get(url, params={"site_url": "https://example.test/sites/Finance"}).status_code == 422
    list_url = f"/sharepoint_connections/{saved_connection}/lists"
    site_id = "example.sharepoint.com,site-id,web-id"
    columns_url = f"{list_url}/{site_id}/list-id/columns"
    assert client.get(list_url, params=params).json() == {
        "site_id": site_id, "lists": [{"id": "list-id", "name": "Inventory"}],
    }
    assert client.get(columns_url).json() == [{"id": "Quantity", "name": "Item quantity"}]
    monkeypatch.setitem(app.dependency_overrides, get_current_active_user, lambda: SimpleNamespace(id=2))
    assert client.get(url, params=params).status_code == 404
    assert client.get(list_url, params=params).status_code == 404
    assert client.get(columns_url).status_code == 404


@pytest.mark.parametrize("drive_type,status", [("business", 200), ("personal", 422)])
def test_discover_organizational_onedrive(saved_connection, monkeypatch, drive_type, status):
    from flowfile_core.routes import sharepoint_connections

    user_id = "a39aa228-d355-4820-a125-9ad8d71413e2"

    def upstream(req):
        if req.url.host == "login.microsoftonline.com":
            return httpx.Response(200, json={"access_token": "fixture-token"})
        assert req.url.path == f"/v1.0/users/{user_id}/drives"
        assert req.headers["Authorization"] == "Bearer fixture-token"
        return httpx.Response(200, json={"value": [
            {"id": "b!fixed-drive", "name": "OneDrive", "driveType": drive_type},
        ]})

    monkeypatch.setattr(
        sharepoint_connections, "_http_client", lambda: httpx.Client(transport=httpx.MockTransport(upstream))
    )
    client = TestClient(app)
    url = f"/sharepoint_connections/{saved_connection}/onedrive"
    assert client.get(url).status_code == 422
    response = client.get(url, params={"user_id": user_id})
    assert response.status_code == status, response.text
    if status == 200:
        assert response.json() == [{"id": "b!fixed-drive", "name": "OneDrive"}]


def test_browse_drive_returns_only_metadata(saved_connection, monkeypatch):
    from flowfile_core.routes import sharepoint_connections

    def upstream(req):
        if req.method == "POST":
            return httpx.Response(200, json={"access_token": "fixture-token"})
        assert req.url.path == "/v1.0/drives/test-drive/root/children"
        assert req.headers["Authorization"] == "Bearer fixture-token"
        if req.url.params.get("$skiptoken") == "next":
            return httpx.Response(
                200,
                json={
                    "value": [
                        {"id": "file-id", "name": "sales.csv", "file": {}, "@microsoft.graph.downloadUrl": "private"},
                    ]
                },
            )
        assert req.url.params["$select"] == "id,name,folder,file"
        return httpx.Response(
            200,
            json={
                "value": [{"id": "folder-id", "name": "Reports", "folder": {}}],
                "@odata.nextLink": "https://graph.microsoft.com/v1.0/drives/test-drive/root/children?$skiptoken=next",
            },
        )

    monkeypatch.setattr(
        sharepoint_connections, "_http_client", lambda: httpx.Client(transport=httpx.MockTransport(upstream))
    )
    client = TestClient(app)
    url = f"/sharepoint_connections/{saved_connection}/drives/test-drive/children"
    response = client.get(url)
    assert response.status_code == 200, response.text
    assert response.json() == [
        {"id": "folder-id", "name": "Reports", "is_folder": True},
        {"id": "file-id", "name": "sales.csv", "is_folder": False},
    ]
    monkeypatch.setitem(app.dependency_overrides, get_current_active_user, lambda: SimpleNamespace(id=2))
    assert client.get(url).status_code == 404


def test_sharepoint_graph_registers_source():
    from flowfile_core.flowfile.handler import FlowfileHandler
    from flowfile_core.flowfile.manage.io_flowfile import populate_graph_from_flow_information
    from flowfile_core.schemas.schemas import FlowInformation, FlowSettings

    handler = FlowfileHandler()
    settings = FlowSettings(flow_id=1, name="sharepoint-test", path=".", execution_location="remote")
    handler.register_flow(settings)
    graph = handler.get_flow(1)
    assert graph is not None
    flow = FlowInformation.model_validate(
        {
            "flow_id": 1,
            "flow_settings": settings,
            "node_starts": [1],
            "data": {
                1: {
                    "id": 1,
                    "type": "sharepoint_reader",
                    "is_setup": True,
                    "setting_input": {
                        "flow_id": 1,
                        "node_id": 1,
                        "user_id": 99,
                        "connection_id": 1,
                        "drive_id": "test-drive",
                        "file_path": "sales.xlsx",
                        "sheet_name": "Sales",
                    },
                }
            },
        }
    )
    populate_graph_from_flow_information(graph, flow, owner_of=lambda _: 1)
    node = graph.get_node(1)
    assert node is not None
    assert node.node_type == "sharepoint_reader"
    assert node.setting_input.user_id == 1
    assert node.setting_input.sheet_name == "Sales"


def test_saved_connection_wire_payload_reads_csv(saved_connection, monkeypatch):
    from flowfile_core.routes import sharepoint_connections
    from flowfile_worker.external_sources import sharepoint_source

    def handle(request):
        if request.method == "POST":
            assert b"client_secret=fixture-client-secret" in request.content
            return httpx.Response(200, json={"access_token": "fixture-token"})
        assert request.url.path == "/v1.0/drives/test-drive/root:/sales.csv:/content"
        return httpx.Response(200, content=b"sku,quantity\nA,2\nB,5\n")

    def client():
        return httpx.Client(transport=httpx.MockTransport(handle))

    database = app.dependency_overrides[get_db]()
    try:
        db = next(database)
        payload = sharepoint_connections.build_read_settings(db, 1, saved_connection, "test-drive", "sales.csv")
        with pytest.raises(HTTPException) as exc:
            sharepoint_connections.build_read_settings(db, 2, saved_connection, "test-drive", "sales.csv")
        assert exc.value.status_code == 404
        wire = payload.model_dump_json()
        assert "fixture-client-secret" not in wire
        monkeypatch.setattr(sharepoint_source, "_http_client", client)
        frame = sharepoint_source.read_saved_file(type(payload).model_validate_json(wire))
        assert frame.to_dicts() == [{"sku": "A", "quantity": 2}, {"sku": "B", "quantity": 5}]
    finally:
        database.close()


@pytest.mark.parametrize(
    ("source", "exported", "write_output"),
    [
        pytest.param("file", False, None, id="onedrive-public-api"),
        pytest.param("list", True, None, id="list-read-export"),
        pytest.param("file", False, "file", id="csv-public-api"),
        pytest.param("file", True, "file", id="csv-export"),
        pytest.param("list", False, "list", id="list-public-api"),
        pytest.param("list", True, "list", id="list-export"),
        pytest.param("file", True, "append", id="excel-append-export"),
    ],
)
def test_read_sharepoint_in_python_without_worker_service(
    saved_connection, monkeypatch, exported, source, write_output,
):
    from contextlib import contextmanager
    from io import BytesIO

    from openpyxl import Workbook, load_workbook

    import flowfile as ff
    from flowfile_core.configs import settings
    from flowfile_core.flowfile.code_generator.code_generator import (
        FlowGraphToFlowFrameConverter,
        export_flow_to_flowframe,
    )
    from flowfile_core.flowfile.execution import registry
    from flowfile_core.flowfile.flow_graph.builders import connectors
    from flowfile_core.routes import sharepoint_connections
    from flowfile_frame import _identity
    from flowfile_worker import spawner
    from flowfile_worker.external_sources import sharepoint_source, sharepoint_writer

    uploaded = []
    onedrive_user_id = "a39aa228-d355-4820-a125-9ad8d71413e2"
    onedrive_id = "b!fixed-drive"
    drive_id = onedrive_id if source == "file" else "test-drive"
    workbook_path = f"/v1.0/drives/{drive_id}/root:/Reports/output.xlsx"
    existing_item_path = f"/v1.0/drives/{drive_id}/items/workbook"
    original = BytesIO()
    if write_output == "append":
        workbook = Workbook()
        workbook.active.title = "Keep"
        workbook.active["A1"] = "=1+1"
        sheet = workbook.create_sheet("Report")
        sheet.append(["sku", "quantity"])
        sheet.append(["Existing", 1])
        workbook.save(original)

    def microsoft(request):
        if request.url.host == "login.microsoftonline.com":
            return httpx.Response(200, json={"access_token": "fixture-token"})
        if request.url.path == f"/v1.0/users/{onedrive_user_id}/drives":
            return httpx.Response(200, json={"value": [
                {"id": onedrive_id, "name": "OneDrive", "driveType": "business"},
            ]})
        if request.url.path.endswith("/lists/output-list/columns"):
            return httpx.Response(200, json={"value": [{"name": "Title", "text": {}}]})
        if write_output == "append":
            if request.method == "GET" and request.url.path == workbook_path:
                return httpx.Response(200, json={"id": "workbook", "eTag": '"v1"'})
            if request.method == "GET" and request.url.path == f"{existing_item_path}/content":
                return httpx.Response(200, content=original.getvalue())
            if request.method == "POST":
                assert request.url.path == f"{existing_item_path}/createUploadSession"
                assert request.headers["If-Match"] == '"v1"'
                return httpx.Response(200, json={"uploadUrl": "https://upload.sharepoint.com/session"})
            if request.method == "PUT":
                assert str(request.url) == "https://upload.sharepoint.com/session"
                uploaded.append(request.content)
                return httpx.Response(201, json={"id": "workbook"})
        if request.method == "POST" and write_output == "list":
            assert request.url.path == "/v1.0/sites/example.sharepoint.com,site,web/lists/output-list/items"
            uploaded.append(json.loads(request.content))
            return httpx.Response(201, json={"id": "created"})
        if request.method == "PUT":
            assert request.url.path == f"/v1.0/drives/{drive_id}/root:/Reports/output.csv:/content"
            uploaded.append(request.content)
            return httpx.Response(201, json={"id": "uploaded"})
        if source == "list":
            assert request.url.path == "/v1.0/sites/example.sharepoint.com,site,web/lists/list-id/items"
            assert request.url.params["$filter"] == "fields/quantity gt 0"
            return httpx.Response(200, json={"value": [
                {"fields": {"sku": "A", "quantity": 2}}, {"fields": {"sku": "B", "quantity": 5}},
            ]})
        assert request.url.path == f'/v1.0/drives/{drive_id}/root:/sales "FY".csv:/content'
        return httpx.Response(200, content=b"sku;quantity\nA;2\nB;5\n")

    def no_worker_service():
        pytest.fail("A local SharePoint flow must not select the standalone worker")

    monkeypatch.setattr(
        sharepoint_source, "_http_client", lambda: httpx.Client(transport=httpx.MockTransport(microsoft))
    )
    monkeypatch.setattr(sharepoint_writer, "_http_client", sharepoint_source._http_client)
    monkeypatch.setattr(sharepoint_connections, "_http_client", sharepoint_source._http_client)
    monkeypatch.setattr(connectors, "get_db_context", contextmanager(app.dependency_overrides[get_db]))
    monkeypatch.setattr(_identity, "current_user_id", lambda: 1)
    monkeypatch.setattr(spawner, "in_process", True)
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", True)
    monkeypatch.delitem(registry._instances, registry.WORKER_BACKEND_ID, raising=False)
    monkeypatch.setitem(registry._factories, registry.WORKER_BACKEND_ID, no_worker_service)

    if source == "list":
        frame = ff.read_sharepoint(saved_connection, list_source={
            "site_id": "example.sharepoint.com,site,web", "list_id": "list-id",
            "columns": ["sku", "quantity"], "odata_filter": "fields/quantity gt 0",
        })
    else:
        discovery = TestClient(app).get(f"/sharepoint_connections/{saved_connection}/onedrive",
                                       params={"user_id": onedrive_user_id})
        assert discovery.status_code == 200
        drive_id = discovery.json()[0]["id"]
        assert drive_id == onedrive_id
        frame = ff.read_sharepoint(
            saved_connection, drive_id=drive_id, file_path='sales "FY".csv', csv_delimiter=";",
        )
    if write_output == "list":
        sink = frame.write_sharepoint(saved_connection, list_target={
            "site_id": "example.sharepoint.com,site,web", "list_id": "output-list", "column_mapping": {"sku": "Title"},
        })
        assert not uploaded
    elif write_output == "file":
        sink = frame.write_sharepoint(
            saved_connection, drive_id=drive_id, file_path="Reports/output.csv", format="csv",
            conflict_mode="overwrite",
        )
        assert not uploaded
    elif write_output == "append":
        sink = frame.write_sharepoint(
            saved_connection, drive_id=drive_id, file_path="Reports/output.xlsx", format="xlsx",
            conflict_mode="append", sheet_name="Report",
        )
        preview = frame.flow_graph.get_node_data(sink.node_id, include_example=False)
        assert preview.setting_input.conflict_mode == "append"
        assert preview.setting_input.sheet_name == "Report"
        assert not uploaded
    if exported:
        code = export_flow_to_flowframe(frame.flow_graph)
        assert "ff.read_sharepoint(" in code
        assert "fixture-client-secret" not in code and "$ffsec$" not in code
        notebook_code = FlowGraphToFlowFrameConverter(frame.flow_graph, placeholders=True).convert()
        assert "ff.canvas_node(" not in notebook_code
        assert "ff.read_sharepoint(" in notebook_code
        assert "fixture-client-secret" not in notebook_code and "$ffsec$" not in notebook_code
        assert (".write_sharepoint(" in code) == bool(write_output)
        assert (".write_sharepoint(" in notebook_code) == bool(write_output)
        namespace = {}
        exec(code, namespace)
        frame = namespace["run_etl_pipeline"]()
    elif write_output:
        frame = sink
    assert frame.collect().to_dicts() == [{"sku": "A", "quantity": 2}, {"sku": "B", "quantity": 5}]
    if write_output == "append":
        [content] = uploaded
        result = load_workbook(BytesIO(content))
        assert result.sheetnames == ["Keep", "Report"]
        assert result["Keep"]["A1"].value == "=1+1"
        assert list(result["Report"].values) == [("sku", "quantity"), ("Existing", 1), ("A", 2), ("B", 5)]
        return
    assert uploaded == {
        None: [], "file": [b"sku,quantity\nA,2\nB,5\n"],
        "list": [{"fields": {"Title": "A"}}, {"fields": {"Title": "B"}}],
    }[write_output]


def test_sharepoint_delta_reader_rejects_cached_results():
    from pydantic import ValidationError

    from flowfile_core.schemas.input_schema import NodeSharePointReader

    with pytest.raises(ValidationError, match="Incremental SharePoint reads require .* no caching"):
        NodeSharePointReader(
            flow_id=116,
            node_id=1,
            user_id=1,
            connection_id=1,
            drive_id="test-drive",
            file_path="reports/sales.csv",
            delta_generation=0,
            cache_results=True,
        )


@pytest.mark.parametrize(
    "extension",
    ["csv", "csv-no-range", "csv-glob", "xlsx", "xlsx-preview", "parquet", "parquet-no-range", "json", "json-no-range",
     "list", "list-write", "csv-delta", "csv-delta-development", "csv-delta-list", "csv-delta-list-development", "xls"],
)
def test_sharepoint_graph_reads_file_over_worker_http(saved_connection, monkeypatch, caplog, extension):
    import base64
    import socket
    import threading
    import time
    from contextlib import contextmanager
    from io import BytesIO
    from pathlib import Path

    import uvicorn

    from flowfile_core.auth.jwt import get_internal_token
    from flowfile_core.configs import settings
    from flowfile_core.flowfile.execution import registry
    from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
    from flowfile_core.flowfile.flow_graph.builders import connectors
    from flowfile_core.flowfile.handler import FlowfileHandler
    from flowfile_core.routes import routes
    from flowfile_core.schemas.input_schema import NodeSharePointReader, NodeSharePointWriter
    from flowfile_core.schemas.schemas import FlowSettings
    from flowfile_worker import spawner, status_dict, status_dict_lock
    from flowfile_worker.auth import INTERNAL_TOKEN_HEADER
    from flowfile_worker.external_sources import sharepoint_source
    from flowfile_worker.main import app as worker_app
    from shared.compute_protocol import PROTOCOL_VERSION, PROTOCOL_VERSION_HEADER

    execution_mode = "Development" if extension.endswith("-development") else "Performance"
    delta_read = extension.startswith("csv-delta")
    delta_list = extension.startswith("csv-delta-list")
    list_write = extension == "list-write"
    if delta_read:
        from flowfile_core.database.models import FlowRegistration, SharePointDeltaCursor
        from flowfile_core.flowfile import sharepoint_delta

        extension = "csv"
        db_context = contextmanager(app.dependency_overrides[get_db])
        monkeypatch.setattr(sharepoint_delta, "get_db_context", db_context)
        with db_context() as db:
            for table in (FlowRegistration.__table__, SharePointDeltaCursor.__table__):
                table.create(db.get_bind(), checkfirst=True)
            registration = FlowRegistration(name="SharePoint HTTP", flow_path=".", owner_id=1)
            db.add(registration)
            db.commit()
            registration_id = registration.id
    if list_write:
        extension = "list"
    glob_read = extension == "csv-glob"
    if glob_read:
        extension = "csv"
    ignores_range = extension in {"csv-no-range", "json-no-range"}
    if ignores_range:
        extension = extension.removesuffix("-no-range")
    ignores_tail = extension == "parquet-no-range"
    if ignores_tail:
        extension = "parquet"
    excel_preview = extension == "xlsx-preview"
    if excel_preview:
        from flowfile_core.database.models import SharePointConnection

        monkeypatch.setenv("FLOWFILE_MODE", "electron")
        with next(app.dependency_overrides[get_db]()) as db:
            connection = db.get(SharePointConnection, saved_connection)
            connection.auth_method = "delegated"
            connection.excel_column_preview = True
            db.commit()
        extension = "xlsx"
    filename = "Reports 2026/sales-*.csv" if glob_read else f"sales.{extension}"
    sheet_name = "Sales" if extension in {"xls", "xlsx"} else None
    expected = [{"sku": "A", "quantity": 2}, {"sku": "B", "quantity": 5}]
    content = b"sku;quantity\nA;2\nB;5\n"
    if extension == "parquet":
        import polars as pl

        output = BytesIO()
        pl.DataFrame(expected).write_parquet(output)
        content = output.getvalue()
    elif extension == "json":
        content = b'[{"sku":"A","quantity":2},{"sku":"B","quantity":5}]'
    elif extension == "xls":
        content = (Path(__file__).parent / "fixtures/sharepoint/sales.xls").read_bytes()
        expected = [{"sku": "B", "quantity": 5}]
    elif extension == "xlsx":
        from openpyxl import Workbook

        workbook = Workbook()
        sheet = workbook.create_sheet("Sales")
        sheet.append(["sku", "quantity"])
        sheet.append(["B", 5])
        output = BytesIO()
        workbook.save(output)
        content = output.getvalue()
        expected = [{"sku": "B", "quantity": 5}]

    downloads = []
    delta_tokens = []
    delta_changes = []
    delta_file_exists = True
    delta_sink_statuses = []
    delta_list_posts = []

    def microsoft(request):
        if request.url.host == "login.microsoftonline.com":
            if excel_preview:
                from urllib.parse import parse_qs

                fields = parse_qs(request.content.decode())
                assert fields["grant_type"] == ["refresh_token"]
                if "Files.ReadWrite" in fields["scope"][0]:
                    downloads.append("preview-token")
                    return httpx.Response(200, json={"access_token": "fixture-token", "scope": "Files.ReadWrite"})
            return httpx.Response(200, json={"access_token": "fixture-token"})
        if request.url.host == "graph.microsoft.com":
            assert "Range" not in request.headers
            assert request.headers["Authorization"] == "Bearer fixture-token"
            if delta_read:
                base = "/v1.0/drives/test-drive"
                if request.url.path == f"{base}/root/delta":
                    delta_tokens.append(request.url.params["token"])
                    return httpx.Response(200, json={
                        "value": delta_changes,
                        "@odata.deltaLink": f"https://graph.microsoft.com{base}/root/delta?token=next-{len(delta_tokens)}",
                    })
                if request.url.path == f"{base}/root:/sales.csv:":
                    if not delta_file_exists:
                        return httpx.Response(404)
                    return httpx.Response(200, json={"id": "sales-item", "file": {}})
                if request.method == "PUT" and request.url.path.endswith("/delta-output.csv:/content"):
                    status = 403 if not delta_sink_statuses else 201
                    delta_sink_statuses.append(status)
                    return httpx.Response(status, json={"id": "delta-output"})
            if (list_write or delta_list) and request.method == "GET" and request.url.path.endswith("/columns"):
                return httpx.Response(200, json={"value": [{"name": "Title", "text": {}}]})
            if (list_write or delta_list) and request.method == "POST":
                assert request.url.path == "/v1.0/sites/example.sharepoint.com,site,web/lists/list-id/items"
                (delta_list_posts if delta_list else downloads).append(json.loads(request.content))
                return httpx.Response(201, json={"id": "created"})
            if excel_preview and "/workbook/" in request.url.path:
                worksheets = "/v1.0/drives/test-drive/root:/sales.xlsx:/workbook/worksheets"
                if request.url.path == worksheets:
                    downloads.append("worksheets")
                    return httpx.Response(200, json={"value": [
                        {"id": "first", "name": "Sheet", "position": 0},
                        {"id": "selected", "name": "Sales", "position": 1},
                    ]})
                assert request.url.path == f"{worksheets}/selected/range(address='A1:IV1')"
                downloads.append("header-range")
                return httpx.Response(200, json={"text": [["sku", "quantity", ""]]})
            if extension == "list" and request.method == "GET":
                assert request.url.path == "/v1.0/sites/example.sharepoint.com,site,web/lists/list-id/items"
                downloads.append("items")
                if request.url.params.get("$skiptoken") == "next":
                    return httpx.Response(200, json={"value": [{"fields": {"sku": "B", "quantity": 5}}]})
                assert request.url.params["$expand"] == "fields($select=sku,quantity)"
                assert request.url.params["$filter"] == "fields/quantity gt 0"
                return httpx.Response(200, json={
                    "value": [{"fields": {"sku": "A", "quantity": 2, "private": "ignored"}}],
                    "@odata.nextLink": f"https://graph.microsoft.com{request.url.path}?$skiptoken=next",
                })
            if extension == "list" and request.method == "PUT":
                assert request.url.path == "/v1.0/drives/test-drive/root:/Reports/output.csv:/content"
                assert request.content == b"sku,quantity\nA,2\nB,5\n"
                downloads.append("upload")
                return httpx.Response(201, json={"id": "uploaded"})
            if glob_read and request.url.path.endswith("/children"):
                assert request.url.path == "/v1.0/drives/test-drive/root:/Reports 2026:/children"
                downloads.append("listing")
                if request.url.params.get("$skiptoken") == "next":
                    return httpx.Response(200, json={"value": [{"name": "sales-a.csv", "file": {}}]})
                return httpx.Response(
                    200,
                    json={
                        "value": [{"name": "sales-b.csv", "file": {}}, {"name": "sales-folder.csv", "folder": {}}],
                        "@odata.nextLink": f"https://graph.microsoft.com{request.url.raw_path.split(b'?')[0].decode()}?$skiptoken=next",
                    },
                )
            if glob_read:
                assert request.url.path.startswith("/v1.0/drives/test-drive/root:/Reports 2026/")
                target = request.url.path.split("/")[-2].removesuffix(":")
                return httpx.Response(302, headers={"Location": f"https://download.example.test/{target}"})
            return httpx.Response(302, headers={"Location": f"https://download.example.test/{filename}"})
        assert request.url.host == "download.example.test"
        assert "Authorization" not in request.headers
        if request.headers.get("Range"):
            if extension == "parquet":
                assert request.headers["Range"] == "bytes=-262144"
                downloads.append("tail")
                if ignores_tail:

                    class UnreadBody(httpx.SyncByteStream):
                        def __iter__(self):
                            raise AssertionError("Preview must not consume a full Parquet response")

                    return httpx.Response(200, stream=UnreadBody())
                start = 4
                return httpx.Response(
                    206,
                    headers={"Content-Range": f"bytes {start}-{len(content)-1}/{len(content)}"},
                    content=content[start:],
                )
            assert request.headers["Range"] == "bytes=0-65535"
            downloads.append("range")
            if ignores_range:

                class UnreadBody(httpx.SyncByteStream):
                    def __iter__(self):
                        raise AssertionError("Preview must not consume a full response")

                return httpx.Response(200, stream=UnreadBody())
            if extension == "json":
                sample = b'[ {"sku":"A","quantity":2}, {"sku":"cut'
                return httpx.Response(206, headers={"Content-Range": f"bytes 0-{len(sample)-1}/1000"}, content=sample)

            return httpx.Response(
                206, headers={"Content-Range": f"bytes 0-{len(content)-1}/{len(content)}"}, content=content
            )
        downloads.append(str(request.url))
        if glob_read:
            return httpx.Response(
                200,
                content={
                    "/sales-a.csv": b"sku;quantity\nA;2\n",
                    "/sales-b.csv": b"sku;quantity\nB;5\n",
                }[request.url.path],
            )
        return httpx.Response(200, content=content)

    monkeypatch.setattr(
        sharepoint_source, "_http_client", lambda: httpx.Client(transport=httpx.MockTransport(microsoft))
    )
    from flowfile_worker.external_sources import sharepoint_writer

    monkeypatch.setattr(sharepoint_writer, "_http_client", sharepoint_source._http_client)
    monkeypatch.setattr(connectors, "get_db_context", contextmanager(app.dependency_overrides[get_db]))
    monkeypatch.setattr(spawner, "in_process", True)
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", True)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(worker_app, lifespan="off", log_level="error"))
        thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
        thread.start()
        try:
            deadline = time.monotonic() + 5
            while not server.started and thread.is_alive() and time.monotonic() < deadline:
                time.sleep(0.01)
            assert server.started
            backend = WorkerBackend(url=f"http://127.0.0.1:{port}", shares_core_disk=True)
            writer_plan_checked = False
            if extension == "list":
                caplog.set_level("INFO")
                original_v1 = backend._v1

                def check_writer_plan(method, path, **kwargs):
                    nonlocal writer_plan_checked
                    if path == "/ops/sharepoint_write":
                        body = kwargs["json"]
                        plan = base64.b64decode(body["operation"], validate=True)
                        ciphertext = body["client_secret_encrypted"]
                        assert ciphertext.startswith("$ffsec$")
                        assert ciphertext.encode() not in plan
                        assert b"fixture-client-secret" not in plan and b"fixture-token" not in plan
                        writer_plan_checked = True
                    return original_v1(method, path, **kwargs)

                monkeypatch.setattr(backend, "_v1", check_writer_plan)
            monkeypatch.setitem(registry._instances, registry.WORKER_BACKEND_ID, backend)
            handler = FlowfileHandler()
            monkeypatch.setattr(routes, "flow_file_handler", handler)
            handler.register_flow(
                FlowSettings(
                    flow_id=116, name="SharePoint HTTP", path=".", execution_location="remote",
                    execution_mode=execution_mode,
                ), user_id=1
            )
            graph = handler.get_flow(116)
            assert graph is not None
            if delta_read:
                graph.flow_settings.source_registration_id = registration_id
            graph.add_sharepoint_reader(
                NodeSharePointReader(
                    flow_id=116,
                    node_id=1,
                    user_id=1,
                    connection_id=saved_connection,
                    drive_id=None if extension == "list" else "test-drive",
                    file_path=None if extension == "list" else filename,
                    list_source={
                        "site_id": "example.sharepoint.com,site,web", "list_id": "list-id",
                        "columns": ["sku", "quantity"], "odata_filter": "fields/quantity gt 0",
                    } if extension == "list" else None,
                    sheet_name=sheet_name,
                    csv_delimiter=";" if extension == "csv" else ",",
                    delta_generation=0 if delta_read else None,
                )
            )
            response = TestClient(app).get("/node", params={"flow_id": 116, "node_id": 1})
            assert response.status_code == 200, response.text
            node_data = response.json()
            preview = node_data["main_output"]
            assert preview["data"] == []
            if excel_preview:
                assert [column["name"] for column in preview["table_schema"]] == ["sku", "quantity"]
                assert not node_data["prediction_warning"]
                assert downloads == ["preview-token", "worksheets", "header-range"]
                downloads.clear()
            elif glob_read or extension in {"xls", "xlsx", "list"}:
                assert preview["columns"] == []
                assert node_data["prediction_warning"] == "SharePoint schema unavailable until run."
                assert not downloads
            elif extension in {"csv", "parquet", "json"}:
                assert [(column["name"], column["data_type"]) for column in preview["table_schema"]] == (
                    [] if ignores_tail or ignores_range else [("sku", "String"), ("quantity", "Int64")]
                )
                if ignores_tail or ignores_range:
                    assert node_data["prediction_warning"] == "SharePoint schema unavailable until run."
                    from flowfile_core.flowfile.flow_graph.connections import add_connection
                    from flowfile_core.schemas.input_schema import NodeConnection, NodeSort

                    graph.add_sort(NodeSort(flow_id=116, node_id=2, sort_input=[]))
                    add_connection(graph, NodeConnection.create_from_simple_input(1, 2))
                    sort_node = graph.get_node(2)
                    assert sort_node is not None
                    settings_data = sort_node.get_node_data(flow_id=116, include_output=False)
                    assert settings_data.prediction_warning == "SharePoint schema unavailable until run."
                    assert settings_data.main_input is not None
                    assert settings_data.main_input.columns == []
                assert downloads == (["range"] if extension != "parquet" else ["tail"])
                downloads.clear()
            if extension == "list":
                from flowfile_core.flowfile.flow_graph.connections import add_connection
                from flowfile_core.flowfile.share.encoder import decode_share_hash
                from flowfile_core.schemas.input_schema import NodeConnection

                graph.add_sharepoint_writer(NodeSharePointWriter(
                    flow_id=116, node_id=3, depending_on_id=1, user_id=1, connection_id=saved_connection,
                    list_target={"site_id": "example.sharepoint.com,site,web", "list_id": "list-id",
                                 "column_mapping": {"sku": "Title"}} if list_write else None,
                    drive_id=None if list_write else "test-drive",
                    file_path=None if list_write else "Reports/output.csv",
                    format=None if list_write else "csv", conflict_mode="error" if list_write else "overwrite",
                ))
                add_connection(graph, NodeConnection.create_from_simple_input(1, 3))
                response = TestClient(app).get("/editor/share_link", params={"flow_id": 116})
                assert response.status_code == 200
                envelope = decode_share_hash(response.json()["url"])
                assert envelope is not None
                shared_nodes = envelope["flow"]["nodes"]
                assert {node["type"] for node in shared_nodes} == {
                    "sharepoint_reader__unsupported", "sharepoint_writer__unsupported",
                }
                assert "$ffsec$" not in str(envelope) and "fixture-client-secret" not in str(envelope)
                shared_writer = next(node for node in shared_nodes if node["id"] == 3)
                assert shared_writer["type"] == "sharepoint_writer__unsupported"
                assert shared_writer["input_ids"] == [1]
                assert shared_writer["setting_input"]["inputs"] == 1
                assert shared_writer["setting_input"]["outputs"] == 0
                for private in ("connection_id", "test-drive", "Reports/output.csv", "fixture-client-secret"):
                    assert private not in str(shared_writer)
                response = TestClient(app).get("/node", params={"flow_id": 116, "node_id": 3})
                assert response.status_code == 200, response.text
                assert not downloads
            if delta_list:
                from flowfile_core.flowfile.flow_graph.connections import add_connection
                from flowfile_core.schemas.input_schema import NodeConnection

                graph.add_sharepoint_writer(NodeSharePointWriter(
                    flow_id=116, node_id=3, depending_on_id=1, user_id=1, connection_id=saved_connection,
                    list_target={"site_id": "example.sharepoint.com,site,web", "list_id": "list-id",
                                 "column_mapping": {"sku": "Title"}},
                ))
                add_connection(graph, NodeConnection.create_from_simple_input(1, 3))
            with status_dict_lock:
                previous_jobs = set(status_dict)
            result = graph.run_graph()
            with status_dict_lock:
                new_jobs = set(status_dict) - previous_jobs
            assert new_jobs
            for job_id in new_jobs:
                status = httpx.get(
                    f"http://127.0.0.1:{port}/v1/jobs/{job_id}",
                    headers={
                        INTERNAL_TOKEN_HEADER: get_internal_token(),
                        PROTOCOL_VERSION_HEADER: str(PROTOCOL_VERSION),
                    },
                )
                assert status.status_code == 200
                assert status.json()["status"] == "Completed"
                for sensitive in ("$ffsec$", "client_secret_encrypted", "fixture-client-secret", "fixture-token"):
                    assert sensitive not in status.text
            assert result is not None
            node = graph.get_node(1)
            assert node is not None
            data = node.get_resulting_data()
            assert data is not None and data.data_frame is not None
            assert data.data_frame.collect().to_dicts() == expected
            if glob_read:
                assert downloads == [
                    "listing",
                    "listing",
                    "https://download.example.test/sales-a.csv",
                    "https://download.example.test/sales-b.csv",
                ]
            elif extension == "list":
                assert downloads == (["items", "items", {"fields": {"Title": "A"}}, {"fields": {"Title": "B"}}]
                                     if list_write else ["items", "items", "upload"])
                assert writer_plan_checked
                flow_log = Path(graph.flow_logger.get_log_filepath()).read_text(encoding="utf-8")
                assert flow_log
                for sensitive in ("$ffsec$", "fixture-client-secret", "fixture-token"):
                    assert sensitive not in caplog.text and sensitive not in flow_log, (
                        "Credential material reached flow or worker logs"
                    )
            else:
                assert len(downloads) == 1
            if extension in {"csv", "parquet"} and not (ignores_range or ignores_tail or glob_read or delta_read):
                from flowfile_core.flowfile.execution.protocol import JobHandle

                running_job = JobHandle(graph.execution_context.backend.id, "in-flight-read")
                cancelled = []
                with monkeypatch.context() as patch:
                    patch.setattr(node, "_submit_in_flight", running_job)
                    patch.setattr(
                        type(graph.execution_context.backend), "cancel", lambda self, job: cancelled.append(job)
                    )
                    assert [column.name for column in node.get_predicted_schema(force=True)] == ["sku", "quantity"]
                    node.cancel()
                    assert cancelled == [running_job]
                assert data.data_frame.collect().to_dicts() == expected
            if delta_read:
                assert delta_tokens == ["latest"]
                assert all(step.success for step in graph.run_graph().node_step_result)
                empty = node.get_resulting_data().data_frame.collect()
                assert empty.is_empty()
                assert empty.schema.names() == ["sku", "quantity"]
                assert delta_tokens == ["latest", "next-1"] and len(downloads) == 1
                delta_changes.append({"id": "sales-item"})
                content = b"sku;quantity\nC;7\n"
                if delta_list:
                    assert delta_list_posts == [{"fields": {"Title": row["sku"]}} for row in expected]
                    assert all(step.success for step in graph.run_graph().node_step_result)
                    assert node.get_resulting_data().data_frame.collect().to_dicts() == [{"sku": "C", "quantity": 7}]
                    assert delta_list_posts == [{"fields": {"Title": row["sku"]}} for row in expected] + [
                        {"fields": {"Title": "C"}},
                    ]
                    return
                graph.run_graph(commit_sources=False)
                assert node.get_resulting_data().data_frame.collect().to_dicts() == [{"sku": "C", "quantity": 7}]
                execute_stages = graph._execute_stages

                def cancel_before_checkpoint(*args, **kwargs):
                    failed = execute_stages(*args, **kwargs)
                    assert not failed
                    graph.flow_settings.is_canceled = True
                    return failed

                with monkeypatch.context() as patch:
                    patch.setattr(graph, "_execute_stages", cancel_before_checkpoint)
                    graph.run_graph()
                from flowfile_core.flowfile.flow_graph.connections import add_connection
                from flowfile_core.schemas.input_schema import NodeConnection

                graph.add_sharepoint_writer(NodeSharePointWriter(
                    flow_id=116, node_id=3, depending_on_id=1, user_id=1, connection_id=saved_connection,
                    drive_id="test-drive", file_path="delta-output.csv", format="csv", conflict_mode="overwrite",
                ))
                add_connection(graph, NodeConnection.create_from_simple_input(1, 3))
                graph.run_graph()
                assert delta_sink_statuses == [403]
                graph.run_graph()
                assert node.get_resulting_data().data_frame.collect().to_dicts() == [{"sku": "C", "quantity": 7}]
                assert delta_sink_statuses == [403, 201]
                assert delta_tokens == ["latest", "next-1"] + ["next-2"] * 4
                assert len(downloads) == 5
                delta_changes[:] = [{"id": "sales-item", "deleted": {}}]
                delta_file_exists = False
                graph.run_graph()
                assert delta_tokens[-1] == "next-6" and len(downloads) == 5
                assert node.get_resulting_data().data_frame.collect().schema == empty.schema
                assert node.get_resulting_data().data_frame.collect().is_empty()
                delta_file_exists = True
                graph.add_sharepoint_reader(node.setting_input.model_copy(update={"delta_generation": 1}))
                graph.run_graph()
                assert delta_tokens[-1] == "latest"
            if extension == "csv" and not ignores_range and not glob_read and not delta_read:
                import flowfile_frame as ff
                from flowfile_frame import _identity

                monkeypatch.setattr(_identity, "current_user_id", lambda: 1)
                frame = ff.read_sharepoint(
                    saved_connection,
                    drive_id="test-drive",
                    file_path=filename,
                    csv_delimiter=";",
                    flow_graph=graph,
                )
                assert frame.collect().to_dicts() == expected
            details = graph.get_node_data(1, include_example=False)
            assert details.setting_input.sheet_name == sheet_name
            if extension == "xlsx":
                assert details.sharepoint_worksheet_names == ["Sheet", "Sales"]
                workbook.remove(sheet)
                changed = BytesIO()
                workbook.save(changed)
                content = changed.getvalue()
                graph.run_graph()
                refreshed = graph.get_node_data(1, include_example=False)
                assert len(downloads) == 2
                assert refreshed.setting_input.sheet_name == "Sales"
                assert refreshed.sharepoint_worksheet_names is None

            elif extension == "xls":
                assert details.sharepoint_worksheet_names == ["Sheet", "Sales"]
            else:
                assert details.sharepoint_worksheet_names is None
        finally:
            server.should_exit = True
            thread.join(timeout=5)


@pytest.mark.parametrize("upstream_status", [200, 401])
def test_checks_saved_credentials_and_drive(saved_connection, monkeypatch, upstream_status):
    from flowfile_core.routes import sharepoint_connections

    def handle(request):
        if request.method == "POST":
            assert request.url.host == "login.microsoftonline.com"
            assert b"client_secret=fixture-client-secret" in request.content
            return httpx.Response(200, json={"access_token": "fixture-token"})
        assert str(request.url) == "https://graph.microsoft.com/v1.0/drives/test-drive?$select=id"
        assert request.headers["Authorization"] == "Bearer fixture-token"
        return httpx.Response(upstream_status, json={"id": "test-drive", "detail": "fixture-client-secret"})

    monkeypatch.setattr(
        sharepoint_connections,
        "_http_client",
        lambda: httpx.Client(transport=httpx.MockTransport(handle)),
        raising=False,
    )
    response = TestClient(app).post(
        "/sharepoint_connections/test", json={"connection_id": saved_connection, "drive_id": "test-drive"}
    )
    assert response.status_code == (200 if upstream_status == 200 else 422), response.text
    assert "fixture-client-secret" not in response.text
    if upstream_status == 200:
        assert response.json() == {"success": True}


@pytest.mark.parametrize("filename", ["Sales.xlsx", "Sales.XLS"])
def test_sharepoint_worksheet_refresh_only_requests_file_metadata(saved_connection, monkeypatch, filename):
    from flowfile_core.routes import sharepoint_connections

    def upstream(request):
        if request.url.host == "login.microsoftonline.com":
            return httpx.Response(200, json={"access_token": "test-token"})
        assert request.url.host == "graph.microsoft.com"
        assert request.headers["Authorization"] == "Bearer test-token"
        assert request.url.path == f"/v1.0/drives/test-drive/root:/Reports/{filename}"
        assert request.url.params["$select"] == "name"
        return httpx.Response(200, json={"name": filename})

    monkeypatch.setattr(
        sharepoint_connections, "_http_client", lambda: httpx.Client(transport=httpx.MockTransport(upstream))
    )
    client = TestClient(app)
    url = f"/sharepoint_connections/{saved_connection}/drives/test-drive/worksheets"
    params = {"file_path": f"Reports/{filename}"}
    response = client.get(url, params=params)
    assert response.status_code == 200, response.text
    assert response.json() == {"file_name": filename, "file_path": f"Reports/{filename}"}
    monkeypatch.setitem(app.dependency_overrides, get_current_active_user, lambda: SimpleNamespace(id=2))
    assert client.get(url, params=params).status_code == 404
