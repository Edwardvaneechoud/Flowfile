from importlib import import_module
from types import SimpleNamespace

import httpx
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
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
        migration = import_module("flowfile_core.alembic.versions.043_sharepoint_connections")
        with engine.begin() as connection, Operations.context(MigrationContext.configure(connection)):
            migration.upgrade()
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
            "default_site_url": "https://example.sharepoint.com/sites/Finance",
        }
    ]
    monkeypatch.setitem(app.dependency_overrides, get_current_active_user, lambda: SimpleNamespace(id=2))
    response = client.get("/sharepoint_connections")
    assert response.status_code == 200
    assert response.json() == []


def test_admin_default_site_is_a_deployment_hint(saved_connection, monkeypatch):
    monkeypatch.setenv("FLOWFILE_SHAREPOINT_DEFAULT_SITE_URL", "https://example.sharepoint.com/sites/Finance")
    client = TestClient(app)
    response = client.get("/sharepoint_connections/browse_defaults")
    assert response.status_code == 200
    assert response.json() == {"site_url": "https://example.sharepoint.com/sites/Finance"}
    monkeypatch.delenv("FLOWFILE_SHAREPOINT_DEFAULT_SITE_URL")
    assert client.get("/sharepoint_connections/browse_defaults").json() == {"site_url": None}


def test_list_libraries_for_explicit_site(saved_connection, monkeypatch):
    from flowfile_core.routes import sharepoint_connections

    def upstream(req):
        if req.method == "POST":
            return httpx.Response(200, json={"access_token": "fixture-token"})
        assert req.url.host == "graph.microsoft.com"
        assert req.headers["Authorization"] == "Bearer fixture-token"
        if req.url.path == "/v1.0/sites/example.sharepoint.com:/sites/Finance":
            return httpx.Response(200, json={"id": "example.sharepoint.com,site-id,web-id"})
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
    monkeypatch.setitem(app.dependency_overrides, get_current_active_user, lambda: SimpleNamespace(id=2))
    assert client.get(url, params=params).status_code == 404


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


@pytest.mark.parametrize("extension", ["csv", "csv-no-range", "xlsx", "parquet", "parquet-no-range", "json", "json-no-range"])
def test_sharepoint_graph_reads_file_over_worker_http(saved_connection, monkeypatch, extension):
    import socket
    import threading
    import time
    from contextlib import contextmanager
    from io import BytesIO

    import uvicorn

    from flowfile_core.configs import settings
    from flowfile_core.flowfile.execution import registry
    from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
    from flowfile_core.flowfile.flow_graph.builders import connectors
    from flowfile_core.flowfile.handler import FlowfileHandler
    from flowfile_core.schemas.input_schema import NodeSharePointReader
    from flowfile_core.schemas.schemas import FlowSettings
    from flowfile_worker import spawner
    from flowfile_worker.external_sources import sharepoint_source
    from flowfile_worker.main import app as worker_app

    ignores_range = extension in {"csv-no-range", "json-no-range"}
    if ignores_range:
        extension = extension.removesuffix("-no-range")
    ignores_tail = extension == "parquet-no-range"
    if ignores_tail:
        extension = "parquet"
    filename = f"sales.{extension}"
    sheet_name = "Sales" if extension == "xlsx" else None
    expected = [{"sku": "A", "quantity": 2}, {"sku": "B", "quantity": 5}]
    content = b"sku;quantity\nA;2\nB;5\n"
    if extension == "parquet":
        import polars as pl

        output = BytesIO()
        pl.DataFrame(expected).write_parquet(output)
        content = output.getvalue()
    elif extension == "json":
        content = b'[{"sku":"A","quantity":2},{"sku":"B","quantity":5}]'
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

    def microsoft(request):
        if request.url.host == "login.microsoftonline.com":
            return httpx.Response(200, json={"access_token": "fixture-token"})
        if request.url.host == "graph.microsoft.com":
            assert "Range" not in request.headers
            assert request.headers["Authorization"] == "Bearer fixture-token"
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
        return httpx.Response(200, content=content)

    monkeypatch.setattr(
        sharepoint_source, "_http_client", lambda: httpx.Client(transport=httpx.MockTransport(microsoft))
    )
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
            monkeypatch.setitem(registry._instances, registry.WORKER_BACKEND_ID, backend)
            handler = FlowfileHandler()
            handler.register_flow(
                FlowSettings(flow_id=116, name="SharePoint HTTP", path=".", execution_location="remote")
            )
            graph = handler.get_flow(116)
            assert graph is not None
            graph.add_sharepoint_reader(
                NodeSharePointReader(
                    flow_id=116,
                    node_id=1,
                    user_id=1,
                    connection_id=saved_connection,
                    drive_id="test-drive",
                    file_path=filename,
                    sheet_name=sheet_name,
                    csv_delimiter=";" if extension == "csv" else ",",
                )
            )
            if extension in {"csv", "parquet", "json"}:
                node_data = graph.get_node_data(1, include_example=False)
                preview = node_data.main_output
                assert preview.columns == ([] if ignores_tail or ignores_range else ["sku", "quantity"])
                if ignores_tail or ignores_range:
                    assert node_data.prediction_warning == "SharePoint schema unavailable until run."
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
                assert preview.data == []
                assert downloads == (["range"] if extension != "parquet" else ["tail"])
                downloads.clear()
            else:
                preview = graph.get_node_data(1, include_example=False).main_output
                assert preview is not None
                assert preview.columns == []
                assert preview.data == []
                assert downloads == []
            result = graph.run_graph()
            assert result is not None
            node = graph.get_node(1)
            assert node is not None
            data = node.get_resulting_data()
            assert data is not None and data.data_frame is not None
            assert data.data_frame.collect().to_dicts() == expected
            assert len(downloads) == 1
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


def test_sharepoint_worksheet_refresh_only_requests_file_metadata(saved_connection, monkeypatch):
    from flowfile_core.routes import sharepoint_connections

    tags = ['"v1"']

    def upstream(request):
        if request.url.host == "login.microsoftonline.com":
            return httpx.Response(200, json={"access_token": "test-token"})
        assert request.url.host == "graph.microsoft.com"
        assert request.headers["Authorization"] == "Bearer test-token"
        assert request.url.path == "/v1.0/drives/test-drive/root:/Reports/Sales.xlsx"
        assert request.url.params["$select"] == "id,name,cTag"
        return httpx.Response(200, json={"id": "item", "name": "Sales.xlsx", "cTag": tags[0]})

    monkeypatch.setattr(
        sharepoint_connections, "_http_client", lambda: httpx.Client(transport=httpx.MockTransport(upstream))
    )
    client = TestClient(app)
    url = f"/sharepoint_connections/{saved_connection}/drives/test-drive/worksheets"
    params = {"file_path": "Reports/Sales.xlsx"}
    for tag in ['"v1"', '"v1"', '"v2"', None]:
        tags[0] = tag
        response = client.get(url, params=params)
        assert response.status_code == 200, response.text
        assert response.json() == {
            "worksheet_names": None,
            "version": ["item", tag] if tag else None,
            "file_name": "Sales.xlsx",
            "file_path": "Reports/Sales.xlsx",
        }
    monkeypatch.setitem(app.dependency_overrides, get_current_active_user, lambda: SimpleNamespace(id=2))
    assert client.get(url, params=params).status_code == 404
