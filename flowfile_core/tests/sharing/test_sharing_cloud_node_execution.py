"""Cloud storage reader/writer nodes in multi-user (docker) mode.

The server's own credentials and disk are refused before anything consults them; saved connections keep working.
"""

import os
import re
import uuid

import boto3
import polars as pl
import pytest
from fastapi import HTTPException

from flowfile_core.auth import sharing
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile import flow_graph
from flowfile_core.flowfile.flow_data_engine.cloud_storage_reader import CloudStorageReader
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_graph import (
    _cloud_change_read_target,
    add_connection,
    get_cloud_connection_settings,
)
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.routes import storage_browser
from flowfile_core.schemas import input_schema, schemas
from shared.cloud_storage.utils import validate_cloud_resource_path

GATE = "Select a cloud storage connection; server credentials are not available in multi-user mode."
ROWS = [{"id": 1, "category": "A"}, {"id": 2, "category": "B"}]
S3_PATH = "s3://flowfile-test/docker_gate/table"
CONNECTION = "docker_gate_minio"
MINIO_ENDPOINT = "http://localhost:9000"


def _minio_available() -> bool:
    try:
        from test_utils.s3.fixtures import is_docker_available, wait_for_minio

        return is_docker_available() and bool(wait_for_minio(max_retries=2, interval=0.5))
    except Exception:
        return False


requires_minio = pytest.mark.skipif(not _minio_available(), reason="MinIO is not running (poetry run start_minio)")


@pytest.fixture
def hermetic_aws(tmp_path, monkeypatch):
    """Sentinel server credentials aimed at a closed port: a leak can never reach real AWS or MinIO."""
    for key in list(os.environ):
        if key.startswith("AWS_"):
            monkeypatch.delenv(key)
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "no-credentials"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "no-config"))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIASERVERAMBIENT")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "server-ambient-secret")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://127.0.0.1:9")
    monkeypatch.setenv("AWS_ALLOW_HTTP", "true")


@pytest.fixture
def ambient_probe(hermetic_aws, monkeypatch):
    """Record every boto3 session and every ambient connection core builds."""
    calls = []
    real_session = boto3.Session
    real_connection = flow_graph.FullCloudStorageConnection

    class _RecordingSession(real_session):
        def __init__(self, *args, **kwargs):
            calls.append(("boto3.Session", kwargs))
            super().__init__(*args, **kwargs)

    def _recording_connection(*args, **kwargs):
        calls.append(("ambient connection", kwargs))
        return real_connection(*args, **kwargs)

    monkeypatch.setattr(boto3, "Session", _RecordingSession)
    monkeypatch.setattr(flow_graph, "FullCloudStorageConnection", _recording_connection)
    return calls


def _graph(execution_location: str = "local", flow_id: int = 1):
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="docker_gate",
            path=".",
            execution_mode="Development",
            execution_location=execution_location,
        )
    )
    return handler.get_flow(flow_id)


def _writer_graph(user_id: int, execution_location: str = "local", **settings):
    graph = _graph(execution_location)
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(flow_id=1, node_id=1, raw_data_format=input_schema.RawData.from_pylist(ROWS))
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=2, node_type="cloud_storage_writer"))
    graph.add_cloud_storage_writer(
        input_schema.NodeCloudStorageWriter.model_validate(
            {
                "flow_id": 1,
                "node_id": 2,
                "user_id": user_id,
                "depending_on_id": 1,
                "cloud_storage_settings": {"resource_path": S3_PATH, **settings},
            }
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2))
    return graph


def _read_settings(**settings) -> input_schema.CloudStorageReadSettings:
    return input_schema.CloudStorageReadSettings(**{"resource_path": S3_PATH, **settings})


def _reader_graph(user_id: int, **settings):
    graph = _graph()
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="cloud_storage_reader"))
    graph.add_cloud_storage_reader(
        input_schema.NodeCloudStorageReader(
            flow_id=1, node_id=1, user_id=user_id, cloud_storage_settings=_read_settings(**settings)
        )
    )
    return graph


def _error_of(run_info, node_id: int) -> str:
    step = next(step for step in run_info.node_step_result if step.node_id == node_id)
    assert not step.success, f"node {node_id} should have failed"
    return step.error


def _share(client, resource_id: int, group_id: int):
    response = client.post(
        "/shares",
        json={
            "resource_type": "cloud_connection",
            "resource_id": resource_id,
            "group_id": group_id,
            "permission": "use",
        },
    )
    assert response.status_code == 200, response.text


@pytest.fixture
def team(users, group_factory):
    """alice (owner) shares with bob (member); carol is not in the group."""
    return group_factory("cloud-gate-team", users["admin"].id, {users["alice"].id: "owner", users["bob"].id: "member"})


@pytest.fixture
def alice_minio(users, client_for) -> int:
    """An access-key MinIO connection owned by alice."""
    body = {
        "connection_name": CONNECTION,
        "storage_type": "s3",
        "auth_method": "access_key",
        "aws_region": "us-east-1",
        "aws_access_key_id": "minioadmin",
        "aws_secret_access_key": "minioadmin",
        "aws_allow_unsafe_html": True,
        "endpoint_url": MINIO_ENDPOINT,
    }
    response = client_for("alice").post("/cloud_connections/cloud_connection", json=body)
    assert response.status_code == 200, response.text
    with get_db_context() as db:
        return (
            db.query(db_models.CloudStorageConnection)
            .filter_by(connection_name=CONNECTION, user_id=users["alice"].id)
            .one()
            .id
        )


class TestNoConnectionIsRefused:
    """A node with no saved connection never falls back to the server's credentials."""

    def test_gate_is_shared_with_the_storage_browser(self):
        assert storage_browser.ambient_credentials_allowed is sharing.ambient_credentials_allowed
        assert sharing.ambient_credentials_allowed() is False

    @pytest.mark.parametrize("auth_mode", ["auto", "env_vars", "aws-cli"])
    @pytest.mark.parametrize("path", [S3_PATH, "az://container/table", "gs://bucket/table"])
    def test_resolution_refuses_before_any_lookup(self, users, ambient_probe, auth_mode, path):
        with pytest.raises(ValueError, match=re.escape(GATE)):
            get_cloud_connection_settings(None, users["bob"].id, auth_mode, resource_path=path)
        assert ambient_probe == []

    def test_a_named_connection_that_does_not_resolve_is_still_not_found(self, users, ambient_probe):
        with pytest.raises(HTTPException) as exc_info:
            get_cloud_connection_settings("no-such-connection", users["bob"].id, "auto", resource_path=S3_PATH)
        assert exc_info.value.status_code == 400
        assert ambient_probe == []

    @pytest.mark.parametrize("auth_mode", ["auto", "aws-cli"])
    def test_reader_run_fails_with_the_gate(self, users, ambient_probe, auth_mode):
        run_info = _reader_graph(users["bob"].id, auth_mode=auth_mode).run_graph()

        assert GATE in _error_of(run_info, 1)
        assert ambient_probe == []

    def test_reader_schema_prediction_does_not_consult_server_credentials(self, users, ambient_probe):
        graph = _reader_graph(users["bob"].id, auth_mode="aws-cli")

        assert not graph.get_node(1).get_predicted_schema()
        assert ambient_probe == []

    def test_writer_run_fails_with_the_gate(self, users, ambient_probe):
        graph = _writer_graph(users["bob"].id, auth_mode="aws-cli", file_format="parquet")

        assert GATE in _error_of(graph.run_graph(), 2)
        assert ambient_probe == []

    @pytest.mark.parametrize("location", ["local", "remote"])
    def test_writer_refuses_before_any_worker_offload(self, users, ambient_probe, location):
        # Called directly: a remote run in docker mode would need the internal token to offload the input.
        graph = _writer_graph(users["bob"].id, location, auth_mode="aws-cli", file_format="parquet")

        with pytest.raises(ValueError, match=re.escape(GATE)):
            graph.get_node(2)._function(FlowDataEngine(pl.DataFrame(ROWS)))
        assert ambient_probe == []

    @pytest.mark.parametrize(
        "settings",
        [
            {"file_format": "delta", "write_mode": "upsert", "merge_keys": ["id"]},
            {"file_format": "delta", "write_mode": "append", "track_changes": True},
            {"file_format": "delta", "write_mode": "append", "partition_by": ["category"]},
        ],
        ids=["merge", "track_changes", "partitioned_append"],
    )
    def test_delta_writer_modes_fail_with_the_gate(self, users, ambient_probe, settings):
        graph = _writer_graph(users["bob"].id, auth_mode="aws-cli", **settings)

        assert GATE in _error_of(graph.run_graph(), 2)
        assert ambient_probe == []


class TestChangeReadsAreRefused:
    """Change-feed reads resolve their connection in three places; each hits the gate first."""

    CDC = {"file_format": "delta", "cdc_mode": "since_version", "cdc_from_version": 0, "auth_mode": "aws-cli"}

    def test_change_read_target(self, users, ambient_probe):
        with pytest.raises(ValueError, match=re.escape(GATE)):
            _cloud_change_read_target(_read_settings(**self.CDC), users["bob"].id)
        assert ambient_probe == []

    def test_change_read_run(self, users, ambient_probe):
        run_info = _reader_graph(users["bob"].id, **self.CDC).run_graph()

        assert GATE in _error_of(run_info, 1)
        assert ambient_probe == []

    def test_change_read_schema_callback(self, users, ambient_probe):
        graph = _reader_graph(users["bob"].id, **self.CDC)

        assert not graph.get_node(1).get_predicted_schema()
        assert ambient_probe == []

    def test_freshness_probe(self, users, ambient_probe):
        graph = _reader_graph(users["bob"].id, **self.CDC)

        with pytest.raises(ValueError, match=re.escape(GATE)):
            graph._cloud_change_read_fingerprint(graph.get_node(1), {})
        graph._refresh_catalog_reader_freshness()  # fails open: the run surfaces the real error
        assert ambient_probe == []


class TestLocalPathsAreRefused:
    """In multi-user mode a cloud node may not read or write the server's own disk, even with a connection."""

    LOCAL = "is a local path, which this server does not allow"

    def test_helper_refuses_an_absolute_local_path_only_when_asked(self, tmp_path):
        path = str(tmp_path / "table")
        assert validate_cloud_resource_path(path, role="writer") == path
        with pytest.raises(ValueError, match=re.escape(f"Cloud storage path '{path}' {self.LOCAL}")):
            validate_cloud_resource_path(path, role="writer", allow_local_paths=False)

    @pytest.mark.parametrize("allow_local_paths", [True, False])
    @pytest.mark.parametrize("path", ["s3://b/k", "az://c/p", "abfss://c@acct.dfs.core.windows.net/p", "gs://b/k"])
    def test_helper_passes_cloud_uris_in_both_modes(self, path, allow_local_paths):
        assert validate_cloud_resource_path(path, role="reader", allow_local_paths=allow_local_paths) == path

    @pytest.mark.parametrize("allow_local_paths", [True, False])
    @pytest.mark.parametrize("path", ["file:///etc/passwd", "https://example.com/x.parquet", "relative/table"])
    def test_helper_refuses_non_uri_paths_in_both_modes(self, path, allow_local_paths):
        with pytest.raises(ValueError, match="is not a URI"):
            validate_cloud_resource_path(path, role="reader", allow_local_paths=allow_local_paths)

    def test_writer_with_a_connection_cannot_write_the_server_disk(self, users, alice_minio, hermetic_aws, tmp_path):
        target = tmp_path / "table"
        graph = _writer_graph(
            users["alice"].id, connection_name=CONNECTION, resource_path=str(target), file_format="delta"
        )

        error = _error_of(graph.run_graph(), 2)

        assert self.LOCAL in error
        assert not target.exists()

    def test_reader_with_a_connection_cannot_read_the_server_disk(self, users, alice_minio, hermetic_aws, tmp_path):
        source = tmp_path / "secret.parquet"
        pl.DataFrame(ROWS).write_parquet(source)
        graph = _reader_graph(users["alice"].id, connection_name=CONNECTION, resource_path=str(source))

        assert self.LOCAL in _error_of(graph.run_graph(), 1)
        assert not graph.get_node(1).get_predicted_schema()

    @pytest.mark.parametrize("path", ["file:///etc/hosts", "https://example.com/data.parquet"])
    def test_reader_refuses_file_and_web_uris(self, users, alice_minio, hermetic_aws, path):
        graph = _reader_graph(users["alice"].id, connection_name=CONNECTION, resource_path=path)

        assert f"Cloud storage path '{path}' is not a URI" in _error_of(graph.run_graph(), 1)


class TestSavedConnectionsStillWork:
    """Owned and group-granted connections resolve with the owner's credentials, never the server's."""

    def test_grantee_resolves_the_owners_connection(self, users, client_for, alice_minio, team, ambient_probe):
        _share(client_for("alice"), alice_minio, team)

        connection = get_cloud_connection_settings(CONNECTION, users["bob"].id, "aws-cli", resource_path=S3_PATH)

        assert (connection.connection_name, connection.auth_method) == (CONNECTION, "access_key")
        options = CloudStorageReader.get_storage_options(connection)
        assert options["aws_access_key_id"] == "minioadmin"
        assert options["endpoint_url"] == MINIO_ENDPOINT
        assert ambient_probe == []

    def test_non_grantee_does_not_resolve_it(self, users, alice_minio, ambient_probe):
        with pytest.raises(HTTPException) as exc_info:
            get_cloud_connection_settings(CONNECTION, users["carol"].id, "auto", resource_path=S3_PATH)
        assert exc_info.value.status_code == 400
        assert ambient_probe == []

    @requires_minio
    def test_grantee_writes_and_reads_minio(self, users, client_for, alice_minio, team, hermetic_aws):
        from test_utils.s3.fixtures import get_minio_client

        _share(client_for("alice"), alice_minio, team)
        prefix = f"docker_gate_{uuid.uuid4().hex[:12]}"
        path = f"s3://flowfile-test/{prefix}/out.parquet"
        try:
            writer = _writer_graph(users["bob"].id, connection_name=CONNECTION, resource_path=path)
            write_run = writer.run_graph()
            assert write_run.success, [step.error for step in write_run.node_step_result]

            reader = _reader_graph(users["bob"].id, connection_name=CONNECTION, resource_path=path)
            read_run = reader.run_graph()
            assert read_run.success, [step.error for step in read_run.node_step_result]
            result = reader.get_node(1).get_resulting_data().data_frame
            assert result.lazy().collect().sort("id").to_dicts() == ROWS
        finally:
            client = get_minio_client()
            listed = client.list_objects_v2(Bucket="flowfile-test", Prefix=f"{prefix}/")
            for obj in listed.get("Contents", []):
                client.delete_object(Bucket="flowfile-test", Key=obj["Key"])


SERVER_IDENTITY = "authenticates with this server's own credentials"
CAPTURE_ENDPOINT = "http://127.0.0.1:9"


def _server_identity_body(name: str, auth_method: str) -> dict:
    storage_type = "adls" if auth_method == "managed_identity" else "s3"
    return {
        "connection_name": name,
        "storage_type": storage_type,
        "auth_method": auth_method,
        "aws_profile": "server-admin" if auth_method == "aws-cli" else None,
        "aws_role_arn": "arn:aws:iam::123456789012:role/server" if auth_method == "iam_role" else None,
        "aws_allow_unsafe_html": True,
        "endpoint_url": CAPTURE_ENDPOINT,
    }


def _own_rows(user_id: int, name: str) -> int:
    with get_db_context() as db:
        return db.query(db_models.CloudStorageConnection).filter_by(connection_name=name, user_id=user_id).count()


class TestServerIdentityConnections:
    """A saved aws-cli/env_vars/iam_role/managed_identity connection runs as the server: admin-only in docker."""

    @pytest.mark.parametrize("auth_method", ["aws-cli", "env_vars", "iam_role", "managed_identity"])
    def test_regular_user_cannot_create_one(self, users, client_for, auth_method):
        name = f"srv_{auth_method}"
        response = client_for("alice").post(
            "/cloud_connections/cloud_connection", json=_server_identity_body(name, auth_method)
        )

        assert response.status_code == 422
        assert SERVER_IDENTITY in response.json()["detail"]
        assert _own_rows(users["alice"].id, name) == 0

    def test_regular_user_cannot_switch_a_connection_to_one(self, users, client_for, alice_minio):
        response = client_for("alice").put(
            "/cloud_connections/cloud_connection", json=_server_identity_body(CONNECTION, "aws-cli")
        )

        assert response.status_code == 422
        with get_db_context() as db:
            row = db.get(db_models.CloudStorageConnection, alice_minio)
            assert (row.auth_method, row.endpoint_url) == ("access_key", MINIO_ENDPOINT)

    def test_admin_may_own_one_and_share_it(self, users, client_for, group_factory, ambient_probe):
        admin = client_for("admin")
        response = admin.post("/cloud_connections/cloud_connection", json=_server_identity_body("srv_env", "env_vars"))
        assert response.status_code == 200, response.text
        with get_db_context() as db:
            row_id = db.query(db_models.CloudStorageConnection).filter_by(connection_name="srv_env").one().id
        group = group_factory("srv-team", users["admin"].id, {users["bob"].id: "member"})
        _share(admin, row_id, group)

        for user in ("admin", "bob"):
            connection = get_cloud_connection_settings("srv_env", users[user].id, "auto", resource_path=S3_PATH)
            assert connection.auth_method == "env_vars"

    @pytest.fixture
    def alice_legacy_aws_cli(self, users) -> str:
        """An aws-cli connection a regular user saved before the refusal existed."""
        with get_db_context() as db:
            db.add(
                db_models.CloudStorageConnection(
                    connection_name="legacy_cli",
                    storage_type="s3",
                    auth_method="aws-cli",
                    aws_profile="server-admin",
                    aws_allow_unsafe_html=True,
                    endpoint_url=CAPTURE_ENDPOINT,
                    user_id=users["alice"].id,
                )
            )
            db.commit()
        return "legacy_cli"

    def test_an_existing_row_fails_closed_on_resolution(self, users, alice_legacy_aws_cli, ambient_probe):
        with pytest.raises(ValueError, match=SERVER_IDENTITY):
            get_cloud_connection_settings(alice_legacy_aws_cli, users["alice"].id, "auto", resource_path=S3_PATH)
        assert ambient_probe == []

    def test_an_existing_row_fails_closed_in_nodes(self, users, alice_legacy_aws_cli, ambient_probe):
        writer = _writer_graph(users["alice"].id, connection_name=alice_legacy_aws_cli)
        reader = _reader_graph(users["alice"].id, connection_name=alice_legacy_aws_cli)

        assert SERVER_IDENTITY in _error_of(writer.run_graph(), 2)
        assert SERVER_IDENTITY in _error_of(reader.run_graph(), 1)
        assert ambient_probe == []

    def test_an_existing_row_fails_closed_in_routes(self, users, client_for, alice_legacy_aws_cli, ambient_probe):
        alice = client_for("alice")
        browse = alice.get("/storage_browser/cloud", params={"connection_name": alice_legacy_aws_cli})
        info = alice.post(
            "/cloud_storage/delta/info", json={"connection_name": alice_legacy_aws_cli, "resource_path": S3_PATH}
        )

        for response in (browse, info):
            assert response.status_code == 400, response.text
            assert response.json()["detail"]["error_code"] == "CONNECTION_NOT_ALLOWED"
        assert ambient_probe == []


class TestSingleUserModesKeepAmbientCredentials:
    """Electron and package mode run on the caller's own machine: the ambient fallback stays."""

    @pytest.fixture(params=["electron", "package"], autouse=True)
    def single_user_mode(self, request, monkeypatch):
        monkeypatch.setenv("FLOWFILE_MODE", request.param)

    def test_gate_allows(self):
        assert sharing.ambient_credentials_allowed() is True

    @pytest.mark.parametrize(
        ("auth_mode", "path", "storage_type", "auth_method"),
        [
            ("aws-cli", S3_PATH, "s3", "aws-cli"),
            ("auto", S3_PATH, "s3", "env_vars"),
            ("env_vars", None, "s3", "env_vars"),
            ("aws-cli", "az://container/table", "adls", "env_vars"),
            ("aws-cli", "abfss://container@acct.dfs.core.windows.net/table", "adls", "env_vars"),
            ("auto", "gs://bucket/table", "gcs", "env_vars"),
            ("auto", "/data/local/table", "s3", "env_vars"),
        ],
    )
    def test_ambient_connection_follows_the_path_scheme(self, auth_mode, path, storage_type, auth_method):
        connection = get_cloud_connection_settings(None, 1, auth_mode, resource_path=path)

        assert (connection.storage_type, connection.auth_method) == (storage_type, auth_method)
        assert connection.connection_name is None

    def test_absolute_local_paths_still_work(self, tmp_path):
        target = tmp_path / "table"
        graph = _writer_graph(1, resource_path=str(target), file_format="delta", auth_mode="auto")

        run_info = graph.run_graph()

        assert run_info.success, [step.error for step in run_info.node_step_result]
        assert (target / "_delta_log").is_dir()

    def test_ambient_gcs_is_refused_for_delta_merges_instead_of_sent_s3_options(self):
        graph = _writer_graph(
            1, resource_path="gs://bucket/table", file_format="delta", write_mode="upsert", merge_keys=["id"]
        )

        assert "not supported on Google Cloud Storage" in _error_of(graph.run_graph(), 2)

    def test_ambient_gcs_is_refused_for_change_reads(self):
        settings = _read_settings(
            resource_path="gs://bucket/table", file_format="delta", cdc_mode="since_version", cdc_from_version=0
        )
        with pytest.raises(ValueError, match="not supported on Google Cloud Storage"):
            _cloud_change_read_target(settings, 1)
