"""Cloud reads never put decrypted credentials into a serialized query plan.

Every test captures each plan core serializes and asserts MinIO's root secret is absent while the data still reads.
With the compute protocol flag on, a read of a stored connection names it instead, and the executing process
resolves it.
"""

import io
import json
import os
import subprocess
import sys
import textwrap
import uuid
from pathlib import Path

import polars as pl
import pytest
from pydantic import SecretStr
from sqlalchemy import create_engine

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.storage_backend import resolve_for_namespace
from flowfile_core.configs import settings as core_settings
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import Base, CatalogTable
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_cloud_connection,
    store_cloud_connection,
)
from flowfile_core.flowfile.execution.errors import ComputeUnavailable
from flowfile_core.flowfile.flow_data_engine.cloud_storage_reader import CloudStorageReader
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.cloud_storage_schemas import (
    CloudStorageReadSettings,
    CloudStorageReadSettingsInternal,
    CloudStorageWriteSettings,
    FullCloudStorageConnection,
)
from shared import cloud_credential_provider as cp
from shared.delta_utils import write_delta
from tests.flowfile.conftest import add_test_catalog_writer, add_test_manual_input, catalog_cleanup, create_test_graph
from tests.test_catalog_cloud_virtual import _create_cloud_schema, _ensure_connection, requires_minio

try:
    from test_utils.s3.fixtures import MINIO_ACCESS_KEY, MINIO_ENDPOINT_URL, MINIO_SECRET_KEY, get_minio_client
except ModuleNotFoundError:  # pragma: no cover - import shim for ad-hoc runs
    sys.path.append(os.path.dirname(os.path.abspath("test_utils/s3/fixtures.py")))
    from test_utils.s3.fixtures import MINIO_ACCESS_KEY, MINIO_ENDPOINT_URL, MINIO_SECRET_KEY, get_minio_client

from test_utils.gcs.fixtures import GCS_ENDPOINT_URL, is_gcs_available
from test_utils.s3.aws_profiles import isolate_aws

REPO_ROOT = Path(__file__).resolve().parents[3]
_BUCKET = "flowfile-test"
SECRET = MINIO_SECRET_KEY.encode()
_ROOT_OPTIONS = {
    "aws_region": "us-east-1",
    "endpoint_url": MINIO_ENDPOINT_URL,
    "aws_allow_http": "true",
    "aws_access_key_id": MINIO_ACCESS_KEY,
    "aws_secret_access_key": MINIO_SECRET_KEY,
}
_ROWS = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}]
_SENTINELS = {name: f"SENTINEL-{name}-{uuid.uuid4().hex}" for name in ("aws", "token", "key", "sas", "client", "gcs")}
_CONNECTION_TYPES = {
    "s3-access_key": dict(
        storage_type="s3",
        auth_method="access_key",
        aws_access_key_id="AKIDSENTINEL",
        aws_secret_access_key=SecretStr(_SENTINELS["aws"]),
        aws_session_token=SecretStr(_SENTINELS["token"]),
        aws_region="us-east-1",
    ),
    "s3-iam_role": dict(storage_type="s3", auth_method="iam_role", aws_role_arn="arn:aws:iam::123456789012:role/x"),
    "s3-aws-cli": dict(storage_type="s3", auth_method="aws-cli", aws_profile="analytics"),
    "s3-env_vars": dict(storage_type="s3", auth_method="env_vars"),
    "adls-access_key": dict(
        storage_type="adls",
        auth_method="access_key",
        azure_account_name="acct",
        azure_account_key=SecretStr(_SENTINELS["key"]),
    ),
    "adls-sas_token": dict(
        storage_type="adls",
        auth_method="sas_token",
        azure_account_name="acct",
        azure_sas_token=SecretStr(_SENTINELS["sas"]),
    ),
    "adls-service_principal": dict(
        storage_type="adls",
        auth_method="service_principal",
        azure_tenant_id="tenant",
        azure_client_id="client",
        azure_client_secret=SecretStr(_SENTINELS["client"]),
    ),
    "adls-managed_identity": dict(storage_type="adls", auth_method="managed_identity", azure_account_name="acct"),
    "gcs-service_account": dict(
        storage_type="gcs",
        auth_method="service_account",
        gcs_service_account_key=SecretStr(json.dumps({"private_key": _SENTINELS["gcs"]})),
        gcs_project_id="proj",
    ),
    "gcs-service_account-endpoint": dict(
        storage_type="gcs",
        auth_method="service_account",
        gcs_service_account_key=SecretStr(json.dumps({"private_key": _SENTINELS["gcs"]})),
        gcs_project_id="proj",
        endpoint_url=GCS_ENDPOINT_URL,
    ),
    "gcs-env_vars-endpoint": dict(
        storage_type="gcs", auth_method="env_vars", gcs_project_id="proj", endpoint_url=GCS_ENDPOINT_URL
    ),
}
_PATHS = {
    "s3": "s3://bucket/data",
    "adls": "abfss://container@acct.dfs.core.windows.net/data",
    "gcs": "gs://bucket/data",
}
_SUFFIXES = {"parquet": ".parquet", "csv": ".csv", "json": ".json", "delta": ""}


def _connection(name: str, auth_method: str, **fields) -> FullCloudStorageConnection:
    return FullCloudStorageConnection(
        connection_name=name,
        storage_type="s3",
        auth_method=auth_method,
        aws_region="us-east-1",
        endpoint_url=MINIO_ENDPOINT_URL,
        aws_allow_unsafe_html=True,
        **fields,
    )


@pytest.fixture
def serialized_plans(monkeypatch):
    """Every plan serialized in this process during the test: what core ships to the worker."""
    captured: list[bytes] = []
    original = pl.LazyFrame.serialize

    def _spy(self, file=None, **kwargs):
        blob = original(self, **kwargs)
        captured.append(blob)
        return blob if file is None else original(self, file, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "serialize", _spy)
    return captured


@pytest.fixture
def minio_prefix():
    prefix = f"plan-secrets-{uuid.uuid4().hex[:8]}"
    yield prefix
    client = get_minio_client()
    for obj in client.list_objects_v2(Bucket=_BUCKET, Prefix=prefix).get("Contents", []):
        client.delete_object(Bucket=_BUCKET, Key=obj["Key"])


@pytest.fixture
def stored_connection():
    """Store a MinIO connection under a unique name for user 1; returns a factory, deletes them after."""
    names: list[str] = []

    def _store(auth_method: str, **fields) -> str:
        name = f"plan secrets {auth_method} {uuid.uuid4().hex[:6]}"
        with get_db_context() as db:
            store_cloud_connection(db, _connection(name, auth_method, **fields), user_id=1)
        names.append(name)
        return name

    yield _store
    with get_db_context() as db:
        for name in names:
            delete_cloud_connection(db, name, user_id=1)


@pytest.fixture(params=[False, True], ids=["protocol-off", "protocol-on"])
def protocol(request, monkeypatch) -> bool:
    monkeypatch.setattr(core_settings.FLOWFILE_COMPUTE_PROTOCOL, "value", request.param)
    return request.param


@pytest.fixture
def minio_sts(monkeypatch, tmp_path):
    """Route iam_role's STS AssumeRole to MinIO, with MinIO's root keys as core's ambient credentials.

    Only core gets these, so a remote read can succeed only through the credentials encrypted into the plan.
    """
    isolate_aws(
        monkeypatch,
        tmp_path,
        endpoint=None,
        AWS_ACCESS_KEY_ID=MINIO_ACCESS_KEY,
        AWS_SECRET_ACCESS_KEY=MINIO_SECRET_KEY,
        AWS_ENDPOINT_URL_STS=MINIO_ENDPOINT_URL,
    )


@pytest.fixture
def storage_options_spy(monkeypatch):
    """Records every options dict built from a connection (the real builder runs)."""
    built: list[dict] = []
    original = CloudStorageReader.get_storage_options

    def _spy(connection):
        options = original(connection)
        built.append(options)
        return options

    monkeypatch.setattr(CloudStorageReader, "get_storage_options", staticmethod(_spy))
    return built


def _seed_files(prefix: str) -> dict[str, str]:
    """Parquet, CSV and NDJSON files, a two-file directory of each, and a change-tracked Delta table under *prefix*."""
    frame = pl.DataFrame(_ROWS)
    base = f"s3://{_BUCKET}/{prefix}"
    frame.write_parquet(f"{base}/single.parquet", storage_options=_ROOT_OPTIONS)
    frame.write_parquet(f"{base}/dir/part0.parquet", storage_options=_ROOT_OPTIONS)
    frame.write_parquet(f"{base}/dir/part1.parquet", storage_options=_ROOT_OPTIONS)
    frame.write_csv(f"{base}/data.csv", storage_options=_ROOT_OPTIONS)
    frame.lazy().sink_ndjson(f"{base}/data.json", storage_options=_ROOT_OPTIONS)
    for part in (0, 1):
        frame.write_csv(f"{base}/csv_dir/part{part}.csv", storage_options=_ROOT_OPTIONS)
        frame.lazy().sink_ndjson(f"{base}/json_dir/part{part}.json", storage_options=_ROOT_OPTIONS)
    write_delta(frame.head(2), f"{base}/delta", mode="overwrite", storage_options=_ROOT_OPTIONS, enable_cdf=True)
    write_delta(frame.tail(1), f"{base}/delta", mode="append", storage_options=_ROOT_OPTIONS)
    return {
        "single": f"{base}/single.parquet",
        "dir": f"{base}/dir/",
        "csv": f"{base}/data.csv",
        "csv_dir": f"{base}/csv_dir/",
        "json": f"{base}/data.json",
        "json_dir": f"{base}/json_dir/",
        "delta": f"{base}/delta",
    }


def _add_reader(graph: FlowGraph, node_id: int, connection_name: str, **settings) -> None:
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="cloud_storage_reader")
    )
    graph.add_cloud_storage_reader(
        input_schema.NodeCloudStorageReader(
            flow_id=graph.flow_id,
            node_id=node_id,
            user_id=1,
            cloud_storage_settings=CloudStorageReadSettings(connection_name=connection_name, **settings),
        )
    )


def _add_record_count(graph: FlowGraph, node_id: int, depending_on_id: int) -> None:
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="record_count"))
    graph.add_record_count(
        input_schema.NodeRecordCount(flow_id=graph.flow_id, node_id=node_id, depending_on_id=depending_on_id)
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(depending_on_id, node_id))


def _count(graph: FlowGraph, node_id: int) -> int:
    return graph.get_node(node_id).get_resulting_data().collect()["number_of_records"].item()


def _assert_run_succeeded(run_info) -> None:
    failures = [(step.node_id, step.error) for step in run_info.node_step_result if not step.success]
    assert run_info.success, failures


def _assert_no_secret(plans: list[bytes], *secrets: bytes) -> None:
    leaked = [i for i, plan in enumerate(plans) if any(secret and secret in plan for secret in secrets)]
    assert leaked == [], f"{len(leaked)} of {len(plans)} serialized plans carry a plaintext secret"


def _no_encrypted_provider(plans: list[bytes]) -> None:
    """No plan carries credentials core resolved: no encrypted provider, whatever its ciphertext, and no ``$ffsec$``."""
    carrying = [i for i, plan in enumerate(plans) if b"EncryptedCredentialProvider" in plan or b"$ffsec$" in plan]
    assert carrying == [], f"{len(carrying)} of {len(plans)} serialized plans carry core-resolved credentials"


class TestSecureScanKwargs:
    """The helper every serialized cloud scan goes through; no I/O."""

    def test_credentials_leave_the_options_and_come_back_decrypted(self):
        sentinel = f"SENTINEL-{uuid.uuid4().hex}"
        options = {**_ROOT_OPTIONS, "aws_secret_access_key": sentinel, "aws_session_token": ""}

        kwargs = CloudStorageReader.get_secure_scan_kwargs(options, user_id=1)

        assert kwargs["storage_options"] == {
            "aws_region": "us-east-1",
            "endpoint_url": MINIO_ENDPOINT_URL,
            "aws_allow_http": "true",
        }
        assert kwargs["credential_provider"].encrypted_credentials.startswith("$ffsec$1$1$")
        assert kwargs["credential_provider"]()[0]["aws_secret_access_key"] == sentinel
        plan = pl.scan_parquet(f"s3://{_BUCKET}/never-read.parquet", **kwargs).select("x").serialize()
        assert sentinel.encode() not in plan

    def test_without_an_owner_the_legacy_master_key_format_still_round_trips(self):
        kwargs = CloudStorageReader.get_secure_scan_kwargs({"aws_secret_access_key": "s"}, user_id=None)
        assert not kwargs["credential_provider"].encrypted_credentials.startswith("$ffsec$")
        assert kwargs["credential_provider"]() == ({"aws_secret_access_key": "s"}, None)

    @pytest.mark.parametrize("options", [None, {}, {"aws_region": "us-east-1"}])
    def test_options_without_credentials_get_no_provider(self, options):
        kwargs = CloudStorageReader.get_secure_scan_kwargs(options, user_id=1)
        assert "credential_provider" not in kwargs
        assert kwargs.get("storage_options") == (options or None)


class TestReadPlansNameTheirConnection:
    """With the protocol flag on, a stored connection's read plan holds a reference and nothing to decrypt; no I/O."""

    @pytest.mark.parametrize(
        ("file_format", "scan_mode"),
        [
            ("parquet", "single_file"),
            ("parquet", "directory"),
            ("csv", "single_file"),
            ("csv", "directory"),
            ("json", "single_file"),
            ("delta", "single_file"),
        ],
    )
    @pytest.mark.parametrize("connection_type", list(_CONNECTION_TYPES))
    def test_every_connection_types_read_plan_carries_only_its_reference(
        self, monkeypatch, connection_type, file_format, scan_mode
    ):
        monkeypatch.setattr(core_settings.FLOWFILE_COMPUTE_PROTOCOL, "value", True)
        monkeypatch.setattr(pl.LazyFrame, "collect_schema", lambda self: pl.Schema({"id": pl.Int64}))
        resolved, made, probed = [], [], []
        monkeypatch.setattr(CloudStorageReader, "get_storage_options", staticmethod(lambda c: resolved.append(c) or {}))
        original_init = cp.CredentialRefProvider.__init__

        def _record(provider, connection_id, user_id):
            made.append((connection_id, user_id))
            original_init(provider, connection_id, user_id)

        monkeypatch.setattr(cp.CredentialRefProvider, "__init__", _record)
        monkeypatch.setattr(
            FlowDataEngine, "_get_schema_from_first_file_in_dir", staticmethod(lambda *a, **k: probed.append(a))
        )
        fields = _CONNECTION_TYPES[connection_type]
        name = f"leak {connection_type} {uuid.uuid4().hex[:6]}"
        connection = FullCloudStorageConnection(connection_name=name, **fields)
        with get_db_context() as db:
            connection_id = store_cloud_connection(db, connection, user_id=1).id
        suffix = "/" if scan_mode == "directory" else _SUFFIXES[file_format]
        read = CloudStorageReadSettingsInternal(
            connection=connection,
            read_settings=CloudStorageReadSettings(
                connection_name=name,
                resource_path=_PATHS[fields["storage_type"]] + suffix,
                file_format=file_format,
                scan_mode=scan_mode,
            ),
        )
        try:
            plan = FlowDataEngine.from_cloud_storage_obj(read, user_id=1).data_frame.serialize()
        finally:
            with get_db_context() as db:
                delete_cloud_connection(db, name, user_id=1)

        assert resolved == []
        _assert_no_secret([plan], *(s.encode() for s in _SENTINELS.values()), b"$ffsec$", b"AKIDSENTINEL")
        if fields["auth_method"] in ("env_vars", "managed_identity"):
            assert made == []
            assert b"CredentialRefProvider" not in plan
        else:
            assert made == [(connection_id, 1)]
            assert b"CredentialRefProvider" in plan
            assert probed == []


def test_a_read_with_no_saved_connection_carries_nothing_core_resolved(
    protocol, storage_options_spy, monkeypatch, tmp_path
):
    """An aws-cli read with no saved connection, core's default profile holding a sentinel key; no I/O.

    Flag on, the plan carries no credential and no provider, so the default chain resolves where the plan runs; flag
    off, core resolves its default chain and the plan carries the keys encrypted.
    """
    sentinel = f"SENTINEL-{uuid.uuid4().hex}"
    isolate_aws(monkeypatch, tmp_path, {"default": ("AKIDNOCONNECTION", sentinel)})
    monkeypatch.setattr(pl.LazyFrame, "collect_schema", lambda self: pl.Schema({"id": pl.Int64}))
    read = CloudStorageReadSettingsInternal(
        connection=FullCloudStorageConnection(storage_type="s3", auth_method="aws-cli", connection_name=None),
        read_settings=CloudStorageReadSettings(
            auth_mode="aws-cli", resource_path=_PATHS["s3"] + _SUFFIXES["parquet"], file_format="parquet"
        ),
    )

    plan = FlowDataEngine.from_cloud_storage_obj(read, user_id=1).data_frame.serialize()

    _assert_no_secret([plan], sentinel.encode(), b"AKIDNOCONNECTION")
    if protocol:
        assert storage_options_spy == []
        _no_encrypted_provider([plan])
        assert b"CredentialRefProvider" not in plan
    else:
        assert len(storage_options_spy) == 1
        assert b"$ffsec$" in plan


@requires_minio
def test_an_in_code_connection_read_through_the_frame_carries_only_its_reference(protocol, minio_prefix):
    """A connection built in code is stored for the frame's user before the frame's reader names it.

    Flag on, the reader's plan carries the reference and nothing core resolved; flag off, the credentials encrypted
    for that user.
    """
    import flowfile_frame as ff

    paths = _seed_files(minio_prefix)
    name = f"plan secrets in code {uuid.uuid4().hex[:6]}"
    ff.create_cloud_storage_connection(
        _connection(
            name, "access_key", aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=SecretStr(MINIO_SECRET_KEY)
        )
    )
    try:
        frame = ff.scan_parquet_from_cloud_storage(paths["single"], connection_name=name)
        assert frame.collect().height == 3
        plan = frame.get_node_settings().get_resulting_data().data_frame.serialize()
    finally:
        ff.del_cloud_storage_connection(name)

    _assert_no_secret([plan], SECRET)
    if protocol:
        _no_encrypted_provider([plan])
        assert b"CredentialRefProvider" in plan
    else:
        assert b"$ffsec$" in plan


@pytest.mark.parametrize("stored", [False, True], ids=["never-stored", "stored-no-user"])
def test_a_direct_read_of_a_connection_not_stored_for_its_user_is_refused_with_the_flag_on(
    protocol, stored, stored_connection, storage_options_spy, monkeypatch
):
    """A read naming a connection the catalog does not hold for its user, or given no user; no I/O.

    Flag on, the read is refused before core resolves anything. Flag off, core resolves the connection and the plan
    carries its credentials encrypted: for the user, or with none in the legacy master-key form, which has no
    ``$ffsec$`` marker.
    """
    sentinel = f"SENTINEL-{uuid.uuid4().hex}"
    fields = dict(aws_access_key_id="AKIDSENTINEL", aws_secret_access_key=SecretStr(sentinel))
    if stored:
        name, user_id = stored_connection("access_key", **fields), None
    else:
        name, user_id = f"plan secrets in code {uuid.uuid4().hex[:6]}", 1
    monkeypatch.setattr(pl.LazyFrame, "collect_schema", lambda self: pl.Schema({"id": pl.Int64}))
    read = CloudStorageReadSettingsInternal(
        connection=_connection(name, "access_key", **fields),
        read_settings=CloudStorageReadSettings(
            connection_name=name, resource_path=_PATHS["s3"] + _SUFFIXES["parquet"], file_format="parquet"
        ),
    )

    if protocol:
        refusal = "is read with no user" if stored else "is not stored for user 1"
        with pytest.raises(ValueError, match=f'Cloud connection "{name}" {refusal}'):
            FlowDataEngine.from_cloud_storage_obj(read, user_id=user_id)
        assert storage_options_spy == []
        return
    plan = FlowDataEngine.from_cloud_storage_obj(read, user_id=user_id).data_frame.serialize()
    assert len(storage_options_spy) == 1
    _assert_no_secret([plan], sentinel.encode())
    assert b"EncryptedCredentialProvider" in plan
    assert (b"$ffsec$" in plan) is not stored


class _SubmitRaises:
    """A backend whose submit raises ``error``, and that cannot be asked for its capabilities."""

    id = "submit-raises"

    def __init__(self, error: Exception):
        self.error = error

    def capabilities(self):
        raise ComputeUnavailable("not asked")

    def submit(self, *args, **kwargs):
        raise self.error


@pytest.mark.parametrize(
    ("file_format", "kind"), [("parquet", "Parquet"), ("csv", "CSV"), ("json", "JSON"), ("delta", "Delta file")]
)
def test_a_compute_error_from_the_schema_read_reaches_the_caller_as_the_backend_raised_it(file_format, kind):
    """A reader leaves the backend's typed error as it is and wraps any other failure in its own message; no I/O."""
    read = CloudStorageReadSettingsInternal(
        connection=FullCloudStorageConnection(storage_type="s3", auth_method="env_vars", connection_name=None),
        read_settings=CloudStorageReadSettings(
            resource_path=_PATHS["s3"] + _SUFFIXES[file_format], file_format=file_format
        ),
    )
    unreachable = ComputeUnavailable("Could not reach the worker")

    with pytest.raises(ComputeUnavailable) as typed:
        FlowDataEngine.from_cloud_storage_obj(read, backend=_SubmitRaises(unreachable))
    with pytest.raises(Exception) as wrapped:
        FlowDataEngine.from_cloud_storage_obj(read, backend=_SubmitRaises(OSError("No such bucket")))

    assert typed.value is unreachable
    assert type(wrapped.value) is Exception
    assert str(wrapped.value) == f"Failed to read {kind} from cloud storage: No such bucket"


@requires_minio
def test_cloud_reader_plans_carry_no_secret(
    execution_location, protocol, stored_connection, minio_prefix, serialized_plans
):
    paths = _seed_files(minio_prefix)
    connection_name = stored_connection(
        "access_key", aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=SecretStr(MINIO_SECRET_KEY)
    )
    graph = create_test_graph(flow_id=7401, execution_location=execution_location)
    readers = {
        1: {"resource_path": paths["single"], "file_format": "parquet"},
        3: {"resource_path": paths["dir"], "file_format": "parquet", "scan_mode": "directory"},
        5: {"resource_path": paths["csv"], "file_format": "csv"},
        7: {"resource_path": paths["json"], "file_format": "json"},
        9: {"resource_path": paths["delta"], "file_format": "delta"},
        11: {
            "resource_path": paths["delta"],
            "file_format": "delta",
            "cdc_mode": "since_version",
            "cdc_from_version": 0,
        },
    }
    for node_id, settings in readers.items():
        _add_reader(graph, node_id, connection_name, **settings)
        _add_record_count(graph, node_id + 1, node_id)

    run_info = graph.run_graph()

    _assert_run_succeeded(run_info)
    # since_version 0 reads the commits after version 0: the one-row append.
    assert {node_id + 1: _count(graph, node_id + 1) for node_id in readers} == {2: 3, 4: 6, 6: 3, 8: 3, 10: 3, 12: 1}
    reader_plans = [graph.get_node(node_id).get_resulting_data().data_frame.serialize() for node_id in readers]
    if execution_location == "remote":
        assert len(serialized_plans) > len(reader_plans), "the run shipped no plan to the worker"
    _assert_no_secret(serialized_plans, SECRET)
    if protocol:
        _no_encrypted_provider(serialized_plans)
        assert any(b"CredentialRefProvider" in plan for plan in serialized_plans)


@requires_minio
@pytest.mark.parametrize("execution_location", ["remote"], indirect=True)
def test_a_cloud_read_on_the_worker_resolves_no_connection_in_core(
    execution_location, protocol, stored_connection, minio_prefix, monkeypatch
):
    """Core's credential providers and options builder raise, so a cloud read sent to the worker passes only if the
    worker resolves it.

    Flag on, each reader's schema comes from the worker; flag off, core builds the read's options.
    """
    paths = _seed_files(minio_prefix)
    connection_name = stored_connection(
        "access_key", aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=SecretStr(MINIO_SECRET_KEY)
    )
    resolved = []

    def _refuse(*args):
        resolved.append(args)
        raise RuntimeError("core resolved a connection for a read on the worker")

    monkeypatch.setattr(cp, "resolve_credential_ref", _refuse)
    monkeypatch.setattr(cp.EncryptedCredentialProvider, "__call__", _refuse)
    monkeypatch.setattr(CloudStorageReader, "get_storage_options", staticmethod(_refuse))
    graph = create_test_graph(flow_id=7410, execution_location=execution_location)
    readers = {
        1: {"resource_path": paths["single"], "file_format": "parquet"},
        3: {"resource_path": paths["dir"], "file_format": "parquet", "scan_mode": "directory"},
        5: {"resource_path": paths["csv"], "file_format": "csv"},
        7: {"resource_path": paths["csv_dir"], "file_format": "csv", "scan_mode": "directory"},
        9: {"resource_path": paths["json"], "file_format": "json"},
        11: {"resource_path": paths["json_dir"], "file_format": "json", "scan_mode": "directory"},
        13: {"resource_path": paths["delta"], "file_format": "delta"},
    }
    for node_id, settings in readers.items():
        _add_reader(graph, node_id, connection_name, **settings)
        _add_record_count(graph, node_id + 1, node_id)

    run_info = graph.run_graph()

    if protocol:
        _assert_run_succeeded(run_info)
        counts = {node_id + 1: _count(graph, node_id + 1) for node_id in readers}
        assert counts == {2: 3, 4: 6, 6: 3, 8: 6, 10: 3, 12: 6, 14: 3}
        assert [column.column_name for column in graph.get_node(1).get_resulting_data().schema] == ["id", "name"]
        assert resolved == []
    else:
        failures = {step.node_id: step.error for step in run_info.node_step_result if not step.success}
        assert set(readers) <= set(failures), failures
        assert all("core resolved a connection" in failures[node_id] for node_id in readers), failures


_GCS_FIXTURE = "gs://test-bucket"


@pytest.mark.skipif(not is_gcs_available(), reason="fake GCS not available")
@pytest.mark.parametrize("execution_location", ["remote"], indirect=True)
def test_a_gcs_read_with_an_endpoint_on_the_worker_reads_nothing_in_core(
    execution_location, protocol, serialized_plans, monkeypatch
):
    """A GCS connection with an endpoint, read on the worker, with core's gcsfs, options builder and resolution
    raising.

    Flag on, every reader is a native scan the worker reads and answers the schema of, no plan carrying rows; flag
    off, core builds the read's options.
    """
    import gcsfs.core

    name = f"plan secrets gcs endpoint {uuid.uuid4().hex[:6]}"
    with get_db_context() as db:
        store_cloud_connection(
            db,
            FullCloudStorageConnection(
                connection_name=name,
                storage_type="gcs",
                auth_method="env_vars",
                gcs_project_id="test-project",
                endpoint_url=GCS_ENDPOINT_URL,
            ),
            user_id=1,
        )
    resolved = []

    def _refuse(*args, **kwargs):
        resolved.append(args)
        raise RuntimeError("core resolved a connection for a read on the worker")

    monkeypatch.setattr(gcsfs.core.GCSFileSystem, "__init__", _refuse)
    monkeypatch.setattr(cp, "resolve_credential_ref", _refuse)
    monkeypatch.setattr(CloudStorageReader, "get_storage_options", staticmethod(_refuse))
    graph = create_test_graph(flow_id=7412, execution_location=execution_location)
    readers = {
        1: {"resource_path": f"{_GCS_FIXTURE}/single-file-parquet/data.parquet", "file_format": "parquet"},
        3: {"resource_path": f"{_GCS_FIXTURE}/multi-file-parquet/", "file_format": "parquet", "scan_mode": "directory"},
        5: {"resource_path": f"{_GCS_FIXTURE}/single-file-csv/data.csv", "file_format": "csv"},
        7: {"resource_path": f"{_GCS_FIXTURE}/multi-file-csv/", "file_format": "csv", "scan_mode": "directory"},
        9: {"resource_path": f"{_GCS_FIXTURE}/single-file-json/data.json", "file_format": "json"},
        11: {"resource_path": f"{_GCS_FIXTURE}/multi-file-json/", "file_format": "json", "scan_mode": "directory"},
        13: {"resource_path": f"{_GCS_FIXTURE}/delta-lake-table", "file_format": "delta"},
    }
    try:
        for node_id, settings in readers.items():
            _add_reader(graph, node_id, name, **settings)
            _add_record_count(graph, node_id + 1, node_id)

        run_info = graph.run_graph()
    finally:
        with get_db_context() as db:
            delete_cloud_connection(db, name, user_id=1)

    if protocol:
        _assert_run_succeeded(run_info)
        assert {node_id + 1: _count(graph, node_id + 1) for node_id in readers} == {
            node_id + 1: 100_000 for node_id in readers
        }
        schema = graph.get_node(1).get_resulting_data().schema
        assert [column.column_name for column in schema] == ["id", "name", "value", "category"]
        assert resolved == []
        assert serialized_plans, "the run shipped no plan to the worker"
        assert max(len(plan) for plan in serialized_plans) < 10_000, "a plan core serialised carries rows"
    else:
        failures = {step.node_id: step.error for step in run_info.node_step_result if not step.success}
        assert set(readers) <= set(failures), failures
        assert all("core resolved a connection" in failures[node_id] for node_id in readers), failures


def test_a_catalog_target_over_a_gcs_namespace_with_an_endpoint_carries_no_key(protocol):
    """A catalog stored on GCS behind an endpoint, its connection holding a sentinel key; no I/O.

    Flag on, the target's scan names the connection and carries no key; flag off, Polars refuses the gcsfs options
    before any plan exists.
    """
    from flowfile_core.catalog.services.namespaces import NamespaceService

    name = f"plan secrets gcs catalog {uuid.uuid4().hex[:6]}"
    with get_db_context() as db:
        store_cloud_connection(
            db,
            FullCloudStorageConnection(connection_name=name, **_CONNECTION_TYPES["gcs-service_account-endpoint"]),
            user_id=1,
        )
    try:
        with get_db_context() as db:
            catalog_id = (
                NamespaceService(SQLAlchemyCatalogRepository(db))
                .create_namespace(
                    f"gcscat_{uuid.uuid4().hex[:8]}",
                    owner_id=1,
                    storage_uri=f"{_GCS_FIXTURE}/catalog",
                    storage_connection_name=name,
                )
                .id
            )
        with get_db_context() as db:
            schema_id = (
                NamespaceService(SQLAlchemyCatalogRepository(db))
                .create_namespace(f"gcssch_{uuid.uuid4().hex[:8]}", owner_id=1, parent_id=catalog_id)
                .id
            )
        kwargs = resolve_for_namespace(schema_id).scan_kwargs(1)
        path = f"{_GCS_FIXTURE}/catalog/never_read"
        if protocol:
            plan = pl.scan_delta(path, **kwargs).serialize()
            _assert_no_secret([plan], _SENTINELS["gcs"].encode())
            _no_encrypted_provider([plan])
            assert b"CredentialRefProvider" in plan
        else:
            with pytest.raises(ValueError, match="cannot combine token with"):
                pl.scan_delta(path, **kwargs)
    finally:
        catalog_cleanup()
        with get_db_context() as db:
            delete_cloud_connection(db, name, user_id=1)


@requires_minio
def test_iam_role_reader_plan_carries_no_temporary_credentials(
    execution_location,
    protocol,
    stored_connection,
    minio_prefix,
    minio_sts,
    storage_options_spy,
    serialized_plans,
    monkeypatch,
):
    """iam_role assumes the role where it is resolved; the temporary key, secret and session token never enter a plan.

    Flag off, core assumes it and the plan carries them encrypted; flag on, the plan names the connection.
    """
    if protocol and execution_location == "remote":
        pytest.skip(
            "with the protocol flag a worker assumes the role with its own identity: see the worker-child tests"
        )
    resolved = []
    original = cp.resolve_credential_ref

    def _record(connection_id, user_id):
        resolved.append(original(connection_id, user_id))
        return resolved[-1]

    monkeypatch.setattr(cp, "resolve_credential_ref", _record)
    paths = _seed_files(minio_prefix)
    connection_name = stored_connection("iam_role", aws_role_arn="arn:aws:iam::123456789012:role/flowfile-test")
    graph = create_test_graph(flow_id=7402, execution_location=execution_location)
    _add_reader(graph, 1, connection_name, resource_path=paths["single"], file_format="parquet")
    _add_record_count(graph, 2, 1)

    run_info = graph.run_graph()

    _assert_run_succeeded(run_info)
    assert _count(graph, 2) == 3
    graph.get_node(1).get_resulting_data().data_frame.serialize()
    sts_options = [options for options in storage_options_spy if options.get("aws_session_token")]
    sts_options += [credentials for credentials, _ in resolved]
    assert sts_options, "the iam_role connection never assumed a role"
    assert bool(resolved) == protocol
    temporary = [
        options[key].encode()
        for options in sts_options
        for key in ("aws_access_key_id", "aws_secret_access_key", "aws_session_token")
    ]
    _assert_no_secret(serialized_plans, SECRET, *temporary)


@requires_minio
def test_iam_role_local_write_uses_the_assumed_role(stored_connection, minio_prefix, minio_sts):
    """The removed placeholder provider also shadowed the STS credentials of core-side (local) writes."""
    connection_name = stored_connection("iam_role", aws_role_arn="arn:aws:iam::123456789012:role/flowfile-test")
    target = f"s3://{_BUCKET}/{minio_prefix}/iam_out.parquet"
    graph = create_test_graph(flow_id=7403, execution_location="local")
    add_test_manual_input(graph, _ROWS, node_id=1)
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="cloud_storage_writer"))
    graph.add_cloud_storage_writer(
        input_schema.NodeCloudStorageWriter(
            flow_id=graph.flow_id,
            node_id=2,
            user_id=1,
            depending_on_id=1,
            cloud_storage_settings=CloudStorageWriteSettings(
                resource_path=target, file_format="parquet", write_mode="overwrite", connection_name=connection_name
            ),
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    _assert_run_succeeded(graph.run_graph())
    assert pl.read_parquet(target, storage_options=_ROOT_OPTIONS).height == 3


@pytest.fixture
def azurite_container():
    from test_utils.azurite.fixtures import get_blob_service_client, is_azurite_reachable

    if not is_azurite_reachable():
        pytest.skip("Azurite not available")
    client = get_blob_service_client()
    name = f"plan-secrets-{uuid.uuid4().hex[:8]}"
    client.create_container(name)
    yield client.get_container_client(name)
    client.delete_container(name)


def test_adls_reader_plan_carries_no_account_key(execution_location, protocol, azurite_container, serialized_plans):
    from test_utils.azurite.fixtures import AZURITE_ACCOUNT_KEY, AZURITE_ACCOUNT_NAME, AZURITE_BLOB_PORT, AZURITE_HOST

    buffer = io.BytesIO()
    pl.DataFrame(_ROWS).write_parquet(buffer)
    azurite_container.upload_blob("data.parquet", buffer.getvalue())
    connection_name = f"plan secrets adls {uuid.uuid4().hex[:6]}"
    with get_db_context() as db:
        store_cloud_connection(
            db,
            FullCloudStorageConnection(
                connection_name=connection_name,
                storage_type="adls",
                auth_method="access_key",
                azure_account_name=AZURITE_ACCOUNT_NAME,
                azure_account_key=SecretStr(AZURITE_ACCOUNT_KEY),
                endpoint_url=f"http://{AZURITE_HOST}:{AZURITE_BLOB_PORT}",
            ),
            user_id=1,
        )
    try:
        graph = create_test_graph(flow_id=7406, execution_location=execution_location)
        path = f"az://{azurite_container.container_name}/data.parquet"
        _add_reader(graph, 1, connection_name, resource_path=path, file_format="parquet")
        _add_record_count(graph, 2, 1)

        _assert_run_succeeded(graph.run_graph())

        assert _count(graph, 2) == 3
        graph.get_node(1).get_resulting_data().data_frame.serialize()
        _assert_no_secret(serialized_plans, AZURITE_ACCOUNT_KEY.encode())
    finally:
        with get_db_context() as db:
            delete_cloud_connection(db, connection_name, user_id=1)


@requires_minio
class TestCatalogReadersOnObjectStorage:
    @pytest.fixture(autouse=True)
    def _clean_catalog(self, monkeypatch):
        monkeypatch.delenv("FLOWFILE_CATALOG_STORAGE_URI", raising=False)
        monkeypatch.delenv("FLOWFILE_CATALOG_STORAGE_CONNECTION", raising=False)
        catalog_cleanup()
        yield
        catalog_cleanup()

    def test_table_sql_virtual_and_change_reads_carry_no_secret(
        self, execution_location, protocol, minio_prefix, serialized_plans
    ):
        _ensure_connection()
        schema_id = _create_cloud_schema(f"s3://{_BUCKET}/{minio_prefix}/catalog")
        target = resolve_for_namespace(schema_id)
        dest = f"s3://{_BUCKET}/{minio_prefix}/catalog/secret_probe"
        write_delta(pl.DataFrame(_ROWS), dest, mode="overwrite", storage_options=target.storage_options)
        table_name = f"secret_probe_{uuid.uuid4().hex[:6]}"
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            table_id = svc.register_table_from_data(
                name=table_name,
                table_path=dest,
                owner_id=1,
                namespace_id=schema_id,
                storage_format="delta",
                schema=[{"name": "id", "dtype": "Int64"}, {"name": "name", "dtype": "Utf8"}],
                row_count=3,
                column_count=2,
                size_bytes=100,
            ).id
            svc.enable_table_cdc(table_id)
            # The SQL editor refuses cloud tables, so the query virtual table is put in the catalog directly.
            virtual_id = (
                SQLAlchemyCatalogRepository(db)
                .create_table(
                    CatalogTable(
                        name=f"{table_name}_query",
                        namespace_id=schema_id,
                        owner_id=1,
                        table_type="virtual",
                        sql_query=f"SELECT * FROM {table_name} WHERE id > 1",
                    )
                )
                .id
            )
        write_delta(pl.DataFrame(_ROWS[:1]), dest, mode="append", storage_options=target.storage_options)

        graph = create_test_graph(flow_id=7404, execution_location=execution_location)
        readers = {
            1: input_schema.NodeCatalogReader(flow_id=graph.flow_id, node_id=1, catalog_table_id=table_id),
            3: input_schema.NodeCatalogReader(
                flow_id=graph.flow_id, node_id=3, sql_query=f"SELECT * FROM {table_name} WHERE id > 1"
            ),
            5: input_schema.NodeCatalogReader(
                flow_id=graph.flow_id,
                node_id=5,
                catalog_table_id=table_id,
                cdc_mode="since_version",
                cdc_from_version=0,
            ),
            7: input_schema.NodeCatalogReader(flow_id=graph.flow_id, node_id=7, catalog_table_id=virtual_id),
        }
        for node_id, settings in readers.items():
            graph.add_node_promise(
                input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="catalog_reader")
            )
            graph.add_catalog_reader(settings)
            _add_record_count(graph, node_id + 1, node_id)

        run_info = graph.run_graph()

        _assert_run_succeeded(run_info)
        assert {node_id + 1: _count(graph, node_id + 1) for node_id in readers} == {2: 4, 4: 2, 6: 1, 8: 2}
        for node_id in readers:
            graph.get_node(node_id).get_resulting_data().data_frame.serialize()
        _assert_no_secret(serialized_plans, SECRET)
        if protocol:
            _no_encrypted_provider(serialized_plans)
            assert any(b"CredentialRefProvider" in plan for plan in serialized_plans)

    def test_scd2_writer_output_carries_no_secret(self, execution_location, protocol, minio_prefix, serialized_plans):
        _ensure_connection()
        schema_id = _create_cloud_schema(f"s3://{_BUCKET}/{minio_prefix}/catalog")
        graph = create_test_graph(flow_id=7408, execution_location=execution_location)
        add_test_manual_input(graph, _ROWS, node_id=1)
        add_test_catalog_writer(
            graph,
            node_id=2,
            depending_on_id=1,
            table_name=f"scd2_probe_{uuid.uuid4().hex[:6]}",
            namespace_id=schema_id,
            write_mode="scd2",
            merge_keys=["id"],
            scd2=input_schema.Scd2Settings(),
        )
        _add_record_count(graph, 3, 2)

        _assert_run_succeeded(graph.run_graph())

        assert _count(graph, 3) == 3
        graph.get_node(2).get_resulting_data().data_frame.serialize()
        _assert_no_secret(serialized_plans, SECRET)
        if protocol:
            _no_encrypted_provider(serialized_plans)
            assert any(b"CredentialRefProvider" in plan for plan in serialized_plans)

    @pytest.mark.parametrize("execution_location", ["remote"], indirect=True)
    def test_a_catalog_table_read_on_the_worker_resolves_no_connection_in_core(
        self, execution_location, protocol, minio_prefix, monkeypatch
    ):
        """Core's credential providers raise, so a table read sent to the worker passes only if the worker resolves it.

        Flag on, the reader's schema comes from the worker; flag off, core decrypts the plan's ciphertext to build it.
        """
        _ensure_connection()
        schema_id = _create_cloud_schema(f"s3://{_BUCKET}/{minio_prefix}/catalog")
        target = resolve_for_namespace(schema_id)
        dest = f"s3://{_BUCKET}/{minio_prefix}/catalog/core_probe"
        write_delta(pl.DataFrame(_ROWS), dest, mode="overwrite", storage_options=target.storage_options)
        with get_db_context() as db:
            table_id = (
                CatalogService(SQLAlchemyCatalogRepository(db))
                .register_table_from_data(
                    name=f"core_probe_{uuid.uuid4().hex[:6]}",
                    table_path=dest,
                    owner_id=1,
                    namespace_id=schema_id,
                    storage_format="delta",
                    schema=[{"name": "id", "dtype": "Int64"}, {"name": "name", "dtype": "Utf8"}],
                    row_count=3,
                    column_count=2,
                    size_bytes=100,
                )
                .id
            )
        write_delta(pl.DataFrame(_ROWS[:1]), dest, mode="append", storage_options=target.storage_options)
        resolved = []

        def _refuse(*args):
            resolved.append(args)
            raise RuntimeError("core resolved a connection for a read on the worker")

        monkeypatch.setattr(cp, "resolve_credential_ref", _refuse)
        monkeypatch.setattr(cp.EncryptedCredentialProvider, "__call__", _refuse)
        graph = create_test_graph(flow_id=7409, execution_location=execution_location)
        for node_id, version in ((1, None), (3, 0)):
            graph.add_node_promise(
                input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="catalog_reader")
            )
            graph.add_catalog_reader(
                input_schema.NodeCatalogReader(
                    flow_id=graph.flow_id, node_id=node_id, catalog_table_id=table_id, delta_version=version
                )
            )
            _add_record_count(graph, node_id + 1, node_id)

        run_info = graph.run_graph()

        if protocol:
            _assert_run_succeeded(run_info)
            assert {node_id: _count(graph, node_id) for node_id in (2, 4)} == {2: 4, 4: 3}
            assert [column.column_name for column in graph.get_node(1).get_resulting_data().schema] == ["id", "name"]
            assert resolved == []
        else:
            failures = {step.node_id: step.error for step in run_info.node_step_result if not step.success}
            assert {1, 3} <= set(failures), failures
            assert all("core resolved a connection" in failures[node_id] for node_id in (1, 3)), failures


@requires_minio
class TestPlanExecutesWhereItIsDecrypted:
    """A core-built plan run in a fresh worker-shaped process, the way a spawned worker child runs it.

    The graphs run in Performance mode: the worker holds a read it does not preview as the plan core sent, so the
    node's leaf is that read. Previewed, the worker stores the rows and the leaf scans its file, which needs no key.
    """

    @pytest.fixture
    def reader_plan(self, protocol, stored_connection, minio_prefix) -> bytes:
        paths = _seed_files(minio_prefix)
        connection_name = stored_connection(
            "access_key", aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=SecretStr(MINIO_SECRET_KEY)
        )
        graph = create_test_graph(flow_id=7405, execution_location="local")
        graph.flow_settings.execution_mode = "Performance"
        _add_reader(graph, 1, connection_name, resource_path=paths["single"], file_format="parquet")
        _assert_run_succeeded(graph.run_graph())
        plan = graph.get_node(1).get_resulting_data().data_frame.serialize()
        assert paths["single"].encode() in plan
        assert SECRET not in plan
        return plan

    def _run_in_worker_child(self, plan: bytes, tmp_path: Path, **env_overrides) -> subprocess.CompletedProcess:
        plan_file = tmp_path / "plan.bin"
        plan_file.write_bytes(plan)
        code = textwrap.dedent(
            f"""
            import io, multiprocessing
            multiprocessing.current_process().name = "Worker-1"
            import flowfile_worker.funcs  # the spawned child's entry surface registers the decryptor
            import polars as pl
            print(pl.LazyFrame.deserialize(io.BytesIO(open({str(plan_file)!r}, "rb").read())).collect().height)
            """
        )
        env = {k: v for k, v in os.environ.items() if not k.startswith("AWS_") and k != "TEST_MODE"}
        env.update(
            AWS_EC2_METADATA_DISABLED="true",
            PYTHONPATH=os.pathsep.join([str(REPO_ROOT / "flowfile_worker"), str(REPO_ROOT)]),
        )
        env.update(env_overrides)
        return subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, env=env, cwd=tmp_path, timeout=120
        )

    def test_worker_child_decrypts_with_the_shared_master_key(self, reader_plan, tmp_path):
        ran = self._run_in_worker_child(reader_plan, tmp_path)
        assert ran.returncode == 0, ran.stderr
        assert ran.stdout.strip().splitlines()[-1] == "3"

    def test_a_worker_without_the_master_key_fails_with_a_clear_error(self, protocol, reader_plan, tmp_path):
        empty_store = tmp_path / "other_secure_store"
        empty_store.mkdir()

        ran = self._run_in_worker_child(reader_plan, tmp_path, FLOWFILE_SECURE_STORAGE_PATH=str(empty_store))

        assert ran.returncode != 0
        if protocol:
            assert "Could not decrypt the secrets of cloud connection" in ran.stderr
        else:
            assert "Could not decrypt the cloud storage credentials embedded in this query plan" in ran.stderr
        assert "Master key not found" in ran.stderr
        assert MINIO_SECRET_KEY not in ran.stderr

    @pytest.mark.parametrize("protocol", [True], indirect=True, ids=["protocol-on"])
    def test_a_worker_on_another_catalog_cannot_resolve_the_reference(self, protocol, reader_plan, tmp_path):
        other = tmp_path / "other_catalog.db"
        engine = create_engine(f"sqlite:///{other}")
        Base.metadata.create_all(engine)
        engine.dispose()

        ran = self._run_in_worker_child(reader_plan, tmp_path, FLOWFILE_DATABASE_URL=f"sqlite:///{other}")

        assert ran.returncode != 0
        assert "The catalog database this process reads has no cloud connection" in ran.stderr
        assert MINIO_SECRET_KEY not in ran.stderr

    @pytest.mark.parametrize("protocol", [True], indirect=True, ids=["protocol-on"])
    def test_iam_role_is_assumed_with_the_executing_worker_identity(
        self, protocol, stored_connection, minio_prefix, minio_sts, tmp_path
    ):
        paths = _seed_files(minio_prefix)
        connection_name = stored_connection("iam_role", aws_role_arn="arn:aws:iam::123456789012:role/flowfile-test")
        graph = create_test_graph(flow_id=7407, execution_location="local")
        graph.flow_settings.execution_mode = "Performance"
        _add_reader(graph, 1, connection_name, resource_path=paths["single"], file_format="parquet")
        _assert_run_succeeded(graph.run_graph())
        plan = graph.get_node(1).get_resulting_data().data_frame.serialize()
        assert paths["single"].encode() in plan
        no_profile = {"AWS_CONFIG_FILE": os.devnull, "AWS_SHARED_CREDENTIALS_FILE": os.devnull}

        with_role = self._run_in_worker_child(
            plan,
            tmp_path,
            AWS_ACCESS_KEY_ID=MINIO_ACCESS_KEY,
            AWS_SECRET_ACCESS_KEY=MINIO_SECRET_KEY,
            AWS_ENDPOINT_URL_STS=MINIO_ENDPOINT_URL,
            **no_profile,
        )
        without = self._run_in_worker_child(plan, tmp_path, **no_profile)

        assert with_role.returncode == 0, with_role.stderr
        assert with_role.stdout.strip().splitlines()[-1] == "3"
        assert without.returncode != 0
        assert "NoCredentialsError" in without.stderr
