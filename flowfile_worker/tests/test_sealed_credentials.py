"""A worker on a hub with keys of its own opens what the hub sealed to it: the exchange's export and import, a Delta
write's connection, and its capabilities say so.

The hub is ``HubSigner``; the node ``team`` has its keys under the test's storage dir; the data is on the suite's
MinIO, and the cases that read or write it skip without one. Jobs run in this process (``spawner.in_process``), so
they read the same node files the test made.
"""

import uuid

import polars as pl
import pytest
from fastapi.testclient import TestClient
from polars.testing import assert_frame_equal

from flowfile_worker import funcs, main, spawner
from shared import hub_node_key
from shared.cloud_storage.storage_options import build_storage_options
from shared.compute_protocol import Capabilities, ResultRef, schema_to_wire
from shared.tests import hub_jwks
from shared.tests.hub_jwks import NODE_NAME
from test_utils.s3.fixtures import MINIO_ACCESS_KEY, MINIO_ENDPOINT_URL, MINIO_SECRET_KEY, get_minio_client
from tests.conftest import V1_HEADERS, ref_location
from tests.test_protocol_routes import _submit

hub_signer = hub_jwks.hub_signer
client = TestClient(main.app, headers=V1_HEADERS)

BUCKET = "flowfile-test"
MINIO = build_storage_options(
    storage_type="s3",
    auth_method="access_key",
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    aws_region="us-east-1",
    endpoint_url=MINIO_ENDPOINT_URL,
    aws_allow_unsafe_html=True,
)


def _minio_available() -> bool:
    try:
        get_minio_client().list_buckets()
        return True
    except Exception:
        return False


requires_minio = pytest.mark.skipif(not _minio_available(), reason="MinIO is not running")


@pytest.fixture
def node(hub_signer):
    """This worker's keys as node ``team``, made in the test's storage dir."""
    return hub_node_key.load_or_create()


@pytest.fixture
def in_process(monkeypatch):
    monkeypatch.setattr(spawner, "in_process", True)


@pytest.fixture
def prefix():
    """A fresh MinIO prefix, emptied afterwards."""
    root = f"t53-sealed/{uuid.uuid4().hex[:12]}"
    yield f"s3://{BUCKET}/{root}"
    minio = get_minio_client()
    for listed in minio.list_objects_v2(Bucket=BUCKET, Prefix=f"{root}/").get("Contents", []):
        minio.delete_object(Bucket=BUCKET, Key=listed["Key"])


def sealed_map(signer, keys, base_uri: str) -> dict[str, str]:
    """The vend's map for a run on ``team`` and another node, as the hub seals it."""
    token = signer.sealed(keys.public_jwks()["enc"], base_uri=base_uri, storage_options=dict(MINIO))
    return {NODE_NAME: token, "core": "sealed.to.another.node.key"}


def test_a_worker_with_keys_on_a_hub_declares_vended_credentials(node):
    caps = Capabilities.model_validate(client.get("/v1/capabilities").json())

    assert caps.credential_resolution == "vended"


def test_a_worker_without_keys_declares_its_master_key(hub_signer):
    caps = Capabilities.model_validate(client.get("/v1/capabilities").json())

    assert caps.credential_resolution == "master_key"


@requires_minio
def test_an_import_with_a_sealed_credential_holds_a_plan_with_no_secret_that_reads_the_store(
    held, node, hub_signer, prefix, in_process
):
    frame = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    uri = f"{prefix}/in.parquet"
    frame.write_parquet(uri, storage_options=MINIO)
    result_id = held()
    body = {
        "uri": uri,
        "credential": {"sealed": sealed_map(hub_signer, node, prefix)},
        "columns": [column.model_dump() for column in schema_to_wire(frame.schema)],
        "row_count": 3,
    }

    response = client.post(f"/v1/results/{result_id}/import", json=body)

    assert response.status_code == 200, response.text
    ref = ResultRef.model_validate_json(response.text)
    plan = ref_location(ref).plan
    assert MINIO_SECRET_KEY.encode() not in plan and MINIO_ACCESS_KEY.encode() not in plan
    sample = client.get(f"/v1/results/{result_id}/sample", params={"n": 3})
    assert_frame_equal(pl.read_ipc_stream(sample.content), frame)


@requires_minio
def test_an_export_with_a_sealed_credential_writes_to_the_store(held, node, hub_signer, prefix, in_process):
    frame = pl.DataFrame({"id": [4, 5], "name": ["d", "e"]})
    result_id = held()
    _submit(result_id, frame.lazy(), must_materialise=True)
    uri = f"{prefix}/out.parquet"

    exported = client.post(
        f"/v1/results/{result_id}/export",
        json={"uri": uri, "credential": {"sealed": sealed_map(hub_signer, node, prefix)}},
    )

    assert exported.status_code == 200, exported.text
    assert_frame_equal(pl.read_parquet(uri, storage_options=MINIO), frame)


def test_a_sealed_credential_this_node_cannot_open_fails_the_import_with_the_reason(held, node, hub_signer):
    body = {
        "uri": f"s3://{BUCKET}/nowhere.parquet",
        "credential": {"sealed": {"core": "sealed.to.another.node.key"}},
        "columns": [],
        "row_count": 0,
    }

    response = client.post(f"/v1/results/{held()}/import", json=body)

    assert response.status_code == 500
    assert "sealed it to core, not to this server (team)" in response.json()["detail"]["error_message"]


def test_an_empty_sealed_map_is_refused_before_anything_runs(held, node):
    body = {"uri": "s3://bucket/x.parquet", "credential": {"sealed": {}}, "columns": [], "row_count": 0}

    assert client.post(f"/v1/results/{held()}/import", json=body).status_code == 422


def test_a_delta_writes_sealed_connection_resolves_to_the_vended_options(node, hub_signer):
    payload = {
        "base_uri": "s3://bucket/root",
        "connection": {
            "storage_type": "s3",
            "auth_method": "access_key",
            "connection_name": None,
            "sealed": sealed_map(hub_signer, node, "s3://bucket/root"),
        },
    }

    assert funcs._resolve_storage_options(payload) == MINIO
