"""Publishing a result a backend holds to the artifact store, and fetching a published artifact onto a backend.

Core prepares, finalises and looks up the catalog row; the backend copies the bytes, to and from a ``file://`` URI on
the filesystem store and presigned ``https://`` URLs on MinIO behind a self-signed TLS forwarder.
"""

import hashlib
import os
import uuid

import pytest

from flowfile_core.artifacts import (
    ArtifactNotFoundError,
    ArtifactUploadError,
    get_storage_backend,
    publish,
    reset_storage_backend,
)
from flowfile_core.artifacts.publish import fetch_artifact, publish_result
from flowfile_core.configs import settings
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import GlobalArtifact
from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.errors import ResultMissing
from flowfile_core.schemas.artifact_schema import PrepareUploadRequest
from shared.artifact_storage import S3Storage, SharedFilesystemStorage
from shared.compute_protocol import file_uri
from test_utils import tls

OWNER_ID = 1
MODEL = bytes(range(256)) * 64
RETRAINED = bytes(reversed(range(256))) * 32
MINIO_BUCKET = "worker-test-bucket"


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _name() -> str:
    return f"model_{uuid.uuid4().hex[:12]}"


def _request(name: str, registration_id: int) -> PrepareUploadRequest:
    return PrepareUploadRequest(name=name, source_registration_id=registration_id, serialization_format="json")


def _publish(backend, ref, name: str, registration_id: int, data: bytes, sha256: str | None = None):
    return publish_result(
        backend,
        ref,
        _request(name, registration_id),
        owner_id=OWNER_ID,
        sha256=sha256 or _digest(data),
        size_bytes=len(data),
    )


def _versions(name: str) -> list[tuple[int, str]]:
    """Every version of ``name`` that is not deleted, as ``(version, status)``."""
    with get_db_context() as db:
        rows = db.query(GlobalArtifact).filter(GlobalArtifact.name == name, GlobalArtifact.status != "deleted")
        return sorted((row.version, row.status) for row in rows)


def _artifact_ids(name: str) -> list[int]:
    with get_db_context() as db:
        return [row.id for row in db.query(GlobalArtifact).filter_by(name=name)]


def _storage_key(artifact_id: int) -> str:
    with get_db_context() as db:
        return db.get(GlobalArtifact, artifact_id).storage_key


def _exported(backend, ref, tmp_path) -> bytes:
    target = tmp_path / f"{uuid.uuid4().hex}.bin"
    assert backend.export(ref, file_uri(target), None) == file_uri(target)
    return target.read_bytes()


@pytest.fixture(params=["local", "worker"])
def backend(request):
    if request.param == "worker":
        if not settings.FLOWFILE_COMPUTE_PROTOCOL:
            pytest.skip("the worker backend holds a blob over its /v1 routes, with the protocol flag on")
        return WorkerBackend(url=settings.WORKER_URL)
    return EmbeddedWorkerBackend()


@pytest.fixture
def hold(tmp_path):
    """Hold bytes as a blob on a backend, imported from a file that is removed at once; every blob is released after."""
    held = []

    def _hold(backend, data: bytes):
        source = tmp_path / f"{uuid.uuid4().hex}.bin"
        source.write_bytes(data)
        ref = backend.import_uri(file_uri(source), None, None, None, kind="blob")
        source.unlink()
        held.append((backend, ref))
        return ref

    yield _hold
    for backend, ref in held:
        backend.release(ref)


def test_a_held_blob_is_published_and_fetched_back_on_the_filesystem_store(backend, registration, hold, tmp_path):
    namespace_id, registration_id = registration
    store = get_storage_backend()
    name = _name()

    published = _publish(backend, hold(backend, MODEL), name, registration_id, MODEL)

    assert (published.status, published.version) == ("ok", 1)
    with get_db_context() as db:
        row = db.get(GlobalArtifact, published.artifact_id)
        assert (row.status, row.sha256, row.size_bytes, row.namespace_id) == (
            "active",
            _digest(MODEL),
            len(MODEL),
            namespace_id,
        )
        storage_key = row.storage_key
    assert (store.permanent / storage_key).read_bytes() == MODEL
    assert not list(store.staging.glob(f"{published.artifact_id}_*"))

    fetched = fetch_artifact(backend, name, None, namespace_id)

    try:
        assert (fetched.backend_id, fetched.kind, fetched.state, fetched.schema, fetched.row_count) == (
            backend.id,
            "blob",
            "materialised",
            [],
            None,
        )
        assert backend.exists(fetched)
        assert _exported(backend, fetched, tmp_path) == MODEL
    finally:
        backend.release(fetched)
    assert backend.exists(fetched) is False
    assert (store.permanent / storage_key).read_bytes() == MODEL

    retrained = _publish(backend, hold(backend, RETRAINED), name, registration_id, RETRAINED)

    assert retrained.version == 2
    assert _versions(name) == [(1, "active"), (2, "active")]
    latest, first = fetch_artifact(backend, name, None, namespace_id), fetch_artifact(backend, name, 1, namespace_id)
    try:
        assert _exported(backend, latest, tmp_path) == RETRAINED
        assert _exported(backend, first, tmp_path) == MODEL
    finally:
        backend.release(latest)
        backend.release(first)


def test_a_failed_export_leaves_no_artifact(backend, registration, hold):
    _, registration_id = registration
    released = hold(backend, MODEL)
    backend.release(released)
    name = _name()

    with pytest.raises(ResultMissing):
        _publish(backend, released, name, registration_id, MODEL)

    assert _versions(name) == []


def test_a_digest_the_store_cannot_verify_leaves_no_artifact(backend, registration, hold):
    _, registration_id = registration
    store = get_storage_backend()
    ref = hold(backend, MODEL)
    name = _name()

    with pytest.raises(ArtifactUploadError):
        _publish(backend, ref, name, registration_id, MODEL, sha256=_digest(RETRAINED))

    assert _versions(name) == []
    (artifact_id,) = _artifact_ids(name)
    assert not list(store.staging.glob(f"{artifact_id}_*"))
    assert backend.exists(ref)


def test_a_store_under_a_folder_with_a_space_publishes_and_fetches(registration, hold, monkeypatch, tmp_path):
    namespace_id, registration_id = registration
    root = tmp_path / "a b"
    store = SharedFilesystemStorage(staging_root=root / "staging", artifacts_root=root / "artifacts")
    monkeypatch.setattr(publish, "get_storage_backend", lambda: store)
    backend = EmbeddedWorkerBackend()
    name = _name()

    published = _publish(backend, hold(backend, MODEL), name, registration_id, MODEL)
    fetched = fetch_artifact(backend, name, None, namespace_id)

    try:
        assert (store.permanent / _storage_key(published.artifact_id)).read_bytes() == MODEL
        assert _exported(backend, fetched, tmp_path) == MODEL
    finally:
        backend.release(fetched)


def test_an_unknown_model_is_not_found_and_nothing_is_held(registration, hold, monkeypatch):
    namespace_id, registration_id = registration
    backend = EmbeddedWorkerBackend()
    name = _name()
    _publish(backend, hold(backend, MODEL), name, registration_id, MODEL)
    imports = []
    monkeypatch.setattr(backend, "import_uri", lambda *args, **kwargs: imports.append(args))

    for unknown, version in ((_name(), None), (name, 2)):
        with pytest.raises(ArtifactNotFoundError):
            fetch_artifact(backend, unknown, version, namespace_id)

    assert imports == []


@pytest.fixture(scope="module")
def minio_over_tls(tmp_path_factory):
    """MinIO behind an HTTPS forwarder with a self-signed certificate: its URL and the certificate's path."""
    with tls.minio_over_tls(tmp_path_factory.mktemp("tls")) as endpoint:
        yield endpoint


@pytest.fixture
def s3_store(minio_over_tls, monkeypatch):
    """The artifact store on MinIO behind the forwarder, under a key prefix of the test's own; the prefix.

    boto3 and ``requests`` trust the forwarder's certificate; every key under the prefix is deleted after.
    """
    from test_utils.s3 import fixtures as s3

    endpoint, certificate = minio_over_tls
    prefix = f"publish-{uuid.uuid4().hex}/"
    monkeypatch.setenv("FLOWFILE_ARTIFACT_STORAGE", "s3")
    monkeypatch.setenv("FLOWFILE_S3_BUCKET", MINIO_BUCKET)
    monkeypatch.setenv("FLOWFILE_S3_PREFIX", prefix)
    monkeypatch.setenv("FLOWFILE_S3_ENDPOINT_URL", endpoint)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", s3.MINIO_ACCESS_KEY)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", s3.MINIO_SECRET_KEY)
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    monkeypatch.setenv("AWS_CA_BUNDLE", str(certificate))
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(certificate))
    reset_storage_backend()
    yield prefix
    reset_storage_backend()
    minio = s3.get_minio_client()
    for listed in minio.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix).get("Contents", []):
        minio.delete_object(Bucket=MINIO_BUCKET, Key=listed["Key"])


def test_a_held_blob_is_published_to_minio_and_fetched_through_presigned_urls(registration, hold, s3_store, tmp_path):
    from test_utils.s3.fixtures import get_minio_client

    namespace_id, registration_id = registration
    backend = EmbeddedWorkerBackend()
    model = os.urandom(3 * 1024 * 1024)
    name = _name()

    published = _publish(backend, hold(backend, model), name, registration_id, model)

    assert published.version == 1
    storage_key = _storage_key(published.artifact_id)
    store = get_storage_backend()
    assert isinstance(store, S3Storage)
    assert store.prepare_download(storage_key).path.startswith("https://")
    assert get_minio_client().get_object(Bucket=MINIO_BUCKET, Key=f"{s3_store}{storage_key}")["Body"].read() == model
    fetched = fetch_artifact(backend, name, None, namespace_id)
    try:
        assert fetched.kind == "blob"
        assert _exported(backend, fetched, tmp_path) == model
    finally:
        backend.release(fetched)
