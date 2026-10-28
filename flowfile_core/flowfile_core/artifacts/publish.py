"""Publish a result a backend holds to the artifact store, and fetch a published artifact onto a backend.

Core handles the catalog row and moves no bytes: the backend copies them to the store's upload target with ``export``
and from its download source with ``import_uri``. That target or source is a ``file://`` URI for the filesystem store
or a presigned ``https://`` URL for the S3 store, which carries its own authorisation, so neither takes a credential.
"""

import logging

from flowfile_core.artifacts import get_storage_backend
from flowfile_core.artifacts.service import ArtifactService
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.execution.protocol import ComputeBackend, ResultRef
from flowfile_core.schemas.artifact_schema import FinalizeUploadResponse, PrepareUploadRequest
from shared.compute_protocol import file_uri

logger = logging.getLogger(__name__)


def publish_result(
    backend: ComputeBackend,
    ref: ResultRef,
    request: PrepareUploadRequest,
    *,
    owner_id: int,
    sha256: str,
    size_bytes: int,
) -> FinalizeUploadResponse:
    """Publish the result ``ref`` that ``backend`` holds as the next version of the artifact ``request`` names.

    The version is finalised with the ``sha256`` and ``size_bytes`` the caller knows. A failed copy or finalise
    deletes the pending version before the error propagates. The caller decides who may publish where; this function
    checks no namespace.

    Raises:
        ArtifactUploadError: The store could not verify the bytes it was given.
        ComputeError: ``backend`` could not copy the result, ``ResultMissing`` when it no longer holds it.
        OSError: The copy failed; the message names no URL query.
    """
    storage = get_storage_backend()
    with get_db_context() as db:
        prepared = ArtifactService(db, storage).prepare_upload(request, owner_id=owner_id)
    try:
        backend.export(ref, _uri(prepared.method, prepared.path), None)
        with get_db_context() as db:
            return ArtifactService(db, storage).finalize_upload(
                artifact_id=prepared.artifact_id,
                storage_key=prepared.storage_key,
                sha256=sha256,
                size_bytes=size_bytes,
            )
    except Exception:
        with get_db_context() as db:
            try:
                ArtifactService(db, storage).delete_artifact(prepared.artifact_id)
            except Exception:
                logger.exception("Failed to roll back pending artifact %s", prepared.artifact_id)
        raise


def fetch_artifact(backend: ComputeBackend, name: str, version: int | None, namespace_id: int | None) -> ResultRef:
    """Hold the bytes of the artifact ``name`` on ``backend`` as a blob, and answer its reference.

    ``version`` ``None`` is the latest active version in ``namespace_id``.

    Raises:
        ArtifactNotFoundError: No active version matches; nothing is held.
        ValueError: The store gave no download source for the version; nothing is held.
        ComputeError: ``backend`` could not hold the bytes.
        OSError: The copy failed; the message names no URL query.
    """
    with get_db_context() as db:
        artifact = ArtifactService(db, get_storage_backend()).get_artifact_by_name(
            name=name, namespace_id=namespace_id, version=version
        )
    source = artifact.download_source
    if source is None:
        raise ValueError(f"The artifact store gave no download source for '{artifact.name}' v{artifact.version}")
    return backend.import_uri(_uri(source.method, source.path), None, None, None, kind="blob")


def _uri(method: str, path: str) -> str:
    """The URI a backend copies an artifact's bytes to or from: the store's path as ``file://``, or its presigned URL.

    The path is percent-encoded as ``Path.as_uri()`` writes it (``file_uri``), which ``put_blob`` and ``get_blob``
    read back.
    """
    if method == "file":
        return file_uri(path)
    if method == "s3_presigned":
        return path
    raise ValueError(f"Artifact storage method {method!r} is neither 'file' nor 's3_presigned'")
