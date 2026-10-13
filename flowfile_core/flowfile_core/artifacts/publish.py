"""Publish a result a backend holds to the artifact store, and fetch a published artifact onto a backend.

Any node that holds a result whose digest it knows publishes it with ``publish_result``, and any node that needs a
catalog artifact on its backend fetches it with ``fetch_artifact``. Core prepares, finalises and looks up the catalog
row and moves no bytes: the backend copies them to the store's upload target with ``export`` and from its download
source with ``import_uri``. A filesystem store's target or source is a ``file://`` URI, an S3 store's a presigned
``https://`` URL, which carries its own authorisation, so neither takes a credential.
"""

import logging

from flowfile_core.artifacts import get_storage_backend
from flowfile_core.artifacts.service import ArtifactService
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.execution.protocol import ComputeBackend, ResultRef
from flowfile_core.schemas.artifact_schema import FinalizeUploadResponse, PrepareUploadRequest

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

    The pending version is prepared for ``owner_id``, ``backend`` copies the result's bytes to the store's upload
    target, and the version is finalised with ``sha256`` and ``size_bytes``, the digest and size the caller knows. A
    copy or a finalise that fails deletes the pending version before its error propagates, so no version is left
    without its bytes. The caller decides who may publish where; this function checks no namespace.

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

    The version is ``version``, the latest active one when ``None``, looked up as ``ArtifactService`` looks up a name
    in ``namespace_id``. ``backend`` copies the bytes from the store's download source.

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

    The path is not quoted, as ``put_blob`` and ``get_blob`` read the text after ``file://`` as the path.
    """
    if method == "file":
        return "file://" + path
    if method == "s3_presigned":
        return path
    raise ValueError(f"Artifact storage method {method!r} is neither 'file' nor 's3_presigned'")
