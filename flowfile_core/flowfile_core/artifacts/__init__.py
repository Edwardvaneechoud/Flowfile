"""Global Artifacts service layer.

Public interface:

* ``ArtifactService`` — business-logic orchestrator
* ``get_storage_backend()`` — factory for storage backend
* ``router`` — FastAPI router for artifact endpoints
* Domain exceptions (``ArtifactError`` hierarchy)
"""

import os
import threading
from typing import TYPE_CHECKING

from .exceptions import (
    ArtifactError,
    ArtifactIntegrityError,
    ArtifactNotActiveError,
    ArtifactNotFoundError,
    ArtifactUploadError,
    NamespaceNotFoundError,
    StorageError,
)
from .routes import router
from .service import ArtifactService

if TYPE_CHECKING:
    from shared.artifact_storage import ArtifactStorageBackend

# Module-level singleton for storage backend (thread-safe)
_backend: "ArtifactStorageBackend | None" = None
_backend_lock = threading.Lock()


def get_storage_backend() -> "ArtifactStorageBackend":
    """Factory function to get the configured storage backend.

    Returns a singleton instance based on environment configuration:
    - FLOWFILE_ARTIFACT_STORAGE=filesystem (default): SharedFilesystemStorage
    - FLOWFILE_ARTIFACT_STORAGE=s3: S3Storage

    For S3, requires additional environment variables:
    - FLOWFILE_S3_BUCKET: S3 bucket name (required)
    - FLOWFILE_S3_PREFIX: Key prefix (default: "global_artifacts/")
    - FLOWFILE_S3_REGION: AWS region (default: "us-east-1")
    - FLOWFILE_S3_ENDPOINT_URL: Custom endpoint for MinIO, etc. (optional)

    Thread-safe: Uses double-checked locking to ensure only one instance
    is created even under concurrent access.
    """
    global _backend

    # Fast path - already initialized
    if _backend is not None:
        return _backend

    # Slow path - initialize with lock
    with _backend_lock:
        # Double-check after acquiring lock
        if _backend is not None:
            return _backend

        backend_type = os.environ.get("FLOWFILE_ARTIFACT_STORAGE", "filesystem")

        if backend_type == "s3":
            from shared.artifact_storage import S3Storage

            bucket = os.environ.get("FLOWFILE_S3_BUCKET")
            if not bucket:
                raise ValueError(
                    "FLOWFILE_S3_BUCKET environment variable is required for S3 storage"
                )

            _backend = S3Storage(
                bucket=bucket,
                prefix=os.environ.get("FLOWFILE_S3_PREFIX", "global_artifacts/"),
                region=os.environ.get("FLOWFILE_S3_REGION", "us-east-1"),
                endpoint_url=os.environ.get("FLOWFILE_S3_ENDPOINT_URL"),
            )
        else:
            from shared.artifact_storage import SharedFilesystemStorage
            from shared.storage_config import storage

            _backend = SharedFilesystemStorage(
                staging_root=storage.artifact_staging_directory,
                artifacts_root=storage.global_artifacts_directory,
            )

    return _backend


def reset_storage_backend() -> None:
    """Reset the storage backend singleton (for testing)."""
    global _backend
    with _backend_lock:
        _backend = None


__all__ = [
    # Service
    "ArtifactService",
    # Router
    "router",
    # Factory
    "get_storage_backend",
    "reset_storage_backend",
    # Exceptions
    "ArtifactError",
    "ArtifactNotFoundError",
    "ArtifactNotActiveError",
    "ArtifactUploadError",
    "ArtifactIntegrityError",
    "StorageError",
    "NamespaceNotFoundError",
]
