"""Global Artifacts module - storage backend factory and configuration."""

import os

from shared.artifact_storage import (
    ArtifactStorageBackend,
    S3Storage,
    SharedFilesystemStorage,
)
from shared.storage_config import storage

_backend: ArtifactStorageBackend | None = None


def get_storage_backend() -> ArtifactStorageBackend:
    """Get or create the artifact storage backend based on environment config."""
    global _backend
    if _backend is None:
        backend_type = os.environ.get("FLOWFILE_ARTIFACT_STORAGE", "filesystem")

        if backend_type == "s3":
            _backend = S3Storage(
                bucket=os.environ["FLOWFILE_S3_BUCKET"],
                prefix=os.environ.get("FLOWFILE_S3_PREFIX", "global_artifacts/"),
                region=os.environ.get("FLOWFILE_S3_REGION", "us-east-1"),
                endpoint_url=os.environ.get("FLOWFILE_S3_ENDPOINT_URL"),
            )
        else:
            _backend = SharedFilesystemStorage(
                shared_root=storage.shared_directory,
                artifacts_root=storage.global_artifacts_directory,
            )

    return _backend
