"""Storage abstraction for persisted global artifacts.

Separates the binary blob from metadata management. The backend handles
only raw bytes — serialization happens in the kernel before upload and
the Core never needs to interpret the blob content.

Designed for extension: swap ``LocalArtifactStorage`` for an S3 or GCS
backend by implementing the same ``ArtifactStorageBackend`` interface.
"""

from __future__ import annotations

import hashlib
import os
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import BinaryIO, Iterator

# 8 MB default chunk size — keeps memory bounded for 1 GB+ uploads.
DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024


class ArtifactStorageBackend(ABC):
    """Interface for artifact blob storage.

    All operations work with a ``storage_key`` that uniquely identifies
    a blob.  The Core generates this key (typically ``<artifact_id>/<filename>``)
    and passes it down.
    """

    @abstractmethod
    def write(self, storage_key: str, stream: BinaryIO, content_length: int | None = None) -> int:
        """Write binary data from *stream* and return bytes written."""

    @abstractmethod
    def read_stream(self, storage_key: str, chunk_size: int = DEFAULT_CHUNK_SIZE) -> Iterator[bytes]:
        """Yield the stored blob in chunks (streaming read)."""

    @abstractmethod
    def delete(self, storage_key: str) -> None:
        """Remove the stored blob."""

    @abstractmethod
    def exists(self, storage_key: str) -> bool:
        """Return ``True`` if the blob exists."""

    @abstractmethod
    def size(self, storage_key: str) -> int:
        """Return size in bytes."""


class LocalArtifactStorage(ArtifactStorageBackend):
    """Stores blobs on local disk under a root directory.

    Layout::

        <root>/
          <artifact_id>/
            <filename>          ← the serialized blob
    """

    def __init__(self, root: Path) -> None:
        self._root = root
        self._root.mkdir(parents=True, exist_ok=True)

    @property
    def root(self) -> Path:
        return self._root

    def _resolve(self, storage_key: str) -> Path:
        # Prevent path traversal
        resolved = (self._root / storage_key).resolve()
        if not str(resolved).startswith(str(self._root.resolve())):
            raise ValueError(f"Invalid storage key: {storage_key}")
        return resolved

    def write(self, storage_key: str, stream: BinaryIO, content_length: int | None = None) -> int:
        path = self._resolve(storage_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        total = 0
        with open(path, "wb") as f:
            while True:
                chunk = stream.read(DEFAULT_CHUNK_SIZE)
                if not chunk:
                    break
                f.write(chunk)
                total += len(chunk)
        return total

    def read_stream(self, storage_key: str, chunk_size: int = DEFAULT_CHUNK_SIZE) -> Iterator[bytes]:
        path = self._resolve(storage_key)
        if not path.exists():
            raise FileNotFoundError(f"Artifact blob not found: {storage_key}")
        with open(path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    def delete(self, storage_key: str) -> None:
        path = self._resolve(storage_key)
        if path.exists():
            path.unlink()
            # Remove parent directory if empty
            try:
                path.parent.rmdir()
            except OSError:
                pass

    def exists(self, storage_key: str) -> bool:
        return self._resolve(storage_key).exists()

    def size(self, storage_key: str) -> int:
        path = self._resolve(storage_key)
        if not path.exists():
            raise FileNotFoundError(f"Artifact blob not found: {storage_key}")
        return path.stat().st_size


def compute_sha256(stream: BinaryIO, chunk_size: int = DEFAULT_CHUNK_SIZE) -> str:
    """Compute SHA-256 hex digest of a binary stream, resetting position to 0."""
    h = hashlib.sha256()
    while True:
        chunk = stream.read(chunk_size)
        if not chunk:
            break
        h.update(chunk)
    stream.seek(0)
    return h.hexdigest()
