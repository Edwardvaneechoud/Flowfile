"""Storage backend abstraction for global artifacts.

Provides a common interface for storing artifact blobs, with two implementations:
- SharedFilesystemStorage: for local/Docker deployments where kernel and Core share a volume
- S3Storage: for cloud deployments with S3-compatible storage
"""

import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path


@dataclass
class UploadTarget:
    """Returned by prepare_upload - tells kernel where to write."""
    method: str          # "file" or "s3_presigned"
    path: str            # Local path OR presigned URL
    storage_key: str     # Unique key for this blob


@dataclass
class DownloadSource:
    """Returned by prepare_download - tells kernel where to read."""
    method: str          # "file" or "s3_presigned"
    path: str            # Local path OR presigned URL


class ArtifactStorageBackend(ABC):
    """Abstract interface for artifact blob storage."""

    @abstractmethod
    def prepare_upload(self, artifact_id: int, filename: str) -> UploadTarget:
        """Generate upload target for kernel. Does not write any data."""

    @abstractmethod
    def finalize_upload(self, storage_key: str, expected_sha256: str) -> int:
        """Verify upload completed successfully.

        Returns size_bytes.
        Raises ValueError if SHA-256 mismatch.
        Raises FileNotFoundError if blob not found.
        """

    @abstractmethod
    def prepare_download(self, storage_key: str) -> DownloadSource:
        """Generate download source for kernel."""

    @abstractmethod
    def delete(self, storage_key: str) -> None:
        """Remove blob from storage."""

    @abstractmethod
    def exists(self, storage_key: str) -> bool:
        """Check if blob exists."""


def _compute_sha256(path: Path) -> str:
    """Compute SHA-256 hash of a file using streaming reads."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


class SharedFilesystemStorage(ArtifactStorageBackend):
    """For local/Docker deployments where kernel and Core share a volume.

    Layout:
        <shared_root>/artifact_staging/     <- kernel writes here (temp)
        <artifacts_root>/<id>/<filename>    <- permanent storage
    """

    def __init__(self, shared_root: Path, artifacts_root: Path):
        self.staging = shared_root / "artifact_staging"
        self.permanent = artifacts_root
        self.staging.mkdir(parents=True, exist_ok=True)
        self.permanent.mkdir(parents=True, exist_ok=True)

    def prepare_upload(self, artifact_id: int, filename: str) -> UploadTarget:
        staging_path = self.staging / f"{artifact_id}_{filename}"
        storage_key = f"{artifact_id}/{filename}"

        return UploadTarget(
            method="file",
            path=str(staging_path),
            storage_key=storage_key,
        )

    def finalize_upload(self, storage_key: str, expected_sha256: str) -> int:
        artifact_id, filename = storage_key.split("/", 1)
        staging_path = self.staging / f"{artifact_id}_{filename}"

        if not staging_path.exists():
            raise FileNotFoundError(f"Staged file not found: {staging_path}")

        actual_sha256 = _compute_sha256(staging_path)
        if actual_sha256 != expected_sha256:
            staging_path.unlink()
            raise ValueError(
                f"SHA-256 mismatch: expected {expected_sha256}, got {actual_sha256}"
            )

        final_path = self.permanent / storage_key
        final_path.parent.mkdir(parents=True, exist_ok=True)
        staging_path.rename(final_path)

        return final_path.stat().st_size

    def prepare_download(self, storage_key: str) -> DownloadSource:
        return DownloadSource(
            method="file",
            path=str(self.permanent / storage_key),
        )

    def delete(self, storage_key: str) -> None:
        path = self.permanent / storage_key
        if path.exists():
            path.unlink()
            try:
                path.parent.rmdir()
            except OSError:
                pass

    def exists(self, storage_key: str) -> bool:
        return (self.permanent / storage_key).exists()


class S3Storage(ArtifactStorageBackend):
    """For cloud deployments with S3-compatible storage.

    Kernel uploads/downloads directly via presigned URLs.
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "global_artifacts/",
        region: str = "us-east-1",
        endpoint_url: str | None = None,
    ):
        import boto3
        self.bucket = bucket
        self.prefix = prefix
        self.client = boto3.client(
            "s3",
            region_name=region,
            endpoint_url=endpoint_url,
        )

    def prepare_upload(self, artifact_id: int, filename: str) -> UploadTarget:
        storage_key = f"{artifact_id}/{filename}"
        s3_key = f"{self.prefix}{storage_key}"

        presigned_url = self.client.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": self.bucket,
                "Key": s3_key,
                "ChecksumAlgorithm": "SHA256",
            },
            ExpiresIn=3600,
        )

        return UploadTarget(
            method="s3_presigned",
            path=presigned_url,
            storage_key=storage_key,
        )

    def finalize_upload(self, storage_key: str, expected_sha256: str) -> int:
        s3_key = f"{self.prefix}{storage_key}"

        try:
            head = self.client.head_object(
                Bucket=self.bucket,
                Key=s3_key,
                ChecksumMode="ENABLED",
            )
        except self.client.exceptions.NoSuchKey:
            raise FileNotFoundError(f"S3 object not found: {s3_key}")

        if "ChecksumSHA256" in head:
            import base64
            s3_sha256 = base64.b64decode(head["ChecksumSHA256"]).hex()
            if s3_sha256 != expected_sha256:
                self.delete(storage_key)
                raise ValueError(f"SHA-256 mismatch")

        return head["ContentLength"]

    def prepare_download(self, storage_key: str) -> DownloadSource:
        s3_key = f"{self.prefix}{storage_key}"

        presigned_url = self.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": s3_key},
            ExpiresIn=3600,
        )

        return DownloadSource(
            method="s3_presigned",
            path=presigned_url,
        )

    def delete(self, storage_key: str) -> None:
        s3_key = f"{self.prefix}{storage_key}"
        self.client.delete_object(Bucket=self.bucket, Key=s3_key)

    def exists(self, storage_key: str) -> bool:
        s3_key = f"{self.prefix}{storage_key}"
        try:
            self.client.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except Exception:
            return False
