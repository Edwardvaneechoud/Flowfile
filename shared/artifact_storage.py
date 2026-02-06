"""Storage backend abstraction for global artifacts.

This module provides a common interface for artifact blob storage, with
implementations for shared filesystem (local/Docker) and S3-compatible storage.

The Core API never handles blob data directly - all binary data flows between
kernel and storage backend. Core only manages metadata.
"""

import hashlib
import logging
import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class UploadTarget:
    """Returned by prepare_upload - tells kernel where to write.

    Attributes:
        method: Storage method - "file" for local filesystem or "s3_presigned" for S3.
        path: Local filesystem path OR presigned URL for upload.
        storage_key: Unique key identifying this blob in storage (e.g., "42/model.joblib").
    """

    method: str
    path: str
    storage_key: str


@dataclass
class DownloadSource:
    """Returned by prepare_download - tells kernel where to read.

    Attributes:
        method: Storage method - "file" for local filesystem or "s3_presigned" for S3.
        path: Local filesystem path OR presigned URL for download.
    """

    method: str
    path: str


class ArtifactStorageBackend(ABC):
    """Abstract interface for artifact blob storage.

    Implementations must handle:
    - Preparing upload targets (where kernel writes)
    - Finalizing uploads (verify integrity, move to permanent storage)
    - Preparing download sources (where kernel reads)
    - Deleting blobs
    - Checking blob existence
    """

    @abstractmethod
    def prepare_upload(self, artifact_id: int, filename: str) -> UploadTarget:
        """Generate upload target for kernel. Does not write any data.

        Args:
            artifact_id: Database ID of the artifact being uploaded.
            filename: Name for the stored file (e.g., "model.joblib").

        Returns:
            UploadTarget with method, path, and storage_key.
        """
        pass

    @abstractmethod
    def finalize_upload(self, storage_key: str, expected_sha256: str) -> int:
        """Verify upload completed successfully and move to permanent storage.

        Args:
            storage_key: The storage key returned from prepare_upload.
            expected_sha256: SHA-256 hash provided by the kernel.

        Returns:
            Size in bytes of the uploaded file.

        Raises:
            FileNotFoundError: If the staged file doesn't exist.
            ValueError: If SHA-256 verification fails.
        """
        pass

    @abstractmethod
    def prepare_download(self, storage_key: str) -> DownloadSource:
        """Generate download source for kernel.

        Args:
            storage_key: The storage key for the artifact blob.

        Returns:
            DownloadSource with method and path.
        """
        pass

    @abstractmethod
    def delete(self, storage_key: str) -> None:
        """Remove blob from storage.

        Args:
            storage_key: The storage key for the artifact blob.
        """
        pass

    @abstractmethod
    def exists(self, storage_key: str) -> bool:
        """Check if blob exists in storage.

        Args:
            storage_key: The storage key for the artifact blob.

        Returns:
            True if blob exists, False otherwise.
        """
        pass


class SharedFilesystemStorage(ArtifactStorageBackend):
    """Storage backend for local/Docker deployments with shared filesystem.

    Kernel and Core share a volume at /shared (container) <-> ~/.flowfile/shared (host).

    Layout:
        <staging_root>/                     <- kernel writes here (temp)
        <artifacts_root>/<id>/<filename>    <- permanent storage
    """

    def __init__(self, staging_root: Path, artifacts_root: Path):
        """Initialize filesystem storage backend.

        Args:
            staging_root: Directory for temporary uploads (shared with kernel).
            artifacts_root: Directory for permanent artifact storage.
        """
        self.staging = Path(staging_root)
        self.permanent = Path(artifacts_root)
        self.staging.mkdir(parents=True, exist_ok=True)
        self.permanent.mkdir(parents=True, exist_ok=True)
        logger.info(
            "[artifact_storage] SharedFilesystemStorage initialized: "
            "staging=%s (exists=%s), permanent=%s (exists=%s)",
            self.staging,
            self.staging.exists(),
            self.permanent,
            self.permanent.exists(),
        )

    def prepare_upload(self, artifact_id: int, filename: str) -> UploadTarget:
        """Prepare a local filesystem path for the kernel to write to."""
        staging_path = self.staging / f"{artifact_id}_{filename}"
        storage_key = f"{artifact_id}/{filename}"
        logger.info(
            "[artifact_storage] prepare_upload: artifact_id=%s, filename='%s', " "staging_path='%s', storage_key='%s'",
            artifact_id,
            filename,
            staging_path,
            storage_key,
        )

        return UploadTarget(
            method="file",
            path=str(staging_path),
            storage_key=storage_key,
        )

    def finalize_upload(self, storage_key: str, expected_sha256: str) -> int:
        """Verify SHA-256 and move staged file to permanent storage."""
        artifact_id, filename = storage_key.split("/", 1)
        staging_path = self.staging / f"{artifact_id}_{filename}"

        logger.info(
            "[artifact_storage] finalize_upload: storage_key='%s', " "staging_path='%s', exists=%s",
            storage_key,
            staging_path,
            staging_path.exists(),
        )
        # List staging directory contents to see what's actually there
        try:
            contents = list(self.staging.iterdir())
            logger.info(
                "[artifact_storage] finalize_upload: staging dir '%s' contains %d files: %s",
                self.staging,
                len(contents),
                [str(f.name) for f in contents],
            )
        except Exception as e:
            logger.error("[artifact_storage] finalize_upload: cannot list staging dir: %s", e)

        if not staging_path.exists():
            raise FileNotFoundError(f"Staged file not found: {staging_path}")

        # Verify integrity
        actual_sha256 = self._compute_sha256(staging_path)
        if actual_sha256 != expected_sha256:
            staging_path.unlink()  # Clean up failed upload
            raise ValueError(f"SHA-256 mismatch: expected {expected_sha256}, got {actual_sha256}")

        # Move to permanent location
        # Use rename for atomicity when on same filesystem, fall back to
        # shutil.move for cross-filesystem moves (e.g., Docker with multiple volumes)
        final_path = self.permanent / storage_key
        final_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            staging_path.rename(final_path)
        except OSError:
            # Cross-filesystem move - not atomic but handles different mounts
            shutil.move(str(staging_path), str(final_path))

        return final_path.stat().st_size

    def prepare_download(self, storage_key: str) -> DownloadSource:
        """Return the permanent storage path for download."""
        return DownloadSource(
            method="file",
            path=str(self.permanent / storage_key),
        )

    def delete(self, storage_key: str) -> None:
        """Delete blob from permanent storage."""
        path = self.permanent / storage_key
        if path.exists():
            path.unlink()
            # Remove parent directory if empty
            try:
                path.parent.rmdir()
            except OSError:
                pass  # Directory not empty or doesn't exist

    def exists(self, storage_key: str) -> bool:
        """Check if blob exists in permanent storage."""
        return (self.permanent / storage_key).exists()

    def _compute_sha256(self, path: Path) -> str:
        """Compute SHA-256 hash of a file using streaming to handle large files."""
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):  # 8MB chunks
                h.update(chunk)
        return h.hexdigest()


class S3Storage(ArtifactStorageBackend):
    """Storage backend for cloud deployments with S3-compatible storage.

    Kernel uploads/downloads directly via presigned URLs, keeping Core lightweight.
    Supports AWS S3, MinIO, and other S3-compatible services.
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "global_artifacts/",
        region: str = "us-east-1",
        endpoint_url: str | None = None,
    ):
        """Initialize S3 storage backend.

        Args:
            bucket: S3 bucket name.
            prefix: Key prefix for all artifacts (default: "global_artifacts/").
            region: AWS region (default: "us-east-1").
            endpoint_url: Custom endpoint URL for S3-compatible services (e.g., MinIO).
        """
        try:
            import boto3
        except ImportError:
            raise ImportError("boto3 is required for S3 storage backend. " "Install with: pip install boto3")

        self.bucket = bucket
        self.prefix = prefix
        self.client = boto3.client(
            "s3",
            region_name=region,
            endpoint_url=endpoint_url,
        )

    def prepare_upload(self, artifact_id: int, filename: str) -> UploadTarget:
        """Generate a presigned URL for the kernel to upload directly to S3."""
        storage_key = f"{artifact_id}/{filename}"
        s3_key = f"{self.prefix}{storage_key}"

        # Generate presigned URL for PUT (valid 1 hour)
        presigned_url = self.client.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": self.bucket,
                "Key": s3_key,
            },
            ExpiresIn=3600,
        )

        return UploadTarget(
            method="s3_presigned",
            path=presigned_url,
            storage_key=storage_key,
        )

    def finalize_upload(self, storage_key: str, expected_sha256: str) -> int:
        """Verify the upload exists in S3 and return its size.

        WARNING: This currently only checks existence, not integrity.
        We trust the kernel's SHA-256 hash without verification.

        TODO: For production integrity guarantees, either:
        1. Use S3's ChecksumSHA256 feature (requires SDK support on upload)
        2. Download the object and verify hash (adds latency/egress cost)
        3. Use S3 Object Lock for immutability guarantees
        """
        s3_key = f"{self.prefix}{storage_key}"

        try:
            head = self.client.head_object(
                Bucket=self.bucket,
                Key=s3_key,
            )
        except self.client.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                raise FileNotFoundError(f"S3 object not found: {s3_key}")
            raise

        return head["ContentLength"]

    def prepare_download(self, storage_key: str) -> DownloadSource:
        """Generate a presigned URL for the kernel to download from S3."""
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
        """Delete object from S3."""
        s3_key = f"{self.prefix}{storage_key}"
        self.client.delete_object(Bucket=self.bucket, Key=s3_key)

    def exists(self, storage_key: str) -> bool:
        """Check if object exists in S3."""
        s3_key = f"{self.prefix}{storage_key}"
        try:
            self.client.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except self.client.exceptions.ClientError:
            return False
