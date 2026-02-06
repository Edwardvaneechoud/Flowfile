"""Tests for the artifact storage backend.

Covers:
- SharedFilesystemStorage operations
- Upload preparation and finalization
- Download source generation
- Deletion
- SHA-256 verification
"""

import hashlib
from pathlib import Path

import pytest

from shared.artifact_storage import (
    ArtifactStorageBackend,
    DownloadSource,
    SharedFilesystemStorage,
    UploadTarget,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def storage(tmp_path) -> SharedFilesystemStorage:
    """Create a SharedFilesystemStorage with temp directories."""
    staging = tmp_path / "staging"
    artifacts = tmp_path / "artifacts"
    return SharedFilesystemStorage(staging, artifacts)


@pytest.fixture
def sample_data() -> bytes:
    """Sample data for testing."""
    return b"This is sample artifact data for testing purposes."


@pytest.fixture
def sample_sha256(sample_data) -> str:
    """SHA-256 of sample data."""
    return hashlib.sha256(sample_data).hexdigest()


# ---------------------------------------------------------------------------
# Upload Target Tests
# ---------------------------------------------------------------------------


class TestPrepareUpload:
    """Tests for prepare_upload functionality."""

    def test_returns_upload_target(self, storage):
        """Should return an UploadTarget with correct fields."""
        target = storage.prepare_upload(artifact_id=1, filename="model.pkl")

        assert isinstance(target, UploadTarget)
        assert target.method == "file"
        assert "1_model.pkl" in target.path
        assert target.storage_key == "1/model.pkl"

    def test_staging_directory_created(self, storage, tmp_path):
        """Should create staging directory if it doesn't exist."""
        # Storage creates directories in __init__
        assert (tmp_path / "staging").exists()

    def test_different_artifacts_get_different_paths(self, storage):
        """Different artifact IDs should get different paths."""
        target1 = storage.prepare_upload(artifact_id=1, filename="model.pkl")
        target2 = storage.prepare_upload(artifact_id=2, filename="model.pkl")

        assert target1.path != target2.path
        assert target1.storage_key != target2.storage_key

    def test_storage_key_format(self, storage):
        """Storage key should be in format 'id/filename'."""
        target = storage.prepare_upload(artifact_id=42, filename="data.parquet")

        assert target.storage_key == "42/data.parquet"


# ---------------------------------------------------------------------------
# Finalize Upload Tests
# ---------------------------------------------------------------------------


class TestFinalizeUpload:
    """Tests for finalize_upload functionality."""

    def test_moves_file_to_permanent_location(
        self, storage, tmp_path, sample_data, sample_sha256
    ):
        """Should move file from staging to permanent storage."""
        target = storage.prepare_upload(artifact_id=1, filename="test.pkl")

        # Write data to staging location
        Path(target.path).write_bytes(sample_data)

        # Finalize
        size = storage.finalize_upload(target.storage_key, sample_sha256)

        # Staging file should be gone
        assert not Path(target.path).exists()

        # Permanent file should exist
        permanent_path = tmp_path / "artifacts" / "1" / "test.pkl"
        assert permanent_path.exists()
        assert permanent_path.read_bytes() == sample_data

        # Size should be correct
        assert size == len(sample_data)

    def test_sha256_verification_success(self, storage, sample_data, sample_sha256):
        """Should succeed when SHA-256 matches."""
        target = storage.prepare_upload(artifact_id=1, filename="test.pkl")
        Path(target.path).write_bytes(sample_data)

        # Should not raise
        size = storage.finalize_upload(target.storage_key, sample_sha256)
        assert size == len(sample_data)

    def test_sha256_verification_failure(self, storage, sample_data):
        """Should raise ValueError when SHA-256 doesn't match."""
        target = storage.prepare_upload(artifact_id=1, filename="test.pkl")
        Path(target.path).write_bytes(sample_data)

        wrong_sha256 = "0" * 64

        with pytest.raises(ValueError, match="SHA-256 mismatch"):
            storage.finalize_upload(target.storage_key, wrong_sha256)

        # Staging file should be cleaned up
        assert not Path(target.path).exists()

    def test_file_not_found(self, storage):
        """Should raise FileNotFoundError when staging file doesn't exist."""
        with pytest.raises(FileNotFoundError):
            storage.finalize_upload("999/nonexistent.pkl", "abc123")

    def test_creates_artifact_subdirectory(self, storage, tmp_path, sample_data, sample_sha256):
        """Should create artifact subdirectory in permanent storage."""
        target = storage.prepare_upload(artifact_id=123, filename="model.pkl")
        Path(target.path).write_bytes(sample_data)

        storage.finalize_upload(target.storage_key, sample_sha256)

        artifact_dir = tmp_path / "artifacts" / "123"
        assert artifact_dir.exists()
        assert artifact_dir.is_dir()


# ---------------------------------------------------------------------------
# Download Source Tests
# ---------------------------------------------------------------------------


class TestPrepareDownload:
    """Tests for prepare_download functionality."""

    def test_returns_download_source(self, storage, tmp_path, sample_data, sample_sha256):
        """Should return a DownloadSource with correct path."""
        # First upload an artifact
        target = storage.prepare_upload(artifact_id=1, filename="test.pkl")
        Path(target.path).write_bytes(sample_data)
        storage.finalize_upload(target.storage_key, sample_sha256)

        # Get download source
        source = storage.prepare_download(target.storage_key)

        assert isinstance(source, DownloadSource)
        assert source.method == "file"
        assert "1/test.pkl" in source.path or "1\\test.pkl" in source.path

    def test_download_path_exists(self, storage, sample_data, sample_sha256):
        """Download path should point to existing file."""
        target = storage.prepare_upload(artifact_id=1, filename="test.pkl")
        Path(target.path).write_bytes(sample_data)
        storage.finalize_upload(target.storage_key, sample_sha256)

        source = storage.prepare_download(target.storage_key)

        assert Path(source.path).exists()
        assert Path(source.path).read_bytes() == sample_data


# ---------------------------------------------------------------------------
# Delete Tests
# ---------------------------------------------------------------------------


class TestDelete:
    """Tests for delete functionality."""

    def test_deletes_file(self, storage, sample_data, sample_sha256):
        """Should delete the artifact file."""
        target = storage.prepare_upload(artifact_id=1, filename="test.pkl")
        Path(target.path).write_bytes(sample_data)
        storage.finalize_upload(target.storage_key, sample_sha256)

        source = storage.prepare_download(target.storage_key)
        assert Path(source.path).exists()

        storage.delete(target.storage_key)

        assert not Path(source.path).exists()

    def test_delete_nonexistent_is_idempotent(self, storage):
        """Deleting nonexistent file should not raise."""
        # Should not raise
        storage.delete("999/nonexistent.pkl")

    def test_removes_empty_parent_directory(self, storage, tmp_path, sample_data, sample_sha256):
        """Should remove empty artifact directory after deletion."""
        target = storage.prepare_upload(artifact_id=1, filename="test.pkl")
        Path(target.path).write_bytes(sample_data)
        storage.finalize_upload(target.storage_key, sample_sha256)

        artifact_dir = tmp_path / "artifacts" / "1"
        assert artifact_dir.exists()

        storage.delete(target.storage_key)

        # Directory should be removed if empty
        assert not artifact_dir.exists()

    def test_preserves_directory_with_other_files(
        self, storage, tmp_path, sample_data, sample_sha256
    ):
        """Should preserve artifact directory if other files exist."""
        # Create two artifacts in same directory
        target1 = storage.prepare_upload(artifact_id=1, filename="file1.pkl")
        target2 = storage.prepare_upload(artifact_id=1, filename="file2.pkl")

        Path(target1.path).write_bytes(sample_data)
        Path(target2.path).write_bytes(sample_data)

        storage.finalize_upload(target1.storage_key, sample_sha256)
        storage.finalize_upload(target2.storage_key, sample_sha256)

        # Delete first file
        storage.delete(target1.storage_key)

        # Directory should still exist (has file2)
        artifact_dir = tmp_path / "artifacts" / "1"
        assert artifact_dir.exists()

        # file2 should still exist
        assert (artifact_dir / "file2.pkl").exists()


# ---------------------------------------------------------------------------
# Exists Tests
# ---------------------------------------------------------------------------


class TestExists:
    """Tests for exists functionality."""

    def test_exists_returns_true_for_existing(self, storage, sample_data, sample_sha256):
        """Should return True for existing artifact."""
        target = storage.prepare_upload(artifact_id=1, filename="test.pkl")
        Path(target.path).write_bytes(sample_data)
        storage.finalize_upload(target.storage_key, sample_sha256)

        assert storage.exists(target.storage_key) is True

    def test_exists_returns_false_for_nonexistent(self, storage):
        """Should return False for nonexistent artifact."""
        assert storage.exists("999/nonexistent.pkl") is False

    def test_exists_returns_false_after_delete(self, storage, sample_data, sample_sha256):
        """Should return False after deletion."""
        target = storage.prepare_upload(artifact_id=1, filename="test.pkl")
        Path(target.path).write_bytes(sample_data)
        storage.finalize_upload(target.storage_key, sample_sha256)

        assert storage.exists(target.storage_key) is True

        storage.delete(target.storage_key)

        assert storage.exists(target.storage_key) is False


# ---------------------------------------------------------------------------
# SHA-256 Computation Tests
# ---------------------------------------------------------------------------


class TestSHA256Computation:
    """Tests for internal SHA-256 computation."""

    def test_computes_correct_sha256(self, storage, tmp_path):
        """Should compute correct SHA-256 hash."""
        test_data = b"Hello, World!"
        expected_sha256 = "dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f"

        path = tmp_path / "test.bin"
        path.write_bytes(test_data)

        result = storage._compute_sha256(path)

        assert result == expected_sha256

    def test_handles_large_files(self, storage, tmp_path):
        """Should handle large files efficiently using chunked reading."""
        # Create a 5MB file
        large_data = b"x" * (5 * 1024 * 1024)
        expected_sha256 = hashlib.sha256(large_data).hexdigest()

        path = tmp_path / "large.bin"
        path.write_bytes(large_data)

        result = storage._compute_sha256(path)

        assert result == expected_sha256


# ---------------------------------------------------------------------------
# Integration Tests
# ---------------------------------------------------------------------------


class TestFullWorkflow:
    """Integration tests for complete upload/download workflow."""

    def test_upload_download_roundtrip(self, storage, sample_data, sample_sha256):
        """Complete upload and download should preserve data."""
        # Upload
        target = storage.prepare_upload(artifact_id=1, filename="roundtrip.pkl")
        Path(target.path).write_bytes(sample_data)
        storage.finalize_upload(target.storage_key, sample_sha256)

        # Download
        source = storage.prepare_download(target.storage_key)
        downloaded_data = Path(source.path).read_bytes()

        assert downloaded_data == sample_data

    def test_multiple_versions_same_name(self, storage):
        """Should support multiple versions of same logical artifact."""
        data_v1 = b"version 1 data"
        data_v2 = b"version 2 data"
        sha_v1 = hashlib.sha256(data_v1).hexdigest()
        sha_v2 = hashlib.sha256(data_v2).hexdigest()

        # Upload v1 (artifact_id=1)
        target1 = storage.prepare_upload(artifact_id=1, filename="model.pkl")
        Path(target1.path).write_bytes(data_v1)
        storage.finalize_upload(target1.storage_key, sha_v1)

        # Upload v2 (artifact_id=2)
        target2 = storage.prepare_upload(artifact_id=2, filename="model.pkl")
        Path(target2.path).write_bytes(data_v2)
        storage.finalize_upload(target2.storage_key, sha_v2)

        # Both should be retrievable
        source1 = storage.prepare_download(target1.storage_key)
        source2 = storage.prepare_download(target2.storage_key)

        assert Path(source1.path).read_bytes() == data_v1
        assert Path(source2.path).read_bytes() == data_v2

    def test_concurrent_uploads(self, storage):
        """Should handle concurrent uploads without interference."""
        import concurrent.futures

        def upload_artifact(artifact_id: int) -> bool:
            try:
                data = f"artifact {artifact_id} data".encode()
                sha256 = hashlib.sha256(data).hexdigest()

                target = storage.prepare_upload(artifact_id, "test.pkl")
                Path(target.path).write_bytes(data)
                storage.finalize_upload(target.storage_key, sha256)

                # Verify
                source = storage.prepare_download(target.storage_key)
                return Path(source.path).read_bytes() == data
            except Exception:
                return False

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(upload_artifact, i) for i in range(10)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        assert all(results)


# ---------------------------------------------------------------------------
# Error Handling Tests
# ---------------------------------------------------------------------------


class TestErrorHandling:
    """Tests for error handling scenarios."""

    def test_invalid_storage_key_format(self, storage, tmp_path, sample_data, sample_sha256):
        """Should handle unusual storage key formats gracefully."""
        # Create a valid artifact first
        target = storage.prepare_upload(artifact_id=1, filename="test.pkl")
        Path(target.path).write_bytes(sample_data)
        storage.finalize_upload(target.storage_key, sample_sha256)

        # These should not crash
        assert storage.exists("invalid") is False
        assert storage.exists("") is False

    def test_special_characters_in_filename(self, storage, sample_data, sample_sha256):
        """Should handle special characters in filename."""
        target = storage.prepare_upload(artifact_id=1, filename="model-v1.2_final.pkl")
        Path(target.path).write_bytes(sample_data)
        storage.finalize_upload(target.storage_key, sample_sha256)

        assert storage.exists(target.storage_key) is True
