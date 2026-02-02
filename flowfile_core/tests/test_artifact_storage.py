"""Tests for the SharedFilesystemStorage backend."""

import hashlib
import tempfile
from pathlib import Path

import pytest

from shared.artifact_storage import SharedFilesystemStorage, _compute_sha256


class TestSharedFilesystemStorage:
    @pytest.fixture()
    def storage(self):
        with tempfile.TemporaryDirectory() as shared, tempfile.TemporaryDirectory() as artifacts:
            yield SharedFilesystemStorage(
                shared_root=Path(shared),
                artifacts_root=Path(artifacts),
            )

    def test_prepare_upload_returns_file_target(self, storage):
        target = storage.prepare_upload(1, "model.pkl")
        assert target.method == "file"
        assert target.storage_key == "1/model.pkl"
        assert "artifact_staging" in target.path

    def test_full_upload_finalize_cycle(self, storage):
        # Prepare
        target = storage.prepare_upload(42, "data.parquet")

        # Simulate kernel writing blob
        blob = b"parquet binary data here"
        Path(target.path).parent.mkdir(parents=True, exist_ok=True)
        Path(target.path).write_bytes(blob)

        # Compute expected hash
        expected_sha256 = hashlib.sha256(blob).hexdigest()

        # Finalize
        size = storage.finalize_upload(target.storage_key, expected_sha256)
        assert size == len(blob)

        # Blob moved to permanent location
        assert not Path(target.path).exists()  # staging cleaned
        assert storage.exists(target.storage_key)

    def test_finalize_rejects_sha256_mismatch(self, storage):
        target = storage.prepare_upload(1, "bad.pkl")
        Path(target.path).write_bytes(b"real data")

        with pytest.raises(ValueError, match="SHA-256 mismatch"):
            storage.finalize_upload(target.storage_key, "wrong_hash")

        # Staging file should be cleaned up
        assert not Path(target.path).exists()

    def test_finalize_raises_on_missing_file(self, storage):
        target = storage.prepare_upload(1, "missing.pkl")
        # Don't write anything

        with pytest.raises(FileNotFoundError):
            storage.finalize_upload(target.storage_key, "abc")

    def test_prepare_download(self, storage):
        # Upload first
        target = storage.prepare_upload(1, "model.pkl")
        blob = b"model bytes"
        Path(target.path).write_bytes(blob)
        sha256 = hashlib.sha256(blob).hexdigest()
        storage.finalize_upload(target.storage_key, sha256)

        # Prepare download
        source = storage.prepare_download(target.storage_key)
        assert source.method == "file"
        assert Path(source.path).read_bytes() == blob

    def test_delete(self, storage):
        # Upload first
        target = storage.prepare_upload(1, "deleteme.pkl")
        blob = b"soon to be deleted"
        Path(target.path).write_bytes(blob)
        sha256 = hashlib.sha256(blob).hexdigest()
        storage.finalize_upload(target.storage_key, sha256)

        assert storage.exists(target.storage_key)
        storage.delete(target.storage_key)
        assert not storage.exists(target.storage_key)

    def test_delete_nonexistent_is_noop(self, storage):
        # Should not raise
        storage.delete("999/nonexistent.pkl")

    def test_exists_false_for_missing(self, storage):
        assert not storage.exists("1/missing.pkl")

    def test_staging_directory_created(self, storage):
        assert storage.staging.exists()

    def test_permanent_directory_created(self, storage):
        assert storage.permanent.exists()


class TestComputeSha256:
    def test_matches_hashlib(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "test.bin"
            data = b"hello world" * 1000
            path.write_bytes(data)

            expected = hashlib.sha256(data).hexdigest()
            assert _compute_sha256(path) == expected

    def test_large_file_streaming(self):
        """Ensure streaming works for files larger than chunk size."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "large.bin"
            # Write ~16MB (larger than the 8MB chunk size)
            chunk = b"x" * (1024 * 1024)
            with open(path, "wb") as f:
                for _ in range(16):
                    f.write(chunk)

            result = _compute_sha256(path)
            assert len(result) == 64  # Valid hex SHA-256
