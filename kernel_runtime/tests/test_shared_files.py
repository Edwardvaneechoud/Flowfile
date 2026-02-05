"""Tests for shared file I/O functions in flowfile_client."""

import os
from pathlib import Path

import pytest

from kernel_runtime import flowfile_client


@pytest.fixture(autouse=True)
def _set_shared_dir(tmp_dir: Path, monkeypatch: pytest.MonkeyPatch):
    """Point the shared directory to a temporary location for each test."""
    shared_dir = tmp_dir / "shared"
    shared_dir.mkdir()
    monkeypatch.setenv("FLOWFILE_SHARED_DIR", str(shared_dir))
    yield


class TestSharedPath:
    def test_shared_path_returns_correct_path(self, tmp_dir: Path):
        path = flowfile_client.shared_path("export.csv")
        expected = tmp_dir / "shared" / "user_files" / "export.csv"
        assert path == str(expected)

    def test_shared_path_creates_parent_directories(self, tmp_dir: Path):
        path = flowfile_client.shared_path("deep/nested/dir/file.txt")
        parent = Path(path).parent
        assert parent.exists()

    def test_shared_path_rejects_path_traversal(self):
        with pytest.raises(ValueError, match="escapes the shared directory"):
            flowfile_client.shared_path("../../../etc/passwd")

    def test_shared_path_rejects_absolute_traversal(self):
        with pytest.raises(ValueError, match="escapes the shared directory"):
            flowfile_client.shared_path("foo/../../..")

    def test_shared_path_handles_subdirectories(self, tmp_dir: Path):
        path = flowfile_client.shared_path("models/training_data.parquet")
        expected = tmp_dir / "shared" / "user_files" / "models" / "training_data.parquet"
        assert path == str(expected)


class TestWriteShared:
    def test_write_shared_text(self, tmp_dir: Path):
        path = flowfile_client.write_shared("hello.txt", "Hello, world!")
        assert os.path.exists(path)
        with open(path) as f:
            assert f.read() == "Hello, world!"

    def test_write_shared_bytes(self, tmp_dir: Path):
        data = b"\x00\x01\x02\x03"
        path = flowfile_client.write_shared("binary.bin", data)
        assert os.path.exists(path)
        with open(path, "rb") as f:
            assert f.read() == data

    def test_write_shared_returns_path(self, tmp_dir: Path):
        path = flowfile_client.write_shared("out.txt", "data")
        expected = tmp_dir / "shared" / "user_files" / "out.txt"
        assert path == str(expected)


class TestReadShared:
    def test_read_shared_text(self, tmp_dir: Path):
        flowfile_client.write_shared("data.txt", "some text content")
        content = flowfile_client.read_shared("data.txt")
        assert content == "some text content"

    def test_read_shared_binary(self, tmp_dir: Path):
        raw = b"\xff\xfe\xfd"
        flowfile_client.write_shared("data.bin", raw)
        content = flowfile_client.read_shared("data.bin", mode="rb")
        assert content == raw

    def test_read_shared_nonexistent_raises_file_not_found(self):
        with pytest.raises(FileNotFoundError, match="not found"):
            flowfile_client.read_shared("nonexistent.csv")


class TestListShared:
    def test_list_shared_empty_directory(self):
        result = flowfile_client.list_shared()
        assert result == []

    def test_list_shared_with_files(self):
        flowfile_client.write_shared("a.txt", "aaa")
        flowfile_client.write_shared("b.csv", "bbb")
        result = flowfile_client.list_shared()
        assert sorted(result) == ["a.txt", "b.csv"]

    def test_list_shared_subdirectory(self):
        flowfile_client.write_shared("sub/file1.txt", "one")
        flowfile_client.write_shared("sub/file2.txt", "two")
        result = flowfile_client.list_shared("sub")
        assert sorted(result) == ["sub/file1.txt", "sub/file2.txt"]

    def test_list_shared_nonexistent_subdirectory(self):
        result = flowfile_client.list_shared("does_not_exist")
        assert result == []

    def test_list_shared_rejects_path_traversal(self):
        with pytest.raises(ValueError, match="escapes the shared directory"):
            flowfile_client.list_shared("../../..")


class TestDeleteShared:
    def test_delete_shared(self):
        flowfile_client.write_shared("temp.txt", "temporary")
        flowfile_client.delete_shared("temp.txt")
        with pytest.raises(FileNotFoundError):
            flowfile_client.read_shared("temp.txt")

    def test_delete_shared_nonexistent_raises(self):
        with pytest.raises(FileNotFoundError, match="not found"):
            flowfile_client.delete_shared("ghost.txt")


class TestWriteReadRoundtrip:
    def test_write_read_roundtrip_csv(self, tmp_dir: Path):
        """Write CSV content and read it back."""
        csv_content = "col1,col2\n1,a\n2,b\n3,c\n"
        flowfile_client.write_shared("roundtrip.csv", csv_content)
        result = flowfile_client.read_shared("roundtrip.csv")
        assert result == csv_content

    def test_write_read_roundtrip_json(self, tmp_dir: Path):
        import json

        data = {"key": "value", "numbers": [1, 2, 3]}
        json_str = json.dumps(data)
        flowfile_client.write_shared("roundtrip.json", json_str)
        result = flowfile_client.read_shared("roundtrip.json")
        assert json.loads(result) == data

    def test_write_read_roundtrip_parquet_bytes(self, tmp_dir: Path):
        """Write raw parquet bytes and read them back."""
        import polars as pl

        df = pl.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        path = flowfile_client.shared_path("roundtrip.parquet")
        df.write_parquet(path)

        result = pl.read_parquet(path)
        assert result.shape == (3, 2)
        assert result["x"].to_list() == [1, 2, 3]
