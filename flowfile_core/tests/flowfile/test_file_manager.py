"""Tests for utils/fileManager module."""

import os
import tempfile

import pytest

from flowfile_core.schemas.input_schema import NewDirectory, RemoveItem, RemoveItemsInput
from flowfile_core.utils.fileManager import create_dir, remove_path, remove_item, remove_paths


class TestCreateDir:
    """Test create_dir function."""

    def test_create_new_directory(self, tmp_path):
        new_dir = NewDirectory(source_path=str(tmp_path), dir_name="test_folder")
        success, error = create_dir(new_dir)
        assert success is True
        assert error is None
        assert os.path.isdir(tmp_path / "test_folder")

    def test_create_existing_directory_fails(self, tmp_path):
        os.mkdir(tmp_path / "existing")
        new_dir = NewDirectory(source_path=str(tmp_path), dir_name="existing")
        success, error = create_dir(new_dir)
        assert success is False
        assert error is not None

    def test_create_in_nonexistent_path_fails(self):
        new_dir = NewDirectory(source_path="/nonexistent/path", dir_name="test")
        success, error = create_dir(new_dir)
        assert success is False
        assert error is not None


class TestRemovePath:
    """Test remove_path function."""

    def test_remove_existing_file(self, tmp_path):
        file_path = tmp_path / "test.txt"
        file_path.write_text("test")
        success, error = remove_path(str(file_path))
        assert success is True
        assert error is None
        assert not file_path.exists()

    def test_remove_nonexistent_file(self):
        success, error = remove_path("/nonexistent/file.txt")
        assert success is False
        assert error is not None


class TestRemoveItem:
    """Test remove_item function."""

    def test_remove_file_with_positive_id(self, tmp_path):
        file_path = tmp_path / "test.txt"
        file_path.write_text("test")
        item = RemoveItem(path=str(file_path), id=1)
        remove_item(item)
        assert not file_path.exists()

    def test_remove_file_with_negative_id(self, tmp_path):
        file_path = tmp_path / "test.txt"
        file_path.write_text("test")
        item = RemoveItem(path=str(file_path), id=-1)
        remove_item(item)
        assert not file_path.exists()

    def test_remove_directory_with_negative_id(self, tmp_path):
        dir_path = tmp_path / "test_dir"
        dir_path.mkdir()
        item = RemoveItem(path=str(dir_path), id=-1)
        remove_item(item)
        assert not dir_path.exists()


class TestRemovePaths:
    """Test remove_paths function."""

    def test_remove_multiple_files(self, tmp_path):
        file1 = tmp_path / "file1.txt"
        file2 = tmp_path / "file2.txt"
        file1.write_text("test1")
        file2.write_text("test2")

        items = RemoveItemsInput(
            paths=[
                RemoveItem(path=str(file1), id=1),
                RemoveItem(path=str(file2), id=2),
            ],
            source_path=str(tmp_path),
        )
        success, error = remove_paths(items)
        assert success is True
        assert error is None
        assert not file1.exists()
        assert not file2.exists()

    def test_remove_nonexistent_fails(self, tmp_path):
        items = RemoveItemsInput(
            paths=[RemoveItem(path="/nonexistent/file.txt", id=1)],
            source_path=str(tmp_path),
        )
        success, error = remove_paths(items)
        assert success is False
        assert error is not None
