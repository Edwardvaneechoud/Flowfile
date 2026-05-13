"""Tests for the centralised catalog read primitives."""
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from flowfile_worker.catalog_reader import open_catalog_table, open_virtual_result
from shared.storage_config import storage


@pytest.fixture(autouse=True)
def _setup_storage(tmp_path: Path):
    old_base, old_user = storage._base_dir, storage._user_data_dir
    storage._base_dir = tmp_path
    storage._user_data_dir = tmp_path
    storage._ensure_directories()
    yield
    storage._base_dir = old_base
    storage._user_data_dir = old_user


class TestOpenCatalogTable:
    def test_happy_path(self, tmp_path: Path):
        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        dest = storage.catalog_tables_directory / "ct_happy"
        df.write_delta(str(dest), mode="error")

        lf = open_catalog_table("ct_happy")
        assert isinstance(lf, pl.LazyFrame)
        out = lf.collect()
        assert out.height == 3
        assert out.columns == ["a", "b"]

    def test_path_traversal_rejected(self):
        with pytest.raises((ValueError, OSError)):
            open_catalog_table("../etc/passwd")

    def test_missing_dir_raises(self):
        with pytest.raises(Exception):
            open_catalog_table("does_not_exist").collect()


class TestOpenVirtualResult:
    def test_happy_path(self, tmp_path: Path):
        df = pl.DataFrame({"x": [10, 20]})
        dest = storage.catalog_virtual_results_directory / "vr_happy.arrow"
        df.write_ipc(str(dest))

        lf = open_virtual_result("vr_happy.arrow")
        assert isinstance(lf, pl.LazyFrame)
        out = lf.collect()
        assert out.height == 2
        assert out.columns == ["x"]

    def test_path_traversal_rejected(self):
        with pytest.raises((ValueError, OSError)):
            open_virtual_result("../etc/passwd")

    def test_missing_file_raises(self):
        with pytest.raises(Exception):
            open_virtual_result("missing.arrow").collect()
