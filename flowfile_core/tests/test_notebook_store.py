"""Unit tests for the on-disk notebook content store (no DB / HTTP)."""

import pytest

from flowfile_core.catalog.services import notebook_store
from flowfile_core.schemas.catalog_schema import NotebookCellModel

NU = "11111111-1111-1111-1111-111111111111"


@pytest.fixture
def store_dir(tmp_path, monkeypatch):
    """Point the store at an isolated temp root so tests never touch ~/.flowfile."""

    class _Storage:
        notebooks_directory = tmp_path

    monkeypatch.setattr(notebook_store, "storage", _Storage)
    return tmp_path


def _cells():
    return [
        NotebookCellModel(id="c1", type="markdown", source="# title", metadata={}),
        NotebookCellModel(id="c2", type="python", source="import polars as pl\ndf = pl.DataFrame()\n", metadata={}),
        NotebookCellModel(id="c3", type="sql", source="SELECT 1", metadata={"max_rows": 50}),
    ]


def _write(owner_id=1, notebook_uuid=NU, cells=None):
    notebook_store.write_notebook_file(
        owner_id,
        notebook_uuid,
        name="nb",
        description="d",
        namespace_name="General.default",
        default_kernel_id=None,
        cells=cells if cells is not None else _cells(),
    )


def test_round_trip(store_dir):
    _write()
    got = notebook_store.read_notebook_cells(1, NU)
    assert got == _cells()


def test_file_layout_and_block_scalar(store_dir):
    _write()
    path = store_dir / "1" / f"{NU}.notebook.yaml"
    assert path.is_file()
    text = path.read_text(encoding="utf-8")
    assert "source: |" in text  # multi-line python source is a literal block


def test_write_is_deterministic(store_dir):
    cells = [NotebookCellModel(id="c1", type="python", source="a=1\nb=2\n", metadata={"z": 1, "a": 2})]
    _write(cells=cells)
    path = store_dir / "1" / f"{NU}.notebook.yaml"
    first = path.read_bytes()
    _write(cells=cells)
    assert path.read_bytes() == first  # no volatile fields, sorted metadata


def test_per_owner_isolation(store_dir):
    _write(owner_id=1)
    _write(owner_id=2)
    assert (store_dir / "1" / f"{NU}.notebook.yaml").is_file()
    assert (store_dir / "2" / f"{NU}.notebook.yaml").is_file()


def test_missing_file_degrades_to_empty(store_dir):
    assert notebook_store.read_notebook_cells(1, NU) == []


def test_corrupt_file_degrades_to_empty(store_dir):
    path = store_dir / "1" / f"{NU}.notebook.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(": : not yaml : :", encoding="utf-8")
    assert notebook_store.read_notebook_cells(1, NU) == []


def test_non_list_cells_degrades_to_empty(store_dir):
    path = store_dir / "1" / f"{NU}.notebook.yaml"
    path.parent.mkdir(parents=True)
    path.write_text("cells: 42\n", encoding="utf-8")
    assert notebook_store.read_notebook_cells(1, NU) == []


def test_skips_bad_cell_keeps_good(store_dir):
    import yaml

    path = store_dir / "1" / f"{NU}.notebook.yaml"
    path.parent.mkdir(parents=True)
    good = {"id": "c1", "type": "markdown", "source": "# ok", "metadata": {}}
    path.write_text(yaml.safe_dump({"cells": [good, 42, {"missing": "type"}]}), encoding="utf-8")
    parsed = notebook_store.read_notebook_cells(1, NU)
    assert len(parsed) == 1
    assert parsed[0].id == "c1"


def test_path_traversal_rejected(store_dir):
    with pytest.raises(ValueError):
        notebook_store._notebook_path(1, "../../etc/passwd")
    # Read/delete swallow the bad id instead of raising.
    assert notebook_store.read_notebook_cells(1, "../../etc/passwd") == []
    notebook_store.delete_notebook_file(1, "../../etc/passwd")


def test_delete_is_idempotent(store_dir):
    _write()
    notebook_store.delete_notebook_file(1, NU)
    assert not (store_dir / "1" / f"{NU}.notebook.yaml").exists()
    notebook_store.delete_notebook_file(1, NU)  # second delete is a no-op
