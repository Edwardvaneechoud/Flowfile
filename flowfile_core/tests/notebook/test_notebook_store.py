"""Tests for standalone notebook persistence (.ipynb round-trip + CRUD)."""

import pytest

from flowfile_core.notebook import store
from flowfile_core.notebook.models import NotebookCell


@pytest.fixture()
def user_storage(tmp_path, monkeypatch):
    """Point notebook storage at a temp dir for the duration of the test."""
    from shared.storage_config import storage as _storage

    monkeypatch.setattr(_storage, "_base_dir", tmp_path)
    monkeypatch.setattr(_storage, "_user_data_dir", tmp_path)
    return 42  # user_id


def test_create_assigns_unique_synthetic_flow_id(user_storage):
    nb1 = store.create_notebook(user_storage, "First", kernel_id=None)
    nb2 = store.create_notebook(user_storage, "Second", kernel_id="k1")
    assert nb1.flow_id != nb2.flow_id
    # Synthetic ids live in the reserved high range (never collide with real flows)
    assert nb1.flow_id >= store._FLOW_ID_BASE
    assert nb2.kernel_id == "k1"
    assert len(nb1.cells) == 1


def test_round_trip_persists_cells_and_metadata(user_storage):
    nb = store.create_notebook(user_storage, "RT", kernel_id=None)
    cells = [
        NotebookCell(id="a", source="import polars as pl\n"),
        NotebookCell(id="b", source="pl.DataFrame({'x': [1, 2]})"),
    ]
    store.save_notebook(user_storage, nb, name="Renamed", kernel_id="k9", cells=cells)

    reloaded = store.get_notebook(user_storage, nb.id)
    assert reloaded is not None
    assert reloaded.name == "Renamed"
    assert reloaded.kernel_id == "k9"
    assert reloaded.flow_id == nb.flow_id  # stable across saves
    assert [c.source for c in reloaded.cells] == [c.source for c in cells]
    assert [c.id for c in reloaded.cells] == ["a", "b"]


def test_list_and_delete(user_storage):
    a = store.create_notebook(user_storage, "A", kernel_id=None)
    b = store.create_notebook(user_storage, "B", kernel_id=None)
    ids = {n.id for n in store.list_notebooks(user_storage)}
    assert {a.id, b.id} <= ids

    assert store.delete_notebook(user_storage, a.id) is True
    assert store.get_notebook(user_storage, a.id) is None
    assert store.delete_notebook(user_storage, a.id) is False


def test_invalid_id_rejected():
    assert store.is_valid_id("../etc/passwd") is False
    assert store.is_valid_id("not-a-uuid") is False
    a = "11111111-1111-1111-1111-111111111111"
    assert store.is_valid_id(a) is True


def test_isolation_between_users(user_storage, tmp_path, monkeypatch):
    nb = store.create_notebook(user_storage, "owned", kernel_id=None)
    # A different user must not see or fetch another user's notebook.
    other_user = 99
    assert store.get_notebook(other_user, nb.id) is None
    assert all(n.id != nb.id for n in store.list_notebooks(other_user))
