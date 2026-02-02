"""Tests for kernel_runtime.persistence — disk-based artifact serialization."""

import json
import time

import cloudpickle
import pytest

from kernel_runtime.persistence import PersistenceManager


@pytest.fixture()
def pm(tmp_path):
    """PersistenceManager using a temporary directory."""
    return PersistenceManager(str(tmp_path), kernel_id="test-kernel")


class TestPersistAndLoad:
    def test_persist_and_load_dict(self, pm: PersistenceManager):
        obj = {"accuracy": 0.95, "params": [1, 2, 3]}
        pm.persist("model", obj, {"name": "model", "node_id": 1})
        loaded_obj, meta = pm.load("model")
        assert loaded_obj == obj
        assert meta["name"] == "model"
        assert "checksum" in meta
        assert "persisted_at" in meta
        assert "size_on_disk" in meta

    def test_persist_and_load_list(self, pm: PersistenceManager):
        obj = [1, 2, 3, "hello"]
        pm.persist("data", obj, {"name": "data", "node_id": 2})
        loaded_obj, _ = pm.load("data")
        assert loaded_obj == obj

    def test_persist_and_load_none(self, pm: PersistenceManager):
        pm.persist("empty", None, {"name": "empty", "node_id": 1})
        loaded_obj, _ = pm.load("empty")
        assert loaded_obj is None

    def test_persist_and_load_lambda(self, pm: PersistenceManager):
        fn = lambda x: x * 2  # noqa: E731
        pm.persist("fn", fn, {"name": "fn", "node_id": 1})
        loaded_fn, _ = pm.load("fn")
        assert loaded_fn(5) == 10

    def test_persist_and_load_nested_object(self, pm: PersistenceManager):
        class MyModel:
            def __init__(self, weights):
                self.weights = weights

            def predict(self, x):
                return sum(w * xi for w, xi in zip(self.weights, x))

        model = MyModel([0.5, 0.3, 0.2])
        pm.persist("model", model, {"name": "model", "node_id": 1})
        loaded, _ = pm.load("model")
        assert loaded.predict([1, 2, 3]) == model.predict([1, 2, 3])

    def test_load_nonexistent_raises(self, pm: PersistenceManager):
        with pytest.raises(FileNotFoundError, match="No persisted artifact"):
            pm.load("nonexistent")

    def test_checksum_validation(self, pm: PersistenceManager):
        pm.persist("item", {"a": 1}, {"name": "item", "node_id": 1})
        # Corrupt the data file
        data_path = pm.storage_path / "item" / "data.artifact"
        data_path.write_bytes(b"corrupted data")
        with pytest.raises(ValueError, match="Checksum mismatch"):
            pm.load("item")

    def test_persist_overwrites_existing(self, pm: PersistenceManager):
        pm.persist("item", "v1", {"name": "item", "node_id": 1})
        pm.persist("item", "v2", {"name": "item", "node_id": 2})
        loaded, meta = pm.load("item")
        assert loaded == "v2"
        assert meta["node_id"] == 2


class TestDelete:
    def test_delete_removes_artifact(self, pm: PersistenceManager):
        pm.persist("temp", 42, {"name": "temp", "node_id": 1})
        assert pm.has_persisted("temp")
        pm.delete("temp")
        assert not pm.has_persisted("temp")

    def test_delete_nonexistent_is_noop(self, pm: PersistenceManager):
        pm.delete("nonexistent")  # should not raise

    def test_delete_then_load_raises(self, pm: PersistenceManager):
        pm.persist("item", "data", {"name": "item", "node_id": 1})
        pm.delete("item")
        with pytest.raises(FileNotFoundError):
            pm.load("item")


class TestClear:
    def test_clear_removes_all(self, pm: PersistenceManager):
        pm.persist("a", 1, {"name": "a", "node_id": 1})
        pm.persist("b", 2, {"name": "b", "node_id": 1})
        pm.clear()
        assert pm.list_persisted() == {}

    def test_clear_idempotent(self, pm: PersistenceManager):
        pm.clear()
        pm.clear()
        assert pm.list_persisted() == {}


class TestListPersisted:
    def test_list_empty(self, pm: PersistenceManager):
        assert pm.list_persisted() == {}

    def test_list_multiple(self, pm: PersistenceManager):
        pm.persist("a", 1, {"name": "a", "node_id": 1})
        pm.persist("b", 2, {"name": "b", "node_id": 2})
        listing = pm.list_persisted()
        assert set(listing.keys()) == {"a", "b"}
        assert listing["a"]["name"] == "a"
        assert listing["b"]["name"] == "b"

    def test_list_metadata_contents(self, pm: PersistenceManager):
        pm.persist("item", [1, 2], {"name": "item", "node_id": 3, "type_name": "list"})
        listing = pm.list_persisted()
        meta = listing["item"]
        assert meta["name"] == "item"
        assert meta["node_id"] == 3
        assert meta["type_name"] == "list"
        assert "checksum" in meta
        assert "persisted_at" in meta
        assert "size_on_disk" in meta


class TestHasPersisted:
    def test_has_persisted_true(self, pm: PersistenceManager):
        pm.persist("item", 42, {"name": "item", "node_id": 1})
        assert pm.has_persisted("item")

    def test_has_persisted_false(self, pm: PersistenceManager):
        assert not pm.has_persisted("nonexistent")


class TestCleanup:
    def test_cleanup_by_name(self, pm: PersistenceManager):
        pm.persist("keep", 1, {"name": "keep", "node_id": 1})
        pm.persist("remove", 2, {"name": "remove", "node_id": 1})
        deleted = pm.cleanup(names=["remove"])
        assert deleted == ["remove"]
        assert pm.has_persisted("keep")
        assert not pm.has_persisted("remove")

    def test_cleanup_by_age(self, pm: PersistenceManager):
        pm.persist("old", 1, {"name": "old", "node_id": 1})
        # Manually set the persisted_at to 2 hours ago
        meta_path = pm.storage_path / "old" / "meta.json"
        meta = json.loads(meta_path.read_text())
        from datetime import datetime, timedelta, timezone

        old_time = datetime.now(timezone.utc) - timedelta(hours=2)
        meta["persisted_at"] = old_time.isoformat()
        meta_path.write_text(json.dumps(meta))

        pm.persist("new", 2, {"name": "new", "node_id": 1})

        deleted = pm.cleanup(max_age_hours=1.0)
        assert "old" in deleted
        assert "new" not in deleted

    def test_cleanup_nonexistent_name(self, pm: PersistenceManager):
        deleted = pm.cleanup(names=["ghost"])
        assert deleted == []

    def test_cleanup_no_criteria_returns_empty(self, pm: PersistenceManager):
        pm.persist("item", 1, {"name": "item", "node_id": 1})
        deleted = pm.cleanup()
        assert deleted == []


class TestDiskUsage:
    def test_disk_usage_empty(self, pm: PersistenceManager):
        assert pm.disk_usage() == 0

    def test_disk_usage_nonzero(self, pm: PersistenceManager):
        pm.persist("item", list(range(1000)), {"name": "item", "node_id": 1})
        assert pm.disk_usage() > 0

    def test_disk_usage_increases(self, pm: PersistenceManager):
        pm.persist("a", 1, {"name": "a", "node_id": 1})
        usage_1 = pm.disk_usage()
        pm.persist("b", list(range(10000)), {"name": "b", "node_id": 1})
        usage_2 = pm.disk_usage()
        assert usage_2 > usage_1
