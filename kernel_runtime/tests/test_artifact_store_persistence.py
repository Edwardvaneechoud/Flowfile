"""Tests for ArtifactStore persistence integration."""

import pytest

from kernel_runtime.artifact_persistence import ArtifactPersistence
from kernel_runtime.artifact_store import ArtifactStore


class TestPersistenceOnPublish:
    """Publishing an artifact should automatically persist to disk."""

    def test_publish_persists_to_disk(self, store_with_persistence, persistence):
        store_with_persistence.publish("model", {"w": [1, 2]}, node_id=1, flow_id=0)

        # Verify it's on disk
        persisted = persistence.list_persisted()
        assert (0, "model") in persisted

        # Verify the data is correct
        loaded = persistence.load("model", flow_id=0)
        assert loaded == {"w": [1, 2]}

    def test_publish_sets_persisted_flag(self, store_with_persistence):
        store_with_persistence.publish("item", 42, node_id=1, flow_id=0)

        meta = store_with_persistence.list_all()
        assert meta["item"]["persisted"] is True

    def test_delete_removes_from_disk(self, store_with_persistence, persistence):
        store_with_persistence.publish("temp", 42, node_id=1, flow_id=0)
        store_with_persistence.delete("temp", flow_id=0)

        assert persistence.list_persisted() == {}

    def test_clear_removes_from_disk(self, store_with_persistence, persistence):
        store_with_persistence.publish("a", 1, node_id=1, flow_id=1)
        store_with_persistence.publish("b", 2, node_id=2, flow_id=2)
        store_with_persistence.clear()

        assert persistence.list_persisted() == {}

    def test_clear_by_flow_removes_from_disk(self, store_with_persistence, persistence):
        store_with_persistence.publish("a", 1, node_id=1, flow_id=1)
        store_with_persistence.publish("b", 2, node_id=2, flow_id=2)
        store_with_persistence.clear(flow_id=1)

        persisted = persistence.list_persisted()
        assert (1, "a") not in persisted
        assert (2, "b") in persisted

    def test_clear_by_node_ids_removes_from_disk(self, store_with_persistence, persistence):
        store_with_persistence.publish("a", 1, node_id=1, flow_id=0)
        store_with_persistence.publish("b", 2, node_id=2, flow_id=0)
        store_with_persistence.clear_by_node_ids({1}, flow_id=0)

        persisted = persistence.list_persisted()
        assert (0, "a") not in persisted
        assert (0, "b") in persisted


class TestLazyRecovery:
    """Lazy loading: artifacts on disk are loaded into memory on first access."""

    def test_lazy_index_built(self, persistence):
        # Pre-populate disk
        meta = {"name": "model", "node_id": 1, "type_name": "dict", "module": "builtins"}
        persistence.save("model", {"w": 1}, meta, flow_id=0)

        # Create a fresh store with persistence
        store = ArtifactStore()
        store.enable_persistence(persistence)
        count = store.build_lazy_index()

        assert count == 1

    def test_lazy_load_on_get(self, persistence):
        # Pre-populate disk
        meta = {"name": "model", "node_id": 1, "type_name": "dict", "module": "builtins"}
        persistence.save("model", {"w": 42}, meta, flow_id=0)

        # Create a fresh store with persistence + lazy index
        store = ArtifactStore()
        store.enable_persistence(persistence)
        store.build_lazy_index()

        # The artifact should not be in memory yet
        listing = store.list_all()
        assert "model" in listing
        assert listing["model"].get("in_memory") is False

        # Accessing it should trigger lazy load
        obj = store.get("model", flow_id=0)
        assert obj == {"w": 42}

        # Now it should be in memory
        listing = store.list_all()
        assert "model" in listing
        # No more in_memory=False flag

    def test_lazy_load_preserves_metadata(self, persistence):
        meta = {"name": "model", "node_id": 5, "type_name": "dict", "module": "builtins",
                "created_at": "2024-01-01T00:00:00+00:00", "size_bytes": 100}
        persistence.save("model", {"w": 1}, meta, flow_id=3)

        store = ArtifactStore()
        store.enable_persistence(persistence)
        store.build_lazy_index()

        # Trigger lazy load
        store.get("model", flow_id=3)

        listing = store.list_all(flow_id=3)
        assert listing["model"]["node_id"] == 5
        assert listing["model"]["flow_id"] == 3
        assert listing["model"]["recovered"] is True

    def test_lazy_list_includes_disk_artifacts(self, persistence):
        meta = {"name": "model", "node_id": 1, "type_name": "dict", "module": "builtins"}
        persistence.save("model", {"w": 1}, meta, flow_id=0)

        store = ArtifactStore()
        store.enable_persistence(persistence)
        store.build_lazy_index()

        # Publish an in-memory artifact
        store.publish("other", 42, node_id=2, flow_id=0)

        listing = store.list_all(flow_id=0)
        assert "model" in listing  # from disk
        assert "other" in listing  # from memory

    def test_publish_removes_from_lazy_index(self, persistence):
        meta = {"name": "model", "node_id": 1, "type_name": "dict", "module": "builtins"}
        persistence.save("model", {"w": 1}, meta, flow_id=0)

        store = ArtifactStore()
        store.enable_persistence(persistence)
        store.build_lazy_index()

        # Delete (which should clear from lazy index) then republish
        store.delete("model", flow_id=0)
        store.publish("model", {"w": 2}, node_id=3, flow_id=0)

        assert store.get("model", flow_id=0) == {"w": 2}


class TestEagerRecovery:
    """Eager recovery: all persisted artifacts loaded into memory at once."""

    def test_recover_all(self, persistence):
        meta1 = {"name": "a", "node_id": 1, "type_name": "int", "module": "builtins"}
        meta2 = {"name": "b", "node_id": 2, "type_name": "str", "module": "builtins"}
        persistence.save("a", 42, meta1, flow_id=0)
        persistence.save("b", "hello", meta2, flow_id=1)

        store = ArtifactStore()
        store.enable_persistence(persistence)
        recovered = store.recover_all()

        assert sorted(recovered) == ["a", "b"]
        assert store.get("a", flow_id=0) == 42
        assert store.get("b", flow_id=1) == "hello"

    def test_recover_skips_already_in_memory(self, persistence):
        meta = {"name": "model", "node_id": 1, "type_name": "int", "module": "builtins"}
        persistence.save("model", 42, meta, flow_id=0)

        store = ArtifactStore()
        store.enable_persistence(persistence)
        store.publish("model", 99, node_id=1, flow_id=0)

        recovered = store.recover_all()
        assert recovered == []  # already in memory
        assert store.get("model", flow_id=0) == 99  # original value preserved

    def test_recover_marks_recovered(self, persistence):
        meta = {"name": "model", "node_id": 1, "type_name": "dict", "module": "builtins"}
        persistence.save("model", {"w": 1}, meta, flow_id=0)

        store = ArtifactStore()
        store.enable_persistence(persistence)
        store.recover_all()

        listing = store.list_all()
        assert listing["model"]["recovered"] is True
        assert listing["model"]["persisted"] is True


class TestNoPersistence:
    """When no persistence backend is attached, store behaves exactly as before."""

    def test_no_persistence_publish_get(self):
        store = ArtifactStore()
        store.publish("item", 42, node_id=1)
        assert store.get("item") == 42

    def test_recover_all_returns_empty(self):
        store = ArtifactStore()
        assert store.recover_all() == []

    def test_build_lazy_index_returns_zero(self):
        store = ArtifactStore()
        assert store.build_lazy_index() == 0
