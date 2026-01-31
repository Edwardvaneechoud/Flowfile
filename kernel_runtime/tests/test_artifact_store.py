"""Tests for kernel_runtime.artifact_store."""

import threading

import pytest

from kernel_runtime.artifact_store import ArtifactStore


class TestPublishAndGet:
    def test_publish_and_retrieve(self, store: ArtifactStore):
        store.publish("my_obj", {"a": 1}, node_id=1)
        assert store.get("my_obj") == {"a": 1}

    def test_publish_duplicate_raises(self, store: ArtifactStore):
        store.publish("key", "first", node_id=1)
        with pytest.raises(ValueError, match="already exists"):
            store.publish("key", "second", node_id=2)

    def test_publish_after_delete_succeeds(self, store: ArtifactStore):
        store.publish("key", "first", node_id=1)
        store.delete("key")
        store.publish("key", "second", node_id=2)
        assert store.get("key") == "second"

    def test_get_missing_raises(self, store: ArtifactStore):
        with pytest.raises(KeyError, match="not found"):
            store.get("nonexistent")

    def test_publish_various_types(self, store: ArtifactStore):
        store.publish("int_val", 42, node_id=1)
        store.publish("list_val", [1, 2, 3], node_id=1)
        store.publish("none_val", None, node_id=1)
        assert store.get("int_val") == 42
        assert store.get("list_val") == [1, 2, 3]
        assert store.get("none_val") is None


class TestListAll:
    def test_empty_store(self, store: ArtifactStore):
        assert store.list_all() == {}

    def test_list_excludes_object(self, store: ArtifactStore):
        store.publish("item", {"secret": "data"}, node_id=5)
        listing = store.list_all()
        assert "item" in listing
        assert "object" not in listing["item"]

    def test_list_metadata_fields(self, store: ArtifactStore):
        store.publish("item", [1, 2], node_id=3)
        meta = store.list_all()["item"]
        assert meta["name"] == "item"
        assert meta["type_name"] == "list"
        assert meta["module"] == "builtins"
        assert meta["node_id"] == 3
        assert "created_at" in meta
        assert "size_bytes" in meta

    def test_list_multiple_items(self, store: ArtifactStore):
        store.publish("a", 1, node_id=1)
        store.publish("b", 2, node_id=2)
        listing = store.list_all()
        assert set(listing.keys()) == {"a", "b"}


class TestClear:
    def test_clear_empties_store(self, store: ArtifactStore):
        store.publish("x", 1, node_id=1)
        store.publish("y", 2, node_id=1)
        store.clear()
        assert store.list_all() == {}

    def test_clear_then_get_raises(self, store: ArtifactStore):
        store.publish("x", 1, node_id=1)
        store.clear()
        with pytest.raises(KeyError):
            store.get("x")

    def test_clear_idempotent(self, store: ArtifactStore):
        store.clear()
        store.clear()
        assert store.list_all() == {}


class TestDelete:
    def test_delete_removes_artifact(self, store: ArtifactStore):
        store.publish("model", {"w": [1, 2]}, node_id=1)
        store.delete("model")
        assert "model" not in store.list_all()

    def test_delete_missing_raises(self, store: ArtifactStore):
        with pytest.raises(KeyError, match="not found"):
            store.delete("nonexistent")

    def test_delete_then_get_raises(self, store: ArtifactStore):
        store.publish("tmp", 42, node_id=1)
        store.delete("tmp")
        with pytest.raises(KeyError, match="not found"):
            store.get("tmp")

    def test_delete_only_target(self, store: ArtifactStore):
        store.publish("keep", 1, node_id=1)
        store.publish("remove", 2, node_id=1)
        store.delete("remove")
        assert store.get("keep") == 1
        assert set(store.list_all().keys()) == {"keep"}


class TestThreadSafety:
    def test_concurrent_publishes(self, store: ArtifactStore):
        errors = []

        def publish_range(start: int, count: int):
            try:
                for i in range(start, start + count):
                    store.publish(f"item_{i}", i, node_id=i)
            except Exception as exc:
                errors.append(exc)

        threads = [
            threading.Thread(target=publish_range, args=(i * 100, 100))
            for i in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        listing = store.list_all()
        assert len(listing) == 400
