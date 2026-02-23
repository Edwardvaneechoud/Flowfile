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


class TestClearByNodeIds:
    def test_clear_by_node_ids_removes_only_target(self, store: ArtifactStore):
        store.publish("a", 1, node_id=1)
        store.publish("b", 2, node_id=2)
        store.publish("c", 3, node_id=1)
        removed = store.clear_by_node_ids({1})
        assert sorted(removed) == ["a", "c"]
        assert "b" in store.list_all()
        assert "a" not in store.list_all()
        assert "c" not in store.list_all()

    def test_clear_by_node_ids_empty_set(self, store: ArtifactStore):
        store.publish("x", 1, node_id=1)
        removed = store.clear_by_node_ids(set())
        assert removed == []
        assert "x" in store.list_all()

    def test_clear_by_node_ids_nonexistent(self, store: ArtifactStore):
        store.publish("x", 1, node_id=1)
        removed = store.clear_by_node_ids({99})
        assert removed == []
        assert "x" in store.list_all()

    def test_clear_by_node_ids_multiple(self, store: ArtifactStore):
        store.publish("a", 1, node_id=1)
        store.publish("b", 2, node_id=2)
        store.publish("c", 3, node_id=3)
        removed = store.clear_by_node_ids({1, 3})
        assert sorted(removed) == ["a", "c"]
        assert set(store.list_all().keys()) == {"b"}

    def test_clear_allows_republish(self, store: ArtifactStore):
        """After clearing a node's artifacts, re-publishing with the same name works."""
        store.publish("model", {"v": 1}, node_id=5)
        store.clear_by_node_ids({5})
        store.publish("model", {"v": 2}, node_id=5)
        assert store.get("model") == {"v": 2}


class TestListByNodeId:
    def test_list_by_node_id(self, store: ArtifactStore):
        store.publish("a", 1, node_id=1)
        store.publish("b", 2, node_id=2)
        store.publish("c", 3, node_id=1)
        listing = store.list_by_node_id(1)
        assert set(listing.keys()) == {"a", "c"}

    def test_list_by_node_id_empty(self, store: ArtifactStore):
        assert store.list_by_node_id(99) == {}

    def test_list_by_node_id_excludes_object(self, store: ArtifactStore):
        store.publish("x", {"secret": "data"}, node_id=1)
        listing = store.list_by_node_id(1)
        assert "object" not in listing["x"]


class TestFlowIsolation:
    """Artifacts with the same name in different flows are independent."""

    def test_same_name_different_flows(self, store: ArtifactStore):
        store.publish("model", "flow1_model", node_id=1, flow_id=1)
        store.publish("model", "flow2_model", node_id=2, flow_id=2)
        assert store.get("model", flow_id=1) == "flow1_model"
        assert store.get("model", flow_id=2) == "flow2_model"

    def test_delete_scoped_to_flow(self, store: ArtifactStore):
        store.publish("model", "v1", node_id=1, flow_id=1)
        store.publish("model", "v2", node_id=2, flow_id=2)
        store.delete("model", flow_id=1)
        # flow 2's artifact is untouched
        assert store.get("model", flow_id=2) == "v2"
        with pytest.raises(KeyError):
            store.get("model", flow_id=1)

    def test_list_all_filtered_by_flow(self, store: ArtifactStore):
        store.publish("a", 1, node_id=1, flow_id=1)
        store.publish("b", 2, node_id=2, flow_id=2)
        store.publish("c", 3, node_id=1, flow_id=1)
        assert set(store.list_all(flow_id=1).keys()) == {"a", "c"}
        assert set(store.list_all(flow_id=2).keys()) == {"b"}

    def test_list_all_unfiltered_returns_everything(self, store: ArtifactStore):
        store.publish("a", 1, node_id=1, flow_id=1)
        store.publish("b", 2, node_id=2, flow_id=2)
        assert set(store.list_all().keys()) == {"a", "b"}

    def test_clear_scoped_to_flow(self, store: ArtifactStore):
        store.publish("a", 1, node_id=1, flow_id=1)
        store.publish("b", 2, node_id=2, flow_id=2)
        store.clear(flow_id=1)
        with pytest.raises(KeyError):
            store.get("a", flow_id=1)
        assert store.get("b", flow_id=2) == 2

    def test_clear_all_clears_every_flow(self, store: ArtifactStore):
        store.publish("a", 1, node_id=1, flow_id=1)
        store.publish("b", 2, node_id=2, flow_id=2)
        store.clear()
        assert store.list_all() == {}

    def test_clear_by_node_ids_scoped_to_flow(self, store: ArtifactStore):
        """Same node_id in different flows — only the targeted flow is cleared."""
        store.publish("model", "f1", node_id=5, flow_id=1)
        store.publish("model", "f2", node_id=5, flow_id=2)
        removed = store.clear_by_node_ids({5}, flow_id=1)
        assert removed == ["model"]
        # flow 2's artifact survives
        assert store.get("model", flow_id=2) == "f2"
        with pytest.raises(KeyError):
            store.get("model", flow_id=1)

    def test_list_by_node_id_scoped_to_flow(self, store: ArtifactStore):
        store.publish("a", 1, node_id=5, flow_id=1)
        store.publish("b", 2, node_id=5, flow_id=2)
        assert set(store.list_by_node_id(5, flow_id=1).keys()) == {"a"}
        assert set(store.list_by_node_id(5, flow_id=2).keys()) == {"b"}
        # Unfiltered returns both
        assert set(store.list_by_node_id(5).keys()) == {"a", "b"}

    def test_metadata_includes_flow_id(self, store: ArtifactStore):
        store.publish("item", 42, node_id=1, flow_id=7)
        meta = store.list_all(flow_id=7)["item"]
        assert meta["flow_id"] == 7


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
