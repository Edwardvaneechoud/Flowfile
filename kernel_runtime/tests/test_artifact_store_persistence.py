"""Tests for ArtifactStore persistence integration.

These tests verify the interaction between ArtifactStore and PersistenceManager
across all recovery modes (lazy, eager, none).
"""

import pytest

from kernel_runtime.artifact_store import ArtifactStore, RecoveryMode
from kernel_runtime.persistence import PersistenceManager


# ---------------------------------------------------------------------------
# Lazy mode tests
# ---------------------------------------------------------------------------


class TestLazyMode:
    """ArtifactStore with RecoveryMode.LAZY — load from disk on first access."""

    def test_publish_persists_to_disk(self, persistent_store_lazy, persistence_manager):
        persistent_store_lazy.publish("model", {"w": [1, 2]}, node_id=1)
        assert persistence_manager.has_persisted("model")

    def test_publish_marks_persisted(self, persistent_store_lazy):
        persistent_store_lazy.publish("item", 42, node_id=1)
        meta = persistent_store_lazy.list_all()["item"]
        assert meta["persisted"] is True

    def test_get_returns_from_memory(self, persistent_store_lazy):
        persistent_store_lazy.publish("item", "hello", node_id=1)
        assert persistent_store_lazy.get("item") == "hello"

    def test_lazy_load_from_disk_after_clear(
        self, persistent_store_lazy, persistence_manager
    ):
        """After clearing memory, get() lazy-loads from disk."""
        persistent_store_lazy.publish("model", {"v": 1}, node_id=1)
        persistent_store_lazy.clear()

        # Memory is empty but disk has the artifact
        result = persistent_store_lazy.get("model")
        assert result == {"v": 1}

    def test_lazy_load_simulating_restart(self, tmp_path):
        """Simulate kernel restart: new ArtifactStore sees persisted data."""
        pm = PersistenceManager(str(tmp_path), kernel_id="k1")

        # First "session" — publish and persist
        store1 = ArtifactStore(persistence=pm, recovery_mode=RecoveryMode.LAZY)
        store1.publish("model", [1, 2, 3], node_id=1)

        # Second "session" — new store, same persistence dir
        store2 = ArtifactStore(persistence=pm, recovery_mode=RecoveryMode.LAZY)
        assert store2.get("model") == [1, 2, 3]

    def test_list_all_includes_disk_only_artifacts(
        self, persistent_store_lazy, persistence_manager
    ):
        persistent_store_lazy.publish("in_mem", 1, node_id=1)
        persistent_store_lazy.clear()

        listing = persistent_store_lazy.list_all()
        assert "in_mem" in listing
        assert listing["in_mem"]["persisted"] is True
        assert listing["in_mem"]["loaded"] is False

    def test_delete_removes_from_disk(self, persistent_store_lazy, persistence_manager):
        persistent_store_lazy.publish("item", 42, node_id=1)
        persistent_store_lazy.delete("item")
        assert not persistence_manager.has_persisted("item")

    def test_delete_disk_only_artifact(
        self, persistent_store_lazy, persistence_manager
    ):
        """Delete an artifact that exists only on disk (not in memory)."""
        persistent_store_lazy.publish("item", "data", node_id=1)
        persistent_store_lazy.clear()  # clear memory
        persistent_store_lazy.delete("item")  # should delete from disk
        assert not persistence_manager.has_persisted("item")

    def test_get_nonexistent_raises(self, persistent_store_lazy):
        with pytest.raises(KeyError, match="not found"):
            persistent_store_lazy.get("ghost")

    def test_clear_preserves_disk(self, persistent_store_lazy, persistence_manager):
        persistent_store_lazy.publish("item", 1, node_id=1)
        persistent_store_lazy.clear()
        assert persistence_manager.has_persisted("item")


# ---------------------------------------------------------------------------
# Eager mode tests
# ---------------------------------------------------------------------------


class TestEagerMode:
    """ArtifactStore with RecoveryMode.EAGER — pre-load all on init."""

    def test_eager_load_on_init(self, tmp_path):
        """Artifacts persisted in a previous session are loaded on init."""
        pm = PersistenceManager(str(tmp_path), kernel_id="k1")
        pm.persist("model", {"accuracy": 0.9}, {"name": "model", "node_id": 1})

        store = ArtifactStore(persistence=pm, recovery_mode=RecoveryMode.EAGER)
        assert store.get("model") == {"accuracy": 0.9}

    def test_eager_load_marks_recovered(self, tmp_path):
        pm = PersistenceManager(str(tmp_path), kernel_id="k1")
        pm.persist("item", 42, {"name": "item", "node_id": 1})

        store = ArtifactStore(persistence=pm, recovery_mode=RecoveryMode.EAGER)
        status = store.recovery_status()
        assert status["artifacts_recovered"] == {"item": True}
        assert status["not_yet_loaded"] == []

    def test_eager_mode_publish_still_persists(self, tmp_path):
        pm = PersistenceManager(str(tmp_path), kernel_id="k1")
        store = ArtifactStore(persistence=pm, recovery_mode=RecoveryMode.EAGER)
        store.publish("new_item", "hello", node_id=5)
        assert pm.has_persisted("new_item")


# ---------------------------------------------------------------------------
# None mode tests
# ---------------------------------------------------------------------------


class TestNoneMode:
    """ArtifactStore with RecoveryMode.NONE — clean slate on init."""

    def test_none_mode_clears_disk_on_init(self, tmp_path):
        pm = PersistenceManager(str(tmp_path), kernel_id="k1")
        pm.persist("old", 42, {"name": "old", "node_id": 1})

        store = ArtifactStore(persistence=pm, recovery_mode=RecoveryMode.NONE)
        assert not pm.has_persisted("old")
        assert store.list_all() == {}

    def test_none_mode_publish_still_works(self, tmp_path):
        pm = PersistenceManager(str(tmp_path), kernel_id="k1")
        store = ArtifactStore(persistence=pm, recovery_mode=RecoveryMode.NONE)
        store.publish("item", "val", node_id=1)
        assert store.get("item") == "val"
        assert pm.has_persisted("item")


# ---------------------------------------------------------------------------
# Recovery operations
# ---------------------------------------------------------------------------


class TestRecoverAll:
    def test_recover_all_loads_from_disk(self, persistent_store_lazy, persistence_manager):
        persistent_store_lazy.publish("a", 1, node_id=1)
        persistent_store_lazy.publish("b", 2, node_id=2)
        persistent_store_lazy.clear()

        results = persistent_store_lazy.recover_all()
        assert results["a"] == "recovered"
        assert results["b"] == "recovered"
        # Now accessible without lazy-load
        assert persistent_store_lazy.get("a") == 1
        assert persistent_store_lazy.get("b") == 2

    def test_recover_already_loaded_skipped(self, persistent_store_lazy):
        persistent_store_lazy.publish("item", 42, node_id=1)
        results = persistent_store_lazy.recover_all()
        assert results["item"] == "already_loaded"

    def test_recover_with_no_persistence(self):
        store = ArtifactStore()
        results = store.recover_all()
        assert results == {}


class TestRecoveryStatus:
    def test_recovery_status_initial(self, persistent_store_lazy):
        status = persistent_store_lazy.recovery_status()
        assert status["recovery_mode"] == "lazy"
        assert status["persistence_enabled"] is True
        assert status["artifacts_in_memory"] == 0
        assert status["artifacts_persisted"] == 0

    def test_recovery_status_after_publish(self, persistent_store_lazy):
        persistent_store_lazy.publish("model", "v1", node_id=1)
        status = persistent_store_lazy.recovery_status()
        assert status["artifacts_in_memory"] == 1
        assert status["artifacts_persisted"] == 1
        assert status["not_yet_loaded"] == []

    def test_recovery_status_after_clear(self, persistent_store_lazy):
        persistent_store_lazy.publish("model", "v1", node_id=1)
        persistent_store_lazy.clear()
        status = persistent_store_lazy.recovery_status()
        assert status["artifacts_in_memory"] == 0
        assert status["artifacts_persisted"] == 1
        assert status["not_yet_loaded"] == ["model"]

    def test_recovery_status_no_persistence(self):
        store = ArtifactStore()
        status = store.recovery_status()
        assert status["persistence_enabled"] is False


class TestPersistenceInfo:
    def test_persistence_info_no_persistence(self):
        store = ArtifactStore()
        info = store.persistence_info()
        assert info["persistence_enabled"] is False
        assert info["disk_usage_bytes"] == 0

    def test_persistence_info_with_artifacts(self, persistent_store_lazy):
        persistent_store_lazy.publish("model", list(range(100)), node_id=1)
        info = persistent_store_lazy.persistence_info()
        assert info["persistence_enabled"] is True
        assert info["total_artifacts"] == 1
        assert info["persisted_count"] == 1
        assert info["memory_only_count"] == 0
        assert info["disk_usage_bytes"] > 0
        assert "model" in info["artifacts"]
        assert info["artifacts"]["model"]["in_memory"] is True
        assert info["artifacts"]["model"]["persisted"] is True

    def test_persistence_info_disk_only(self, persistent_store_lazy):
        persistent_store_lazy.publish("item", 42, node_id=1)
        persistent_store_lazy.clear()
        info = persistent_store_lazy.persistence_info()
        assert info["total_artifacts"] == 1
        assert info["artifacts"]["item"]["in_memory"] is False
        assert info["artifacts"]["item"]["persisted"] is True


class TestCleanup:
    def test_cleanup_by_name(self, persistent_store_lazy, persistence_manager):
        persistent_store_lazy.publish("keep", 1, node_id=1)
        persistent_store_lazy.publish("remove", 2, node_id=1)
        deleted = persistent_store_lazy.cleanup(names=["remove"])
        assert deleted == ["remove"]
        assert persistent_store_lazy.get("keep") == 1
        with pytest.raises(KeyError):
            persistent_store_lazy.get("remove")

    def test_cleanup_removes_from_memory(self, persistent_store_lazy):
        persistent_store_lazy.publish("item", 42, node_id=1)
        persistent_store_lazy.cleanup(names=["item"])
        with pytest.raises(KeyError):
            persistent_store_lazy.get("item")

    def test_cleanup_no_persistence(self):
        store = ArtifactStore()
        assert store.cleanup(names=["anything"]) == []


# ---------------------------------------------------------------------------
# Backwards compatibility — store without persistence behaves as before
# ---------------------------------------------------------------------------


class TestNoPersistenceBackwardsCompat:
    def test_publish_and_get(self, store):
        store.publish("item", 42, node_id=1)
        assert store.get("item") == 42

    def test_list_all_no_extra_keys(self, store):
        store.publish("item", [1], node_id=1)
        meta = store.list_all()["item"]
        assert "persisted" in meta
        assert meta["persisted"] is False

    def test_clear_and_get_raises(self, store):
        store.publish("item", 1, node_id=1)
        store.clear()
        with pytest.raises(KeyError):
            store.get("item")
