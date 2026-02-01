"""Tests for artifact persistence and recovery."""

import json
import time
from pathlib import Path

import pytest

from kernel_runtime.artifact_store import ArtifactStore
from kernel_runtime.persistence import ArtifactPersistence, RecoveryMode


# ======================================================================
# ArtifactPersistence (low-level disk operations)
# ======================================================================


class TestPersistenceSaveAndLoad:
    def test_save_and_load_dict(self, persistence: ArtifactPersistence):
        obj = {"key": "value", "numbers": [1, 2, 3]}
        persistence.save("my_dict", obj, {"type_name": "dict", "module": "builtins"})

        loaded_obj, meta = persistence.load("my_dict")
        assert loaded_obj == obj
        assert meta["type_name"] == "dict"
        assert "checksum" in meta
        assert "persisted_at" in meta
        assert meta["data_size_bytes"] > 0

    def test_save_and_load_list(self, persistence: ArtifactPersistence):
        obj = [1, 2, 3, "hello"]
        persistence.save("my_list", obj, {"type_name": "list"})
        loaded, _ = persistence.load("my_list")
        assert loaded == obj

    def test_save_and_load_none(self, persistence: ArtifactPersistence):
        persistence.save("none_val", None, {"type_name": "NoneType"})
        loaded, _ = persistence.load("none_val")
        assert loaded is None

    def test_save_and_load_nested_object(self, persistence: ArtifactPersistence):
        obj = {"model": {"weights": [0.1, 0.2], "bias": 0.5}, "config": {"lr": 0.01}}
        persistence.save("model", obj, {"type_name": "dict"})
        loaded, _ = persistence.load("model")
        assert loaded == obj

    def test_save_and_load_lambda(self, persistence: ArtifactPersistence):
        func = lambda x: x * 2  # noqa: E731
        persistence.save("my_func", func, {"type_name": "function"})
        loaded, _ = persistence.load("my_func")
        assert loaded(5) == 10

    def test_load_nonexistent_raises(self, persistence: ArtifactPersistence):
        with pytest.raises(FileNotFoundError, match="not found"):
            persistence.load("nonexistent")

    def test_checksum_validation(self, persistence: ArtifactPersistence):
        persistence.save("item", [1, 2], {"type_name": "list"})

        # Corrupt the data file
        data_path = persistence.artifacts_dir / "item" / "data.artifact"
        data_path.write_bytes(b"corrupted data")

        with pytest.raises(ValueError, match="Checksum mismatch"):
            persistence.load("item")

    def test_save_creates_directory_structure(self, persistence: ArtifactPersistence):
        persistence.save("test_obj", {"a": 1}, {"type_name": "dict"})
        artifact_dir = persistence.artifacts_dir / "test_obj"
        assert artifact_dir.is_dir()
        assert (artifact_dir / "data.artifact").is_file()
        assert (artifact_dir / "meta.json").is_file()

    def test_meta_json_is_valid(self, persistence: ArtifactPersistence):
        persistence.save("item", 42, {"type_name": "int", "node_id": 5})
        meta_path = persistence.artifacts_dir / "item" / "meta.json"
        meta = json.loads(meta_path.read_text())
        assert meta["type_name"] == "int"
        assert meta["node_id"] == 5
        assert meta["checksum"].startswith("sha256:")
        assert "persisted_at" in meta


class TestPersistenceDelete:
    def test_delete_removes_from_disk(self, persistence: ArtifactPersistence):
        persistence.save("model", {"w": 1}, {"type_name": "dict"})
        assert (persistence.artifacts_dir / "model").exists()

        persistence.delete("model")
        assert not (persistence.artifacts_dir / "model").exists()

    def test_delete_nonexistent_is_noop(self, persistence: ArtifactPersistence):
        persistence.delete("nonexistent")  # Should not raise


class TestPersistenceListAndClear:
    def test_list_persisted_empty(self, persistence: ArtifactPersistence):
        assert persistence.list_persisted() == {}

    def test_list_persisted_with_items(self, persistence: ArtifactPersistence):
        persistence.save("a", 1, {"type_name": "int"})
        persistence.save("b", "hello", {"type_name": "str"})
        result = persistence.list_persisted()
        assert set(result.keys()) == {"a", "b"}
        assert result["a"]["type_name"] == "int"

    def test_clear_removes_all(self, persistence: ArtifactPersistence):
        persistence.save("x", 1, {"type_name": "int"})
        persistence.save("y", 2, {"type_name": "int"})
        persistence.clear()
        assert persistence.list_persisted() == {}
        # Directory should still exist (recreated)
        assert persistence.artifacts_dir.exists()


class TestPersistenceCleanup:
    def test_cleanup_removes_old_artifacts(self, persistence: ArtifactPersistence):
        # Save an artifact with a very old persisted_at timestamp
        persistence.save("old_item", 1, {"type_name": "int"})
        meta_path = persistence.artifacts_dir / "old_item" / "meta.json"
        meta = json.loads(meta_path.read_text())
        meta["persisted_at"] = "2020-01-01T00:00:00+00:00"
        meta_path.write_text(json.dumps(meta))

        persistence.save("new_item", 2, {"type_name": "int"})

        removed = persistence.cleanup(max_age_hours=1)
        assert "old_item" in removed
        assert "new_item" not in removed
        assert persistence.list_persisted() == {"new_item": pytest.approx(persistence.list_persisted()["new_item"])}

    def test_cleanup_with_no_old_artifacts(self, persistence: ArtifactPersistence):
        persistence.save("fresh", 1, {"type_name": "int"})
        removed = persistence.cleanup(max_age_hours=24)
        assert removed == []
        assert "fresh" in persistence.list_persisted()


class TestPersistenceStats:
    def test_stats_empty(self, persistence: ArtifactPersistence):
        stats = persistence.get_stats()
        assert stats["kernel_id"] == "test-kernel"
        assert stats["artifact_count"] == 0
        assert stats["total_disk_bytes"] == 0
        assert stats["artifacts"] == {}

    def test_stats_with_items(self, persistence: ArtifactPersistence):
        persistence.save("model", {"w": [1, 2, 3]}, {"type_name": "dict"})
        stats = persistence.get_stats()
        assert stats["artifact_count"] == 1
        assert stats["total_disk_bytes"] > 0
        assert "model" in stats["artifacts"]
        assert stats["artifacts"]["model"]["type_name"] == "dict"


# ======================================================================
# ArtifactStore with persistence
# ======================================================================


class TestPersistentStorePublish:
    def test_publish_persists_to_disk(self, persistent_store: ArtifactStore, persistence: ArtifactPersistence):
        persistent_store.publish("model", {"w": 1}, node_id=1)
        # Verify it's persisted on disk
        persisted = persistence.list_persisted()
        assert "model" in persisted

    def test_publish_sets_persisted_flag(self, persistent_store: ArtifactStore):
        persistent_store.publish("model", {"w": 1}, node_id=1)
        listing = persistent_store.list_all()
        assert listing["model"]["persisted"] is True

    def test_publish_stores_in_memory(self, persistent_store: ArtifactStore):
        persistent_store.publish("item", 42, node_id=1)
        assert persistent_store.get("item") == 42


class TestPersistentStoreDelete:
    def test_delete_removes_from_disk(self, persistent_store: ArtifactStore, persistence: ArtifactPersistence):
        persistent_store.publish("model", {"w": 1}, node_id=1)
        persistent_store.delete("model")
        assert persistence.list_persisted() == {}

    def test_delete_removes_from_memory(self, persistent_store: ArtifactStore):
        persistent_store.publish("model", {"w": 1}, node_id=1)
        persistent_store.delete("model")
        with pytest.raises(KeyError):
            persistent_store.get("model")


class TestPersistentStoreLazyLoad:
    def test_lazy_load_on_get(self, persistent_store: ArtifactStore, persistence: ArtifactPersistence):
        """After clear(), artifacts still loadable from disk via get()."""
        persistent_store.publish("model", {"w": [1, 2]}, node_id=1)
        persistent_store.clear()

        # Not in memory after clear
        assert persistent_store.list_all() == {}

        # But accessible via get() (lazy load)
        obj = persistent_store.get("model")
        assert obj == {"w": [1, 2]}

    def test_lazy_loaded_not_in_list_all(self, persistent_store: ArtifactStore):
        """Lazy-loaded artifacts should NOT appear in list_all() (delta tracking)."""
        persistent_store.publish("model", {"w": 1}, node_id=1)
        persistent_store.clear()

        # Lazy load
        persistent_store.get("model")

        # list_all() excludes lazy-loaded (for delta tracking)
        assert persistent_store.list_all() == {}

    def test_lazy_loaded_appears_in_list_available(self, persistent_store: ArtifactStore):
        """Lazy-loaded artifacts should appear in list_available()."""
        persistent_store.publish("model", {"w": 1}, node_id=1)
        persistent_store.clear()

        available = persistent_store.list_available()
        assert "model" in available
        assert available["model"]["persisted"] is True

    def test_lazy_load_marks_recovered(self, persistent_store: ArtifactStore):
        persistent_store.publish("model", {"w": 1}, node_id=1)
        persistent_store.clear()

        persistent_store.get("model")
        available = persistent_store.list_available()
        assert available["model"].get("recovered") is True

    def test_get_nonexistent_still_raises(self, persistent_store: ArtifactStore):
        with pytest.raises(KeyError, match="not found"):
            persistent_store.get("nonexistent")


class TestPersistentStoreClear:
    def test_clear_preserves_disk(self, persistent_store: ArtifactStore, persistence: ArtifactPersistence):
        persistent_store.publish("model", {"w": 1}, node_id=1)
        persistent_store.clear()

        # Memory is cleared
        assert persistent_store.list_all() == {}
        # Disk is preserved
        assert "model" in persistence.list_persisted()


class TestPersistentStoreRecoverAll:
    def test_recover_all_loads_persisted(self, persistent_store: ArtifactStore, persistence: ArtifactPersistence):
        persistent_store.publish("model", {"w": 1}, node_id=1)
        persistent_store.publish("encoder", {"classes": ["a", "b"]}, node_id=2)
        persistent_store.clear()

        recovered = persistent_store.recover_all()
        assert set(recovered) == {"model", "encoder"}

    def test_recover_all_makes_artifacts_accessible(self, persistent_store: ArtifactStore):
        persistent_store.publish("model", {"w": 1}, node_id=1)
        persistent_store.clear()

        persistent_store.recover_all()
        assert persistent_store.get("model") == {"w": 1}

    def test_recover_all_skips_existing(self, persistent_store: ArtifactStore):
        persistent_store.publish("model", {"w": 1}, node_id=1)

        # Recover without clearing - should skip since already in memory
        recovered = persistent_store.recover_all()
        assert recovered == []

    def test_recover_all_without_persistence(self, store: ArtifactStore):
        """recover_all on a store without persistence returns empty list."""
        recovered = store.recover_all()
        assert recovered == []


class TestPersistentStoreListAvailable:
    def test_list_available_includes_memory_and_disk(
        self, persistent_store: ArtifactStore, persistence: ArtifactPersistence
    ):
        persistent_store.publish("in_memory", 1, node_id=1)
        persistent_store.clear()

        # Persist a second artifact directly to disk
        persistence.save("disk_only", 2, {"type_name": "int", "module": "builtins"})

        available = persistent_store.list_available()
        assert "in_memory" in available
        assert "disk_only" in available

    def test_list_available_deduplicates(self, persistent_store: ArtifactStore):
        """An artifact in both memory and disk should appear once."""
        persistent_store.publish("model", {"w": 1}, node_id=1)
        available = persistent_store.list_available()
        assert len([k for k in available if k == "model"]) == 1


class TestDeltaTrackingWithPersistence:
    """Verify that lazy loading doesn't break the execute delta tracking."""

    def test_lazy_load_during_execution_not_counted_as_new(
        self, persistent_store: ArtifactStore
    ):
        """Simulates the delta tracking in /execute."""
        persistent_store.publish("model", {"w": 1}, node_id=1)
        persistent_store.clear()

        # Simulate execute: capture before
        before = set(persistent_store.list_all().keys())
        assert before == set()

        # During execution, user code reads a persisted artifact
        persistent_store.get("model")

        # Capture after
        after = set(persistent_store.list_all().keys())
        assert after == set()  # Lazy-loaded artifacts excluded

        new = sorted(after - before)
        deleted = sorted(before - after)
        assert new == []
        assert deleted == []

    def test_publish_after_lazy_load_counted_as_new(
        self, persistent_store: ArtifactStore
    ):
        """If user reads an old artifact then publishes a new one, only the
        new one shows up as published."""
        persistent_store.publish("old_model", {"w": 1}, node_id=1)
        persistent_store.clear()

        before = set(persistent_store.list_all().keys())

        # Lazy load old artifact
        persistent_store.get("old_model")
        # Publish a genuinely new artifact
        persistent_store.publish("new_model", {"w": 2}, node_id=2)

        after = set(persistent_store.list_all().keys())
        new = sorted(after - before)
        assert new == ["new_model"]

    def test_delete_lazy_loaded_then_republish(self, persistent_store: ArtifactStore):
        """Delete a lazy-loaded artifact and republish with same name."""
        persistent_store.publish("model", {"v": 1}, node_id=1)
        persistent_store.clear()

        # Lazy load
        persistent_store.get("model")
        # Delete and republish
        persistent_store.delete("model")
        persistent_store.publish("model", {"v": 2}, node_id=2)

        # Should be accessible and show in list_all as explicitly published
        assert persistent_store.get("model") == {"v": 2}
        assert "model" in persistent_store.list_all()


# ======================================================================
# RecoveryMode enum
# ======================================================================


class TestRecoveryMode:
    def test_lazy_mode(self):
        assert RecoveryMode.LAZY == "lazy"

    def test_eager_mode(self):
        assert RecoveryMode.EAGER == "eager"

    def test_none_mode(self):
        assert RecoveryMode.NONE == "none"

    def test_from_string(self):
        assert RecoveryMode("lazy") == RecoveryMode.LAZY
        assert RecoveryMode("eager") == RecoveryMode.EAGER
        assert RecoveryMode("none") == RecoveryMode.NONE

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            RecoveryMode("invalid")


# ======================================================================
# Kernel runtime endpoint tests (persistence-related)
# ======================================================================


class TestRecoveryEndpoint:
    def test_recover_without_persistence(self, client):
        """When persistence is disabled, recover returns disabled status."""
        resp = client.post("/recover")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "disabled"
        assert data["recovered_artifacts"] == []

    def test_recovery_status_endpoint(self, client):
        resp = client.get("/recovery-status")
        assert resp.status_code == 200
        data = resp.json()
        assert "mode" in data
        assert "status" in data
        assert "recovered_artifacts" in data


class TestCleanupEndpoint:
    def test_cleanup_without_persistence(self, client):
        resp = client.post("/cleanup", json={"max_age_hours": 24})
        assert resp.status_code == 200
        data = resp.json()
        assert data["removed_artifacts"] == []
        assert data["remaining_count"] == 0


class TestPersistenceEndpoint:
    def test_persistence_info_without_persistence(self, client):
        resp = client.get("/persistence")
        assert resp.status_code == 200
        data = resp.json()
        assert data["enabled"] is False
        assert data["artifact_count"] == 0


class TestHealthWithPersistence:
    def test_health_shows_persistence_disabled(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
        assert data["persistence_enabled"] is False
