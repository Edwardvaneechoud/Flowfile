"""Tests for kernel_runtime.artifact_persistence."""

import json
import time

import pytest

from kernel_runtime.artifact_persistence import ArtifactPersistence, RecoveryMode, _safe_dirname


class TestSafeDirname:
    def test_simple_name(self):
        assert _safe_dirname("model") == "model"

    def test_with_spaces(self):
        assert _safe_dirname("my model") == "my_model"

    def test_with_special_chars(self):
        assert _safe_dirname("model/v1:latest") == "model_v1_latest"

    def test_with_dots_and_dashes(self):
        assert _safe_dirname("model-v1.0") == "model-v1.0"


class TestSaveAndLoad:
    def test_save_and_load_dict(self, persistence: ArtifactPersistence):
        obj = {"weights": [1.0, 2.0, 3.0], "bias": 0.5}
        metadata = {"name": "model", "node_id": 1, "type_name": "dict", "module": "builtins"}

        persistence.save("model", obj, metadata, flow_id=0)
        loaded = persistence.load("model", flow_id=0)

        assert loaded == obj

    def test_save_and_load_list(self, persistence: ArtifactPersistence):
        obj = [1, 2, 3, "hello"]
        metadata = {"name": "data", "node_id": 2, "type_name": "list", "module": "builtins"}

        persistence.save("data", obj, metadata, flow_id=0)
        loaded = persistence.load("data", flow_id=0)

        assert loaded == obj

    def test_save_and_load_none(self, persistence: ArtifactPersistence):
        metadata = {"name": "nothing", "node_id": 1, "type_name": "NoneType", "module": "builtins"}

        persistence.save("nothing", None, metadata, flow_id=0)
        loaded = persistence.load("nothing", flow_id=0)

        assert loaded is None

    def test_save_and_load_lambda(self, persistence: ArtifactPersistence):
        """cloudpickle handles lambdas that standard pickle cannot."""
        fn = lambda x: x * 2  # noqa: E731
        metadata = {"name": "fn", "node_id": 1, "type_name": "function", "module": "__main__"}

        persistence.save("fn", fn, metadata, flow_id=0)
        loaded = persistence.load("fn", flow_id=0)

        assert loaded(5) == 10

    def test_save_and_load_custom_class(self, persistence: ArtifactPersistence):
        class MyModel:
            def __init__(self, w):
                self.w = w

            def predict(self, x):
                return x * self.w

        obj = MyModel(3.0)
        metadata = {"name": "custom", "node_id": 1, "type_name": "MyModel", "module": "__main__"}

        persistence.save("custom", obj, metadata, flow_id=0)
        loaded = persistence.load("custom", flow_id=0)

        assert loaded.predict(4) == 12.0

    def test_load_nonexistent_raises(self, persistence: ArtifactPersistence):
        with pytest.raises(FileNotFoundError, match="No persisted artifact"):
            persistence.load("nonexistent", flow_id=0)

    def test_metadata_written(self, persistence: ArtifactPersistence):
        metadata = {"name": "item", "node_id": 5, "type_name": "int", "module": "builtins"}
        persistence.save("item", 42, metadata, flow_id=0)

        loaded_meta = persistence.load_metadata("item", flow_id=0)
        assert loaded_meta is not None
        assert loaded_meta["name"] == "item"
        assert loaded_meta["node_id"] == 5
        assert "checksum" in loaded_meta
        assert "persisted_at" in loaded_meta
        assert "data_size_bytes" in loaded_meta

    def test_checksum_validation(self, persistence: ArtifactPersistence):
        metadata = {"name": "item", "node_id": 1, "type_name": "int", "module": "builtins"}
        persistence.save("item", 42, metadata, flow_id=0)

        # Corrupt the data file
        data_path = persistence._data_path(0, "item")
        data_path.write_bytes(b"corrupted data")

        with pytest.raises(ValueError, match="Checksum mismatch"):
            persistence.load("item", flow_id=0)


class TestFlowIsolation:
    def test_same_name_different_flows(self, persistence: ArtifactPersistence):
        meta1 = {"name": "model", "node_id": 1, "type_name": "str", "module": "builtins"}
        meta2 = {"name": "model", "node_id": 2, "type_name": "str", "module": "builtins"}

        persistence.save("model", "flow1_model", meta1, flow_id=1)
        persistence.save("model", "flow2_model", meta2, flow_id=2)

        assert persistence.load("model", flow_id=1) == "flow1_model"
        assert persistence.load("model", flow_id=2) == "flow2_model"

    def test_delete_scoped_to_flow(self, persistence: ArtifactPersistence):
        meta = {"name": "model", "node_id": 1, "type_name": "str", "module": "builtins"}
        persistence.save("model", "v1", meta, flow_id=1)
        persistence.save("model", "v2", meta, flow_id=2)

        persistence.delete("model", flow_id=1)

        with pytest.raises(FileNotFoundError):
            persistence.load("model", flow_id=1)
        assert persistence.load("model", flow_id=2) == "v2"


class TestDelete:
    def test_delete_removes_files(self, persistence: ArtifactPersistence):
        meta = {"name": "temp", "node_id": 1, "type_name": "int", "module": "builtins"}
        persistence.save("temp", 42, meta, flow_id=0)
        persistence.delete("temp", flow_id=0)

        with pytest.raises(FileNotFoundError):
            persistence.load("temp", flow_id=0)

    def test_delete_nonexistent_is_safe(self, persistence: ArtifactPersistence):
        # Should not raise
        persistence.delete("nonexistent", flow_id=0)


class TestClear:
    def test_clear_all(self, persistence: ArtifactPersistence):
        meta = {"name": "a", "node_id": 1, "type_name": "int", "module": "builtins"}
        persistence.save("a", 1, meta, flow_id=1)
        persistence.save("b", 2, {**meta, "name": "b"}, flow_id=2)

        persistence.clear()

        assert persistence.list_persisted() == {}

    def test_clear_by_flow_id(self, persistence: ArtifactPersistence):
        meta = {"name": "a", "node_id": 1, "type_name": "int", "module": "builtins"}
        persistence.save("a", 1, meta, flow_id=1)
        persistence.save("b", 2, {**meta, "name": "b"}, flow_id=2)

        persistence.clear(flow_id=1)

        persisted = persistence.list_persisted()
        assert len(persisted) == 1
        assert (2, "b") in persisted


class TestListPersisted:
    def test_empty(self, persistence: ArtifactPersistence):
        assert persistence.list_persisted() == {}

    def test_lists_all(self, persistence: ArtifactPersistence):
        meta = {"name": "a", "node_id": 1, "type_name": "int", "module": "builtins"}
        persistence.save("a", 1, meta, flow_id=1)
        persistence.save("b", 2, {**meta, "name": "b"}, flow_id=2)

        persisted = persistence.list_persisted()
        assert len(persisted) == 2
        assert (1, "a") in persisted
        assert (2, "b") in persisted

    def test_filter_by_flow_id(self, persistence: ArtifactPersistence):
        meta = {"name": "a", "node_id": 1, "type_name": "int", "module": "builtins"}
        persistence.save("a", 1, meta, flow_id=1)
        persistence.save("b", 2, {**meta, "name": "b"}, flow_id=2)

        persisted = persistence.list_persisted(flow_id=1)
        assert len(persisted) == 1
        assert (1, "a") in persisted


class TestDiskUsage:
    def test_disk_usage_increases(self, persistence: ArtifactPersistence):
        assert persistence.disk_usage_bytes() == 0

        meta = {"name": "big", "node_id": 1, "type_name": "bytes", "module": "builtins"}
        persistence.save("big", b"x" * 10000, meta, flow_id=0)

        assert persistence.disk_usage_bytes() > 10000


class TestCleanup:
    def test_cleanup_by_age(self, persistence: ArtifactPersistence):
        meta = {"name": "old", "node_id": 1, "type_name": "int", "module": "builtins"}
        persistence.save("old", 1, meta, flow_id=0)

        # Manually backdate the persisted_at in metadata
        meta_path = persistence._meta_path(0, "old")
        meta_data = json.loads(meta_path.read_text())
        meta_data["persisted_at"] = "2020-01-01T00:00:00+00:00"
        meta_path.write_text(json.dumps(meta_data))

        removed = persistence.cleanup(max_age_hours=1)
        assert removed == 1
        assert persistence.list_persisted() == {}

    def test_cleanup_by_name(self, persistence: ArtifactPersistence):
        meta = {"name": "a", "node_id": 1, "type_name": "int", "module": "builtins"}
        persistence.save("a", 1, meta, flow_id=0)
        persistence.save("b", 2, {**meta, "name": "b"}, flow_id=0)

        removed = persistence.cleanup(names=[(0, "a")])
        assert removed == 1

        persisted = persistence.list_persisted()
        assert len(persisted) == 1
        assert (0, "b") in persisted

    def test_cleanup_keeps_recent(self, persistence: ArtifactPersistence):
        meta = {"name": "recent", "node_id": 1, "type_name": "int", "module": "builtins"}
        persistence.save("recent", 1, meta, flow_id=0)

        removed = persistence.cleanup(max_age_hours=24)
        assert removed == 0
        assert len(persistence.list_persisted()) == 1


class TestRecoveryMode:
    def test_enum_values(self):
        assert RecoveryMode.LAZY == "lazy"
        assert RecoveryMode.EAGER == "eager"
        assert RecoveryMode.CLEAR == "clear"

    def test_from_string(self):
        assert RecoveryMode("lazy") == RecoveryMode.LAZY
        assert RecoveryMode("eager") == RecoveryMode.EAGER
        assert RecoveryMode("clear") == RecoveryMode.CLEAR

    def test_none_backwards_compatibility(self):
        """'none' is accepted for backwards compatibility but maps to CLEAR."""
        assert RecoveryMode("none") == RecoveryMode.CLEAR
