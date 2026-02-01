"""Unit tests for flowfile_core.flowfile.artifacts."""

from datetime import datetime

import pytest

from flowfile_core.flowfile.artifacts import ArtifactContext, ArtifactRef, NodeArtifactState


# ---------------------------------------------------------------------------
# ArtifactRef
# ---------------------------------------------------------------------------


class TestArtifactRef:
    def test_create_ref(self):
        ref = ArtifactRef(name="model", source_node_id=1, kernel_id="k1")
        assert ref.name == "model"
        assert ref.source_node_id == 1
        assert ref.kernel_id == "k1"
        assert isinstance(ref.created_at, datetime)

    def test_refs_are_hashable(self):
        """Frozen dataclass instances can be used in sets / as dict keys."""
        ref = ArtifactRef(name="model", source_node_id=1)
        assert hash(ref) is not None
        s = {ref}
        assert ref in s

    def test_refs_equality(self):
        ts = datetime(2025, 1, 1)
        a = ArtifactRef(name="x", source_node_id=1, created_at=ts)
        b = ArtifactRef(name="x", source_node_id=1, created_at=ts)
        assert a == b

    def test_to_dict(self):
        ref = ArtifactRef(
            name="model",
            source_node_id=1,
            kernel_id="k1",
            type_name="RandomForest",
            module="sklearn.ensemble",
            size_bytes=1024,
        )
        d = ref.to_dict()
        assert d["name"] == "model"
        assert d["source_node_id"] == 1
        assert d["kernel_id"] == "k1"
        assert d["type_name"] == "RandomForest"
        assert d["module"] == "sklearn.ensemble"
        assert d["size_bytes"] == 1024
        assert "created_at" in d


# ---------------------------------------------------------------------------
# NodeArtifactState
# ---------------------------------------------------------------------------


class TestNodeArtifactState:
    def test_defaults(self):
        state = NodeArtifactState()
        assert state.published == []
        assert state.available == {}
        assert state.consumed == []

    def test_to_dict(self):
        ref = ArtifactRef(name="m", source_node_id=1, kernel_id="k")
        state = NodeArtifactState(published=[ref], available={"m": ref}, consumed=["m"])
        d = state.to_dict()
        assert len(d["published"]) == 1
        assert "m" in d["available"]
        assert d["consumed"] == ["m"]


# ---------------------------------------------------------------------------
# ArtifactContext — Recording
# ---------------------------------------------------------------------------


class TestArtifactContextRecording:
    def test_record_published_with_dict(self):
        ctx = ArtifactContext()
        refs = ctx.record_published(
            node_id=1,
            kernel_id="k1",
            artifacts=[{"name": "model", "type_name": "RF"}],
        )
        assert len(refs) == 1
        assert refs[0].name == "model"
        assert refs[0].type_name == "RF"
        assert refs[0].source_node_id == 1
        assert refs[0].kernel_id == "k1"

    def test_record_published_with_string_list(self):
        ctx = ArtifactContext()
        refs = ctx.record_published(node_id=2, kernel_id="k1", artifacts=["a", "b"])
        assert len(refs) == 2
        assert refs[0].name == "a"
        assert refs[1].name == "b"

    def test_record_published_multiple_nodes(self):
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.record_published(2, "k1", ["encoder"])
        assert len(ctx.get_published_by_node(1)) == 1
        assert len(ctx.get_published_by_node(2)) == 1

    def test_record_published_updates_kernel_artifacts(self):
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ka = ctx.get_kernel_artifacts("k1")
        assert "model" in ka
        assert ka["model"].source_node_id == 1

    def test_record_consumed(self):
        ctx = ArtifactContext()
        ctx.record_consumed(5, ["model", "scaler"])
        state = ctx._node_states[5]
        assert state.consumed == ["model", "scaler"]


# ---------------------------------------------------------------------------
# ArtifactContext — Availability
# ---------------------------------------------------------------------------


class TestArtifactContextAvailability:
    def test_compute_available_from_direct_upstream(self):
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        avail = ctx.compute_available(node_id=2, kernel_id="k1", upstream_node_ids=[1])
        assert "model" in avail
        assert avail["model"].source_node_id == 1

    def test_compute_available_transitive(self):
        """Node 3 should see artifacts from node 1 via node 2."""
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        # Node 2 doesn't publish anything
        # Node 3 lists both 1 and 2 as upstream
        avail = ctx.compute_available(node_id=3, kernel_id="k1", upstream_node_ids=[1, 2])
        assert "model" in avail

    def test_compute_available_different_kernels_isolated(self):
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        avail = ctx.compute_available(node_id=2, kernel_id="k2", upstream_node_ids=[1])
        assert avail == {}

    def test_compute_available_same_kernel_visible(self):
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        avail = ctx.compute_available(node_id=2, kernel_id="k1", upstream_node_ids=[1])
        assert "model" in avail

    def test_compute_available_stores_on_node_state(self):
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.compute_available(node_id=2, kernel_id="k1", upstream_node_ids=[1])
        assert "model" in ctx.get_available_for_node(2)

    def test_compute_available_no_upstream_returns_empty(self):
        ctx = ArtifactContext()
        avail = ctx.compute_available(node_id=1, kernel_id="k1", upstream_node_ids=[])
        assert avail == {}

    def test_compute_available_multiple_artifacts(self):
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model", "scaler"])
        ctx.record_published(2, "k1", ["encoder"])
        avail = ctx.compute_available(node_id=3, kernel_id="k1", upstream_node_ids=[1, 2])
        assert set(avail.keys()) == {"model", "scaler", "encoder"}

    def test_compute_available_overwrites_previous(self):
        """Re-computing availability replaces old data."""
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.compute_available(node_id=2, kernel_id="k1", upstream_node_ids=[1])
        # Re-compute with no upstream
        ctx.compute_available(node_id=2, kernel_id="k1", upstream_node_ids=[])
        assert ctx.get_available_for_node(2) == {}


# ---------------------------------------------------------------------------
# ArtifactContext — Deletion tracking
# ---------------------------------------------------------------------------


class TestArtifactContextDeletion:
    def test_record_deleted_removes_from_kernel_index(self):
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.record_deleted(2, "k1", ["model"])
        assert ctx.get_kernel_artifacts("k1") == {}

    def test_record_deleted_removes_from_published_lists(self):
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model", "scaler"])
        ctx.record_deleted(2, "k1", ["model"])
        published = ctx.get_published_by_node(1)
        names = [r.name for r in published]
        assert "model" not in names
        assert "scaler" in names

    def test_record_deleted_tracks_on_node_state(self):
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.record_deleted(2, "k1", ["model"])
        state = ctx._node_states[2]
        assert "model" in state.deleted

    def test_deleted_artifact_not_available_downstream(self):
        """If node 2 deletes an artifact published by node 1,
        node 3 should not see it as available."""
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.record_deleted(2, "k1", ["model"])
        avail = ctx.compute_available(node_id=3, kernel_id="k1", upstream_node_ids=[1, 2])
        assert "model" not in avail

    def test_delete_and_republish_flow(self):
        """Node 1 publishes, node 2 deletes, node 3 re-publishes,
        node 4 should see the new version."""
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.record_deleted(2, "k1", ["model"])
        ctx.record_published(3, "k1", ["model"])
        avail = ctx.compute_available(node_id=4, kernel_id="k1", upstream_node_ids=[1, 2, 3])
        assert "model" in avail
        assert avail["model"].source_node_id == 3


# ---------------------------------------------------------------------------
# ArtifactContext — Clearing
# ---------------------------------------------------------------------------


class TestArtifactContextClearing:
    def test_clear_kernel_removes_only_that_kernel(self):
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.record_published(2, "k2", ["encoder"])
        ctx.clear_kernel("k1")
        assert ctx.get_kernel_artifacts("k1") == {}
        assert "encoder" in ctx.get_kernel_artifacts("k2")

    def test_clear_kernel_removes_from_node_states(self):
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.record_published(1, "k2", ["encoder"])
        ctx.clear_kernel("k1")
        published = ctx.get_published_by_node(1)
        names = [r.name for r in published]
        assert "model" not in names
        assert "encoder" in names

    def test_clear_kernel_removes_from_available(self):
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.compute_available(node_id=2, kernel_id="k1", upstream_node_ids=[1])
        ctx.clear_kernel("k1")
        assert ctx.get_available_for_node(2) == {}

    def test_clear_all_removes_everything(self):
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.record_published(2, "k2", ["encoder"])
        ctx.compute_available(node_id=3, kernel_id="k1", upstream_node_ids=[1])
        ctx.clear_all()
        assert ctx.get_published_by_node(1) == []
        assert ctx.get_published_by_node(2) == []
        assert ctx.get_available_for_node(3) == {}
        assert ctx.get_kernel_artifacts("k1") == {}
        assert ctx.get_kernel_artifacts("k2") == {}
        assert ctx.get_all_artifacts() == {}


# ---------------------------------------------------------------------------
# ArtifactContext — Selective node clearing
# ---------------------------------------------------------------------------


class TestArtifactContextClearNodes:
    def test_clear_nodes_removes_only_target(self):
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.record_published(2, "k1", ["encoder"])
        ctx.clear_nodes({1})
        assert ctx.get_published_by_node(1) == []
        assert len(ctx.get_published_by_node(2)) == 1
        assert ctx.get_kernel_artifacts("k1") == {"encoder": ctx.get_published_by_node(2)[0]}

    def test_clear_nodes_preserves_other_node_metadata(self):
        """Clearing node 2 should leave node 1's artifacts intact."""
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.record_published(2, "k1", ["scaler"])
        ctx.clear_nodes({2})
        published_1 = ctx.get_published_by_node(1)
        assert len(published_1) == 1
        assert published_1[0].name == "model"
        ka = ctx.get_kernel_artifacts("k1")
        assert "model" in ka
        assert "scaler" not in ka

    def test_clear_nodes_empty_set(self):
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.clear_nodes(set())
        assert len(ctx.get_published_by_node(1)) == 1

    def test_clear_nodes_nonexistent(self):
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.clear_nodes({99})  # Should not raise
        assert len(ctx.get_published_by_node(1)) == 1

    def test_clear_nodes_allows_re_record(self):
        """After clearing, the node can re-record new artifacts."""
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.clear_nodes({1})
        ctx.record_published(1, "k1", ["model_v2"])
        published = ctx.get_published_by_node(1)
        assert len(published) == 1
        assert published[0].name == "model_v2"

    def test_clear_nodes_updates_publisher_index(self):
        """Publisher index should be cleaned up when a node is cleared."""
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.clear_nodes({1})
        # After clearing, the artifact should not show up as available
        avail = ctx.compute_available(node_id=2, kernel_id="k1", upstream_node_ids=[1])
        assert avail == {}

    def test_clear_nodes_preserves_upstream_for_downstream(self):
        """Simulates debug mode: node 1 is skipped (not cleared),
        node 2 is re-running (cleared). Node 3 should still see node 1's artifact."""
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.record_published(2, "k1", ["predictions"])
        # Clear only node 2 (it will re-run)
        ctx.clear_nodes({2})
        # Node 3 should still see "model" from node 1
        avail = ctx.compute_available(node_id=3, kernel_id="k1", upstream_node_ids=[1, 2])
        assert "model" in avail
        assert "predictions" not in avail


# ---------------------------------------------------------------------------
# ArtifactContext — Queries
# ---------------------------------------------------------------------------


class TestArtifactContextQueries:
    def test_get_published_by_node_returns_empty_for_unknown(self):
        ctx = ArtifactContext()
        assert ctx.get_published_by_node(999) == []

    def test_get_available_for_node_returns_empty_for_unknown(self):
        ctx = ArtifactContext()
        assert ctx.get_available_for_node(999) == {}

    def test_get_kernel_artifacts(self):
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["a", "b"])
        ka = ctx.get_kernel_artifacts("k1")
        assert set(ka.keys()) == {"a", "b"}

    def test_get_kernel_artifacts_empty(self):
        ctx = ArtifactContext()
        assert ctx.get_kernel_artifacts("nonexistent") == {}

    def test_get_all_artifacts(self):
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.record_published(2, "k2", ["encoder"])
        all_arts = ctx.get_all_artifacts()
        assert set(all_arts.keys()) == {"model", "encoder"}

    def test_get_all_artifacts_empty(self):
        ctx = ArtifactContext()
        assert ctx.get_all_artifacts() == {}


# ---------------------------------------------------------------------------
# ArtifactContext — Serialisation
# ---------------------------------------------------------------------------


class TestArtifactContextSerialization:
    def test_to_dict_structure(self):
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", [{"name": "model", "type_name": "RF"}])
        ctx.compute_available(node_id=2, kernel_id="k1", upstream_node_ids=[1])
        d = ctx.to_dict()
        assert "nodes" in d
        assert "kernels" in d
        assert "1" in d["nodes"]
        assert "2" in d["nodes"]
        assert "k1" in d["kernels"]
        assert "model" in d["kernels"]["k1"]

    def test_to_dict_empty_context(self):
        ctx = ArtifactContext()
        d = ctx.to_dict()
        assert d == {"nodes": {}, "kernels": {}}

    def test_to_dict_is_json_serialisable(self):
        import json

        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        d = ctx.to_dict()
        # Should not raise
        serialised = json.dumps(d)
        assert isinstance(serialised, str)
