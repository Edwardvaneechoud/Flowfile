"""Unit tests for flowfile_core.flowfile.artifacts."""

from datetime import datetime

import pytest

from flowfile_core.flowfile.artifacts import (
    ArtifactContext,
    ArtifactRef,
    NodeArtifactState,
    merge_declared_artifacts,
)
from flowfile_core.kernel.models import ExecuteResult, PublishedArtifact


# ArtifactRef


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


# NodeArtifactState


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


# ArtifactContext — Recording


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

    def test_record_published_appends_without_dedup(self):
        """``record_published`` is append-only — callers must ``clear_nodes``
        between re-executions or ``state.published`` grows unboundedly.

        Regression for: re-running a Python Script node that publishes
        ``"name"`` repeatedly used to make the UI's Published panel show
        duplicate entries. The fix lives at the call site
        (``flow_graph._execute_python_in_kernel`` clears the node's state
        before recording), not here — this test pins the append-only
        contract so a future "fix" of the contract here would surface
        through the call-site test instead.
        """
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["name"])
        ctx.record_published(1, "k1", ["name"])
        ctx.record_published(1, "k1", ["name"])
        assert len(ctx.get_published_by_node(1)) == 3
        # But the kernel-wide index dedupes (latest publish wins for a name)
        assert len(ctx.get_kernel_artifacts("k1")) == 1


# ArtifactContext — Availability


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
        ctx.compute_available(node_id=2, kernel_id="k1", upstream_node_ids=[])
        assert ctx.get_available_for_node(2) == {}


# ArtifactContext — Deletion tracking


class TestArtifactContextDeletion:
    def test_record_deleted_removes_from_kernel_index(self):
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.record_deleted(2, "k1", ["model"])
        assert ctx.get_kernel_artifacts("k1") == {}

    def test_record_deleted_preserves_publisher_published_list(self):
        """Deletion does NOT remove from publisher's published list (historical record)."""
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model", "scaler"])
        ctx.record_deleted(2, "k1", ["model"])
        published = ctx.get_published_by_node(1)
        names = [r.name for r in published]
        assert "model" in names
        assert "scaler" in names
        state = ctx._node_states[2]
        assert "model" in state.deleted

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


# ArtifactContext — Clearing


class TestArtifactContextClearing:
    def test_clear_kernel_removes_only_that_kernel(self):
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.record_published(2, "k2", ["encoder"])
        ctx.clear_kernel("k1")
        assert ctx.get_kernel_artifacts("k1") == {}
        assert "encoder" in ctx.get_kernel_artifacts("k2")

    def test_clear_kernel_preserves_published_lists(self):
        """clear_kernel removes from kernel index but preserves published (historical record)."""
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.record_published(1, "k2", ["encoder"])
        ctx.clear_kernel("k1")
        published = ctx.get_published_by_node(1)
        names = [r.name for r in published]
        assert "model" in names
        assert "encoder" in names
        assert ctx.get_kernel_artifacts("k1") == {}

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


# ArtifactContext — Selective node clearing


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
        avail = ctx.compute_available(node_id=2, kernel_id="k1", upstream_node_ids=[1])
        assert avail == {}

    def test_clear_nodes_preserves_upstream_for_downstream(self):
        """Simulates debug mode: node 1 is skipped (not cleared),
        node 2 is re-running (cleared). Node 3 should still see node 1's artifact."""
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.record_published(2, "k1", ["predictions"])
        ctx.clear_nodes({2})
        avail = ctx.compute_available(node_id=3, kernel_id="k1", upstream_node_ids=[1, 2])
        assert "model" in avail
        assert "predictions" not in avail


# ArtifactContext — Queries


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


# ArtifactContext — Serialisation


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


# ArtifactContext — Deletion origin tracking


class TestArtifactContextDeletionOrigins:
    def test_get_producer_nodes_for_deletions_basic(self):
        """Deleting an artifact tracks the original publisher."""
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.record_deleted(2, "k1", ["model"])
        producers = ctx.get_producer_nodes_for_deletions({2})
        assert producers == {1}

    def test_get_producer_nodes_for_deletions_no_deletions(self):
        """Nodes without deletions return an empty set."""
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        producers = ctx.get_producer_nodes_for_deletions({1})
        assert producers == set()

    def test_get_producer_nodes_for_deletions_multiple_artifacts(self):
        """Deleting multiple artifacts from different producers."""
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.record_published(2, "k1", ["scaler"])
        ctx.record_deleted(3, "k1", ["model", "scaler"])
        producers = ctx.get_producer_nodes_for_deletions({3})
        assert producers == {1, 2}

    def test_clear_nodes_removes_deletion_origins(self):
        """Clearing a deleter node also clears its deletion origins."""
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.record_deleted(2, "k1", ["model"])
        ctx.clear_nodes({2})
        producers = ctx.get_producer_nodes_for_deletions({2})
        assert producers == set()

    def test_clear_all_removes_deletion_origins(self):
        """clear_all removes all deletion origin tracking."""
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.record_deleted(2, "k1", ["model"])
        ctx.clear_all()
        producers = ctx.get_producer_nodes_for_deletions({2})
        assert producers == set()

    def test_clear_kernel_removes_deletion_origins(self):
        """clear_kernel removes deletion origins for that kernel only."""
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.record_published(2, "k2", ["encoder"])
        ctx.record_deleted(3, "k1", ["model"])
        ctx.record_deleted(3, "k2", ["encoder"])
        ctx.clear_kernel("k1")
        producers = ctx.get_producer_nodes_for_deletions({3})
        assert producers == {2}


# ArtifactContext — Independent chain isolation


class TestArtifactContextChainIsolation:
    """Verify that artifacts from independent DAG chains are not visible
    to nodes in other chains, even when they share the same kernel."""

    def test_independent_chains_are_isolated(self):
        """Chain A (nodes 4→5→6) and chain B (nodes 7→8→9) share kernel 'ds'.
        Node 9 should only see artifacts from its own chain (8), not from
        chain A (node 5).
        """
        ctx = ArtifactContext()
        ctx.record_published(5, "ds", [{"name": "linear_model", "type_name": "dict"}])
        ctx.record_published(8, "ds", [
            {"name": "graph", "type_name": "Graph"},
            {"name": "centrality", "type_name": "dict"},
        ])

        avail_6 = ctx.compute_available(node_id=6, kernel_id="ds", upstream_node_ids=[5, 4])
        assert "linear_model" in avail_6
        assert "graph" not in avail_6
        assert "centrality" not in avail_6

        avail_9 = ctx.compute_available(node_id=9, kernel_id="ds", upstream_node_ids=[8, 7])
        assert "graph" in avail_9
        assert "centrality" in avail_9
        assert "linear_model" not in avail_9

    def test_kernel_artifacts_returns_all_regardless_of_chain(self):
        """get_kernel_artifacts returns ALL artifacts in the kernel, which is
        the data source the frontend was using before the fix."""
        ctx = ArtifactContext()
        ctx.record_published(5, "ds", ["linear_model"])
        ctx.record_published(8, "ds", ["graph", "centrality"])

        all_kernel = ctx.get_kernel_artifacts("ds")
        assert set(all_kernel.keys()) == {"linear_model", "graph", "centrality"}

    def test_upstream_ids_determine_visibility(self):
        """Only the upstream_node_ids list determines what a node can see.
        Nodes not in the upstream list are invisible."""
        ctx = ArtifactContext()
        ctx.record_published(1, "k", ["a"])
        ctx.record_published(2, "k", ["b"])
        ctx.record_published(3, "k", ["c"])

        avail = ctx.compute_available(node_id=4, kernel_id="k", upstream_node_ids=[1])
        assert set(avail.keys()) == {"a"}

        avail = ctx.compute_available(node_id=5, kernel_id="k", upstream_node_ids=[2, 3])
        assert set(avail.keys()) == {"b", "c"}


# ArtifactContext — Allowlist map derivation (lineage sent to the kernel)


class TestArtifactAllowlistMap:
    """The {name: source_node_id} map core ships to the kernel is derived from
    the same ``compute_available`` result used for availability."""

    def test_allowlist_map_from_available(self):
        ctx = ArtifactContext()
        ctx.record_published(1, "k1", ["model"])
        ctx.record_published(2, "k1", ["scaler"])
        available = ctx.compute_available(node_id=3, kernel_id="k1", upstream_node_ids=[1, 2])
        allowlist = {name: ref.source_node_id for name, ref in available.items()}
        assert allowlist == {"model": 1, "scaler": 2}

    def test_allowlist_map_empty_when_nothing_upstream(self):
        ctx = ArtifactContext()
        available = ctx.compute_available(node_id=1, kernel_id="k1", upstream_node_ids=[])
        assert {name: ref.source_node_id for name, ref in available.items()} == {}


# ArtifactContext — Rich publish metadata round-trip


class TestArtifactRichMetadataRoundTrip:
    def test_record_published_rich_dicts_surface_in_summaries(self):
        """type_name/module from rich publish dicts flow into get_node_summaries."""
        ctx = ArtifactContext()
        ctx.record_published(
            1,
            "k1",
            [{"name": "model", "type_name": "RandomForestClassifier", "module": "sklearn.ensemble"}],
        )
        summaries = ctx.get_node_summaries()
        published = summaries["1"]["published"]
        assert published == [
            {
                "name": "model",
                "type_name": "RandomForestClassifier",
                "module": "sklearn.ensemble",
                "has_preview": False,
                "preview_mime": "",
            }
        ]


# ArtifactContext — Preview metadata round-trip


class TestArtifactPreviewMetadata:
    def test_record_published_carries_preview_fields(self):
        ctx = ArtifactContext()
        refs = ctx.record_published(
            1,
            "k1",
            [{"name": "chart", "has_preview": True, "preview_mime": "image/png"}],
        )
        assert refs[0].has_preview is True
        assert refs[0].preview_mime == "image/png"

    def test_to_dict_includes_preview_fields(self):
        ref = ArtifactRef(name="chart", source_node_id=1, has_preview=True, preview_mime="image/png")
        d = ref.to_dict()
        assert d["has_preview"] is True
        assert d["preview_mime"] == "image/png"

    def test_node_summaries_surface_preview_fields(self):
        ctx = ArtifactContext()
        ctx.record_published(
            1,
            "k1",
            [{"name": "chart", "has_preview": True, "preview_mime": "image/png"}],
        )
        published = ctx.get_node_summaries()["1"]["published"]
        assert published == [
            {
                "name": "chart",
                "type_name": "",
                "module": "",
                "has_preview": True,
                "preview_mime": "image/png",
            }
        ]

    def test_legacy_dict_without_keys_defaults(self):
        """Old kernel payloads omit the preview keys → defaults False/''."""
        ctx = ArtifactContext()
        refs = ctx.record_published(1, "k1", [{"name": "model"}])
        assert refs[0].has_preview is False
        assert refs[0].preview_mime == ""

    def test_none_preview_mime_normalizes_to_empty(self):
        """model_dump yields None for an unset preview_mime; it must become ''."""
        ctx = ArtifactContext()
        refs = ctx.record_published(1, "k1", [{"name": "model", "has_preview": False, "preview_mime": None}])
        assert refs[0].preview_mime == ""


# PublishedArtifact / ExecuteResult (kernel wire model) forward/backward compat


class TestPublishedArtifactCompat:
    def test_new_fields_default_on_legacy_payload(self):
        """A payload from an old kernel image lacks the preview keys → defaults."""
        art = PublishedArtifact(name="model", type_name="RandomForest", size_bytes=10)
        assert art.has_preview is False
        assert art.preview_mime is None

    def test_new_fields_parse_when_present(self):
        art = PublishedArtifact(name="chart", has_preview=True, preview_mime="image/png")
        assert art.has_preview is True
        assert art.preview_mime == "image/png"

    def test_normalize_published_maps_plain_strings(self):
        """Stale 0.4.0 kernels emit bare name strings; they degrade to names-only."""
        result = ExecuteResult(success=True, artifacts_published=["model", "scaler"])
        assert [a.name for a in result.artifacts_published] == ["model", "scaler"]
        assert result.artifacts_published[0].has_preview is False


# merge_declared_artifacts


class TestMergeDeclaredArtifacts:
    def test_observed_only(self):
        observed = {"model": ArtifactRef(name="model", source_node_id=1, kernel_id="k1", type_name="dict")}
        result = merge_declared_artifacts(observed, [], "k1")
        assert len(result) == 1
        assert result[0]["name"] == "model"
        assert result[0]["status"] == "published"
        assert result[0]["type_name"] == "dict"

    def test_declared_only_entry_shape(self):
        result = merge_declared_artifacts({}, [(2, "encoder", "sklearn.preprocessing.LabelEncoder")], "k1")
        assert result == [
            {
                "name": "encoder",
                "source_node_id": 2,
                "kernel_id": "k1",
                "type_name": "LabelEncoder",
                "module": "sklearn.preprocessing",
                "size_bytes": 0,
                "created_at": None,
                "status": "declared",
            }
        ]

    def test_observed_wins_on_conflict(self):
        observed = {"model": ArtifactRef(name="model", source_node_id=1, kernel_id="k1", type_name="RealType")}
        declared = [(1, "model", "declared.Type")]
        result = merge_declared_artifacts(observed, declared, "k1")
        assert len(result) == 1
        assert result[0]["status"] == "published"
        assert result[0]["type_name"] == "RealType"

    def test_type_rpartition_split(self):
        result = merge_declared_artifacts({}, [(3, "a", "a.b.c.MyType")], "k1")
        assert result[0]["module"] == "a.b.c"
        assert result[0]["type_name"] == "MyType"

    def test_no_dot_type(self):
        result = merge_declared_artifacts({}, [(3, "a", "MyType")], "k1")
        assert result[0]["module"] == ""
        assert result[0]["type_name"] == "MyType"

    def test_none_type(self):
        result = merge_declared_artifacts({}, [(3, "a", None)], "k1")
        assert result[0]["module"] == ""
        assert result[0]["type_name"] == ""

    def test_declared_duplicate_first_wins(self):
        declared = [(1, "shared", "first.A"), (2, "shared", "second.B")]
        result = merge_declared_artifacts({}, declared, "k1")
        assert len(result) == 1
        assert result[0]["source_node_id"] == 1
        assert result[0]["type_name"] == "A"

    def test_statuses_and_ordering(self):
        observed = {"published_art": ArtifactRef(name="published_art", source_node_id=1, kernel_id="k1")}
        declared = [(2, "declared_art", "mod.T"), (1, "published_art", "mod.Ignored")]
        result = merge_declared_artifacts(observed, declared, "k1")
        by_name = {r["name"]: r for r in result}
        assert by_name["published_art"]["status"] == "published"
        assert by_name["declared_art"]["status"] == "declared"
        assert len(result) == 2
