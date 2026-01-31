"""Unit tests for ArtifactContext."""

import pytest

from flowfile_core.flowfile.artifacts import ArtifactContext, ArtifactRef


class TestArtifactRef:
    def test_create_ref(self):
        ref = ArtifactRef(
            name="model",
            source_node_id=1,
            kernel_id="ml-kernel",
            type_name="RandomForestClassifier",
        )
        assert ref.name == "model"
        assert ref.source_node_id == 1

    def test_refs_are_hashable(self):
        ref1 = ArtifactRef(name="model", source_node_id=1, kernel_id="k1")
        ref2 = ArtifactRef(name="model", source_node_id=1, kernel_id="k1")
        ref3 = ArtifactRef(name="model", source_node_id=2, kernel_id="k1")

        assert ref1 == ref2
        assert ref1 != ref3
        assert len({ref1, ref2, ref3}) == 2


class TestArtifactContextRecording:
    def test_record_published_simple(self):
        ctx = ArtifactContext()

        refs = ctx.record_published(
            node_id=1,
            kernel_id="ml",
            artifacts=[{"name": "model", "type_name": "RandomForest"}],
        )

        assert len(refs) == 1
        assert refs[0].name == "model"
        assert refs[0].type_name == "RandomForest"

        # Should be queryable
        published = ctx.get_published_by_node(1)
        assert len(published) == 1
        assert published[0].name == "model"

    def test_record_published_string_list(self):
        """Handle case where artifacts is just a list of names."""
        ctx = ArtifactContext()

        refs = ctx.record_published(
            node_id=1,
            kernel_id="ml",
            artifacts=["model", "encoder"],
        )

        assert len(refs) == 2
        assert {r.name for r in refs} == {"model", "encoder"}

    def test_record_multiple_nodes(self):
        ctx = ArtifactContext()

        ctx.record_published(1, "ml", [{"name": "model"}])
        ctx.record_published(2, "ml", [{"name": "scaler"}])

        assert ctx.get_published_by_node(1)[0].name == "model"
        assert ctx.get_published_by_node(2)[0].name == "scaler"

    def test_kernel_artifacts_tracked(self):
        ctx = ArtifactContext()

        ctx.record_published(1, "ml", [{"name": "model"}])
        ctx.record_published(2, "ml", [{"name": "scaler"}])
        ctx.record_published(3, "gpu", [{"name": "embeddings"}])

        ml_artifacts = ctx.get_kernel_artifacts("ml")
        assert set(ml_artifacts.keys()) == {"model", "scaler"}

        gpu_artifacts = ctx.get_kernel_artifacts("gpu")
        assert set(gpu_artifacts.keys()) == {"embeddings"}

    def test_record_consumed(self):
        ctx = ArtifactContext()

        ctx.record_consumed(1, ["model", "scaler"])

        consumed = ctx.get_consumed_by_node(1)
        assert consumed == ["model", "scaler"]

    def test_get_all_artifacts(self):
        ctx = ArtifactContext()

        ctx.record_published(1, "ml", [{"name": "model"}])
        ctx.record_published(2, "gpu", [{"name": "embeddings"}])

        all_artifacts = ctx.get_all_artifacts()
        assert set(all_artifacts.keys()) == {"model", "embeddings"}


class TestArtifactContextAvailability:
    def test_compute_available_from_upstream(self):
        ctx = ArtifactContext()

        # Node 1 publishes model
        ctx.record_published(1, "ml", [{"name": "model"}])

        # Node 2 is downstream of node 1
        available = ctx.compute_available(
            node_id=2,
            kernel_id="ml",
            upstream_node_ids=[1],
        )

        assert "model" in available
        assert available["model"].source_node_id == 1

    def test_compute_available_transitive(self):
        """Artifacts flow transitively through the graph."""
        ctx = ArtifactContext()

        # Node 1 publishes model
        ctx.record_published(1, "ml", [{"name": "model"}])

        # Node 2 is downstream of 1, computes availability
        ctx.compute_available(2, "ml", upstream_node_ids=[1])

        # Node 3 is downstream of 2 (not directly of 1)
        available = ctx.compute_available(3, "ml", upstream_node_ids=[2])

        # Should still have access to model (transitive)
        assert "model" in available

    def test_compute_available_different_kernels_isolated(self):
        """Artifacts don't cross kernel boundaries."""
        ctx = ArtifactContext()

        # Node 1 publishes on kernel "ml"
        ctx.record_published(1, "ml", [{"name": "model"}])

        # Node 2 is on kernel "gpu" - should NOT see the model
        available = ctx.compute_available(
            node_id=2,
            kernel_id="gpu",
            upstream_node_ids=[1],
        )

        assert "model" not in available

    def test_compute_available_same_kernel_sees_artifact(self):
        """Artifacts are visible within same kernel."""
        ctx = ArtifactContext()

        ctx.record_published(1, "ml", [{"name": "model"}])

        available = ctx.compute_available(2, "ml", upstream_node_ids=[1])

        assert "model" in available

    def test_available_stored_on_node(self):
        ctx = ArtifactContext()

        ctx.record_published(1, "ml", [{"name": "model"}])
        ctx.compute_available(2, "ml", upstream_node_ids=[1])

        # Should be queryable later
        available = ctx.get_available_for_node(2)
        assert "model" in available

    def test_compute_available_no_upstream(self):
        """Node with no upstream has no available artifacts."""
        ctx = ArtifactContext()
        available = ctx.compute_available(1, "ml", upstream_node_ids=[])
        assert available == {}

    def test_compute_available_multiple_upstream(self):
        """Artifacts from multiple upstream nodes are merged."""
        ctx = ArtifactContext()

        ctx.record_published(1, "ml", [{"name": "model"}])
        ctx.record_published(2, "ml", [{"name": "scaler"}])

        available = ctx.compute_available(3, "ml", upstream_node_ids=[1, 2])

        assert "model" in available
        assert "scaler" in available


class TestArtifactContextClearing:
    def test_clear_kernel(self):
        ctx = ArtifactContext()

        ctx.record_published(1, "ml", [{"name": "model"}])
        ctx.record_published(2, "gpu", [{"name": "embeddings"}])

        ctx.clear_kernel("ml")

        assert ctx.get_kernel_artifacts("ml") == {}
        assert "embeddings" in ctx.get_kernel_artifacts("gpu")

    def test_clear_all(self):
        ctx = ArtifactContext()

        ctx.record_published(1, "ml", [{"name": "model"}])
        ctx.compute_available(2, "ml", upstream_node_ids=[1])

        ctx.clear_all()

        assert ctx.get_published_by_node(1) == []
        assert ctx.get_available_for_node(2) == {}
        assert ctx.get_kernel_artifacts("ml") == {}

    def test_clear_nonexistent_kernel(self):
        """Clearing a kernel that doesn't exist should not raise."""
        ctx = ArtifactContext()
        ctx.clear_kernel("nonexistent")


class TestArtifactContextSerialization:
    def test_to_dict(self):
        ctx = ArtifactContext()

        ctx.record_published(1, "ml", [{"name": "model", "type_name": "RF"}])
        ctx.compute_available(2, "ml", upstream_node_ids=[1])

        data = ctx.to_dict()

        assert "nodes" in data
        assert "kernels" in data
        assert 1 in data["nodes"]
        assert data["nodes"][1]["published"][0]["name"] == "model"
        assert data["nodes"][1]["published"][0]["type"] == "RF"

    def test_to_dict_empty(self):
        ctx = ArtifactContext()
        data = ctx.to_dict()
        assert data == {"nodes": {}, "kernels": {}}

    def test_to_dict_includes_availability(self):
        ctx = ArtifactContext()

        ctx.record_published(1, "ml", [{"name": "model"}])
        ctx.compute_available(2, "ml", upstream_node_ids=[1])

        data = ctx.to_dict()

        assert "model" in data["nodes"][2]["available"]
        assert "ml" in data["kernels"]
        assert "model" in data["kernels"]["ml"]


class TestArtifactContextEdgeCases:
    def test_get_published_nonexistent_node(self):
        ctx = ArtifactContext()
        assert ctx.get_published_by_node(999) == []

    def test_get_available_nonexistent_node(self):
        ctx = ArtifactContext()
        assert ctx.get_available_for_node(999) == {}

    def test_get_consumed_nonexistent_node(self):
        ctx = ArtifactContext()
        assert ctx.get_consumed_by_node(999) == []

    def test_get_kernel_artifacts_nonexistent(self):
        ctx = ArtifactContext()
        assert ctx.get_kernel_artifacts("nonexistent") == {}

    def test_artifact_override_same_name(self):
        """Later upstream node overrides earlier one for same artifact name."""
        ctx = ArtifactContext()

        ctx.record_published(1, "ml", [{"name": "model"}])
        ctx.record_published(2, "ml", [{"name": "model"}])

        # Node 2's model should win (last upstream in list)
        available = ctx.compute_available(3, "ml", upstream_node_ids=[1, 2])
        assert available["model"].source_node_id == 2
