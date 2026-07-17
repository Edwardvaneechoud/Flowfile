"""Unit tests for the ExecuteResult.artifacts_published normalizing validator."""

from flowfile_core.kernel.models import ExecuteResult, PublishedArtifact


class TestExecuteResultPublishedNormalization:
    def test_pure_string_list(self):
        """Stale 0.4.0 kernels emit plain names; they normalize to names-only refs."""
        result = ExecuteResult(success=True, artifacts_published=["model", "scaler"])
        assert all(isinstance(a, PublishedArtifact) for a in result.artifacts_published)
        assert [a.name for a in result.artifacts_published] == ["model", "scaler"]
        assert result.artifacts_published[0].type_name == ""
        assert result.artifacts_published[0].module == ""
        assert result.artifacts_published[0].size_bytes == 0

    def test_pure_rich_dicts(self):
        result = ExecuteResult(
            success=True,
            artifacts_published=[
                {"name": "model", "type_name": "RF", "module": "sklearn.ensemble", "size_bytes": 1024}
            ],
        )
        art = result.artifacts_published[0]
        assert isinstance(art, PublishedArtifact)
        assert art.name == "model"
        assert art.type_name == "RF"
        assert art.module == "sklearn.ensemble"
        assert art.size_bytes == 1024

    def test_mixed_strings_and_dicts(self):
        result = ExecuteResult(
            success=True,
            artifacts_published=["plain", {"name": "rich", "type_name": "dict"}],
        )
        assert all(isinstance(a, PublishedArtifact) for a in result.artifacts_published)
        by_name = {a.name: a for a in result.artifacts_published}
        assert by_name["plain"].type_name == ""
        assert by_name["rich"].type_name == "dict"

    def test_empty_default(self):
        result = ExecuteResult(success=True)
        assert result.artifacts_published == []

    def test_published_artifact_instances_pass_through(self):
        result = ExecuteResult(
            success=True,
            artifacts_published=[PublishedArtifact(name="x", type_name="T")],
        )
        assert result.artifacts_published[0].name == "x"
        assert result.artifacts_published[0].type_name == "T"
