"""Tests for kernel_runtime.flowfile_client."""

from pathlib import Path

import polars as pl
import pytest

from kernel_runtime.artifact_store import ArtifactStore
from kernel_runtime import flowfile_client


@pytest.fixture(autouse=True)
def _reset_context():
    """Ensure context is cleared before and after each test."""
    flowfile_client._clear_context()
    yield
    flowfile_client._clear_context()


@pytest.fixture()
def ctx(tmp_dir: Path) -> dict:
    """Set up a standard context and return its parameters."""
    store = ArtifactStore()
    input_dir = tmp_dir / "inputs"
    output_dir = tmp_dir / "outputs"
    input_dir.mkdir()
    output_dir.mkdir()

    # Write a default input parquet
    df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
    main_path = input_dir / "main.parquet"
    df.write_parquet(str(main_path))

    flowfile_client._set_context(
        node_id=1,
        input_paths={"main": str(main_path)},
        output_dir=str(output_dir),
        artifact_store=store,
    )
    return {
        "store": store,
        "input_dir": input_dir,
        "output_dir": output_dir,
        "main_path": main_path,
    }


class TestContextManagement:
    def test_missing_context_raises(self):
        with pytest.raises(RuntimeError, match="context not initialized"):
            flowfile_client.read_input()

    def test_set_and_clear(self, tmp_dir: Path):
        store = ArtifactStore()
        flowfile_client._set_context(
            node_id=1,
            input_paths={},
            output_dir=str(tmp_dir),
            artifact_store=store,
        )
        # Should not raise
        flowfile_client._get_context_value("node_id")

        flowfile_client._clear_context()
        with pytest.raises(RuntimeError):
            flowfile_client._get_context_value("node_id")


class TestReadInput:
    def test_read_main_input(self, ctx: dict):
        lf = flowfile_client.read_input()
        assert isinstance(lf, pl.LazyFrame)
        df = lf.collect()
        assert set(df.columns) == {"x", "y"}
        assert len(df) == 3

    def test_read_named_input(self, ctx: dict):
        lf = flowfile_client.read_input("main")
        df = lf.collect()
        assert df["x"].to_list() == [1, 2, 3]

    def test_read_missing_input_raises(self, ctx: dict):
        with pytest.raises(KeyError, match="not found"):
            flowfile_client.read_input("nonexistent")

    def test_read_inputs_returns_dict(self, ctx: dict):
        inputs = flowfile_client.read_inputs()
        assert isinstance(inputs, dict)
        assert "main" in inputs
        assert isinstance(inputs["main"], pl.LazyFrame)


class TestReadMultipleInputs:
    def test_multiple_named_inputs(self, tmp_dir: Path):
        store = ArtifactStore()
        input_dir = tmp_dir / "inputs"
        input_dir.mkdir(exist_ok=True)

        left_path = input_dir / "left.parquet"
        right_path = input_dir / "right.parquet"
        pl.DataFrame({"id": [1, 2]}).write_parquet(str(left_path))
        pl.DataFrame({"id": [3, 4]}).write_parquet(str(right_path))

        flowfile_client._set_context(
            node_id=2,
            input_paths={"left": str(left_path), "right": str(right_path)},
            output_dir=str(tmp_dir / "outputs"),
            artifact_store=store,
        )

        inputs = flowfile_client.read_inputs()
        assert set(inputs.keys()) == {"left", "right"}
        assert inputs["left"].collect()["id"].to_list() == [1, 2]
        assert inputs["right"].collect()["id"].to_list() == [3, 4]


class TestPublishOutput:
    def test_publish_dataframe(self, ctx: dict):
        df = pl.DataFrame({"a": [1, 2]})
        flowfile_client.publish_output(df)
        out = Path(ctx["output_dir"]) / "main.parquet"
        assert out.exists()
        result = pl.read_parquet(str(out))
        assert result["a"].to_list() == [1, 2]

    def test_publish_lazyframe(self, ctx: dict):
        lf = pl.LazyFrame({"b": [10, 20]})
        flowfile_client.publish_output(lf)
        out = Path(ctx["output_dir"]) / "main.parquet"
        assert out.exists()
        result = pl.read_parquet(str(out))
        assert result["b"].to_list() == [10, 20]

    def test_publish_named_output(self, ctx: dict):
        df = pl.DataFrame({"c": [5]})
        flowfile_client.publish_output(df, name="custom")
        out = Path(ctx["output_dir"]) / "custom.parquet"
        assert out.exists()

    def test_publish_creates_output_dir(self, tmp_dir: Path):
        store = ArtifactStore()
        new_output = tmp_dir / "new" / "nested"
        flowfile_client._set_context(
            node_id=1,
            input_paths={},
            output_dir=str(new_output),
            artifact_store=store,
        )
        df = pl.DataFrame({"v": [1]})
        flowfile_client.publish_output(df)
        assert (new_output / "main.parquet").exists()


class TestArtifacts:
    def test_publish_and_read_artifact(self, ctx: dict):
        flowfile_client.publish_artifact("my_dict", {"key": "value"})
        result = flowfile_client.read_artifact("my_dict")
        assert result == {"key": "value"}

    def test_list_artifacts(self, ctx: dict):
        flowfile_client.publish_artifact("a", 1)
        flowfile_client.publish_artifact("b", [2, 3])
        listing = flowfile_client.list_artifacts()
        assert set(listing.keys()) == {"a", "b"}

    def test_read_missing_artifact_raises(self, ctx: dict):
        with pytest.raises(KeyError, match="not found"):
            flowfile_client.read_artifact("missing")
