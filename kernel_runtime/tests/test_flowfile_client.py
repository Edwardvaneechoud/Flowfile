"""Tests for kernel_runtime.flowfile_client."""

import datetime
import json
import os
from decimal import Decimal
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

    df = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
    main_path = input_dir / "main.parquet"
    df.write_parquet(str(main_path))

    flowfile_client._set_context(
        node_id=1,
        input_paths={"main": [str(main_path)]},
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

    def test_read_input_no_upstream_raises_runtime_error(self, tmp_dir: Path):
        store = ArtifactStore()
        flowfile_client._set_context(
            node_id=1,
            input_paths={},
            output_dir=str(tmp_dir),
            artifact_store=store,
        )
        with pytest.raises(RuntimeError, match="Upstream nodes did not run yet"):
            flowfile_client.read_input()

    def test_read_input_empty_paths_raises_runtime_error(self, tmp_dir: Path):
        store = ArtifactStore()
        flowfile_client._set_context(
            node_id=1,
            input_paths={"main": []},
            output_dir=str(tmp_dir),
            artifact_store=store,
        )
        with pytest.raises(RuntimeError, match="Upstream nodes did not run yet"):
            flowfile_client.read_input()

    def test_read_inputs_returns_dict(self, ctx: dict):
        inputs = flowfile_client.read_inputs()
        assert isinstance(inputs, dict)
        assert "main" in inputs
        assert isinstance(inputs["main"], list)
        assert len(inputs["main"]) == 1
        assert isinstance(inputs["main"][0], pl.LazyFrame)


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
            input_paths={"left": [str(left_path)], "right": [str(right_path)]},
            output_dir=str(tmp_dir / "outputs"),
            artifact_store=store,
        )

        inputs = flowfile_client.read_inputs()
        assert set(inputs.keys()) == {"left", "right"}
        assert inputs["left"][0].collect()["id"].to_list() == [1, 2]
        assert inputs["right"][0].collect()["id"].to_list() == [3, 4]

    def test_read_input_concatenates_multiple_main_paths(self, tmp_dir: Path):
        """When 'main' has multiple paths, read_input returns a union of all."""
        store = ArtifactStore()
        input_dir = tmp_dir / "inputs"
        input_dir.mkdir(exist_ok=True)

        path_a = input_dir / "main_0.parquet"
        path_b = input_dir / "main_1.parquet"
        pl.DataFrame({"val": [1, 2]}).write_parquet(str(path_a))
        pl.DataFrame({"val": [3, 4]}).write_parquet(str(path_b))

        flowfile_client._set_context(
            node_id=3,
            input_paths={"main": [str(path_a), str(path_b)]},
            output_dir=str(tmp_dir / "outputs"),
            artifact_store=store,
        )

        df = flowfile_client.read_input().collect()
        assert sorted(df["val"].to_list()) == [1, 2, 3, 4]

    def test_read_first_returns_only_first(self, tmp_dir: Path):
        """read_first returns only the first file, not the union."""
        store = ArtifactStore()
        input_dir = tmp_dir / "inputs"
        input_dir.mkdir(exist_ok=True)

        path_a = input_dir / "main_0.parquet"
        path_b = input_dir / "main_1.parquet"
        pl.DataFrame({"val": [1, 2]}).write_parquet(str(path_a))
        pl.DataFrame({"val": [3, 4]}).write_parquet(str(path_b))

        flowfile_client._set_context(
            node_id=4,
            input_paths={"main": [str(path_a), str(path_b)]},
            output_dir=str(tmp_dir / "outputs"),
            artifact_store=store,
        )

        df = flowfile_client.read_first().collect()
        assert df["val"].to_list() == [1, 2]

    def test_read_first_missing_name_raises(self, ctx: dict):
        with pytest.raises(KeyError, match="not found"):
            flowfile_client.read_first("nonexistent")

    def test_read_first_no_upstream_raises_runtime_error(self, tmp_dir: Path):
        store = ArtifactStore()
        flowfile_client._set_context(
            node_id=1,
            input_paths={},
            output_dir=str(tmp_dir),
            artifact_store=store,
        )
        with pytest.raises(RuntimeError, match="Upstream nodes did not run yet"):
            flowfile_client.read_first()

    def test_read_inputs_with_multiple_main_paths(self, tmp_dir: Path):
        """read_inputs should return a list of LazyFrames per name."""
        store = ArtifactStore()
        input_dir = tmp_dir / "inputs"
        input_dir.mkdir(exist_ok=True)

        path_0 = input_dir / "main_0.parquet"
        path_1 = input_dir / "main_1.parquet"
        path_2 = input_dir / "main_2.parquet"
        pl.DataFrame({"x": [1]}).write_parquet(str(path_0))
        pl.DataFrame({"x": [2]}).write_parquet(str(path_1))
        pl.DataFrame({"x": [3]}).write_parquet(str(path_2))

        flowfile_client._set_context(
            node_id=5,
            input_paths={"main": [str(path_0), str(path_1), str(path_2)]},
            output_dir=str(tmp_dir / "outputs"),
            artifact_store=store,
        )

        inputs = flowfile_client.read_inputs()
        assert len(inputs["main"]) == 3
        values = [lf.collect()["x"].to_list()[0] for lf in inputs["main"]]
        assert sorted(values) == [1, 2, 3]


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
        names = {item.name for item in listing}
        assert names == {"a", "b"}

    def test_read_missing_artifact_raises(self, ctx: dict):
        with pytest.raises(KeyError, match="not found"):
            flowfile_client.read_artifact("missing")

    def test_publish_duplicate_artifact_raises(self, ctx: dict):
        flowfile_client.publish_artifact("model", {"v": 1})
        with pytest.raises(ValueError, match="already exists"):
            flowfile_client.publish_artifact("model", {"v": 2})

    def test_delete_artifact(self, ctx: dict):
        flowfile_client.publish_artifact("temp", 42)
        flowfile_client.delete_artifact("temp")
        with pytest.raises(KeyError, match="not found"):
            flowfile_client.read_artifact("temp")

    def test_delete_missing_artifact_raises(self, ctx: dict):
        with pytest.raises(KeyError, match="not found"):
            flowfile_client.delete_artifact("nonexistent")

    def test_delete_then_republish(self, ctx: dict):
        flowfile_client.publish_artifact("model", "v1")
        flowfile_client.delete_artifact("model")
        flowfile_client.publish_artifact("model", "v2")
        assert flowfile_client.read_artifact("model") == "v2"


class TestDisplay:
    def test_reset_displays(self):
        flowfile_client._reset_displays()
        assert flowfile_client._get_displays() == []

    def test_display_plain_text(self):
        flowfile_client._reset_displays()
        flowfile_client.display("hello world")
        displays = flowfile_client._get_displays()
        assert len(displays) == 1
        assert displays[0]["mime_type"] == "text/plain"
        assert displays[0]["data"] == "hello world"
        assert displays[0]["title"] == ""

    def test_display_with_title(self):
        flowfile_client._reset_displays()
        flowfile_client.display("some data", title="My Title")
        displays = flowfile_client._get_displays()
        assert len(displays) == 1
        assert displays[0]["title"] == "My Title"

    def test_display_html_string(self):
        flowfile_client._reset_displays()
        html = "<b>bold text</b>"
        flowfile_client.display(html)
        displays = flowfile_client._get_displays()
        assert len(displays) == 1
        assert displays[0]["mime_type"] == "text/html"
        assert displays[0]["data"] == html

    def test_display_complex_html(self):
        flowfile_client._reset_displays()
        html = '<div class="test"><p>Hello</p></div>'
        flowfile_client.display(html)
        displays = flowfile_client._get_displays()
        assert len(displays) == 1
        assert displays[0]["mime_type"] == "text/html"

    def test_display_multiple_outputs(self):
        flowfile_client._reset_displays()
        flowfile_client.display("first")
        flowfile_client.display("second")
        flowfile_client.display("third")
        displays = flowfile_client._get_displays()
        assert len(displays) == 3
        assert displays[0]["data"] == "first"
        assert displays[1]["data"] == "second"
        assert displays[2]["data"] == "third"

    def test_display_number_as_plain_text(self):
        flowfile_client._reset_displays()
        flowfile_client.display(42)
        displays = flowfile_client._get_displays()
        assert len(displays) == 1
        assert displays[0]["mime_type"] == "text/plain"
        assert displays[0]["data"] == "42"

    def test_display_dict_as_plain_text(self):
        flowfile_client._reset_displays()
        flowfile_client.display({"key": "value"})
        displays = flowfile_client._get_displays()
        assert len(displays) == 1
        assert displays[0]["mime_type"] == "text/plain"
        assert "key" in displays[0]["data"]

    def test_get_displays_returns_copy(self):
        """Ensure _get_displays returns the actual list that can be cleared."""
        flowfile_client._reset_displays()
        flowfile_client.display("test")
        displays1 = flowfile_client._get_displays()
        assert len(displays1) == 1
        flowfile_client._reset_displays()
        displays2 = flowfile_client._get_displays()
        assert len(displays2) == 0


class TestDisplayTypeDetection:
    def test_is_html_string_true(self):
        assert flowfile_client._is_html_string("<b>test</b>") is True
        assert flowfile_client._is_html_string("<div></div>") is True
        assert flowfile_client._is_html_string("Hello <b>world</b>!") is True

    def test_is_html_string_false(self):
        assert flowfile_client._is_html_string("plain text") is False
        assert flowfile_client._is_html_string("just text with math: 5 < 10") is False  # only <
        assert flowfile_client._is_html_string("x < 10 and y > 5") is False  # comparison, not HTML
        assert flowfile_client._is_html_string("a < b > c") is False  # not actual HTML tags
        assert flowfile_client._is_html_string(123) is False
        assert flowfile_client._is_html_string(None) is False

    def test_is_matplotlib_figure_without_import(self):
        """Without matplotlib installed, should return False."""
        result = flowfile_client._is_matplotlib_figure("not a figure")
        assert result is False

    def test_is_plotly_figure_without_import(self):
        """Without plotly installed, should return False."""
        result = flowfile_client._is_plotly_figure("not a figure")
        assert result is False

    def test_is_pil_image_without_import(self):
        """Without PIL installed, should return False."""
        result = flowfile_client._is_pil_image("not an image")
        assert result is False


class TestSharedLocation:
    """Tests for flowfile_ctx.get_shared_location()."""

    def test_returns_path_under_user_files(self, tmp_dir: Path, monkeypatch: pytest.MonkeyPatch):
        shared_dir = str(tmp_dir / "shared")
        monkeypatch.setenv("FLOWFILE_KERNEL_SHARED_DIR", shared_dir)

        result = flowfile_client.get_shared_location("test_file.csv")
        assert result == os.path.join(shared_dir, "user_files", "test_file.csv")

    def test_creates_parent_directories(self, tmp_dir: Path, monkeypatch: pytest.MonkeyPatch):
        shared_dir = str(tmp_dir / "shared")
        monkeypatch.setenv("FLOWFILE_KERNEL_SHARED_DIR", shared_dir)

        result = flowfile_client.get_shared_location("other_dir/test_file.csv")
        expected = os.path.join(shared_dir, "user_files", "other_dir", "test_file.csv")
        assert result == expected
        assert os.path.isdir(os.path.dirname(result))

    def test_nested_subdirectories(self, tmp_dir: Path, monkeypatch: pytest.MonkeyPatch):
        shared_dir = str(tmp_dir / "shared")
        monkeypatch.setenv("FLOWFILE_KERNEL_SHARED_DIR", shared_dir)

        result = flowfile_client.get_shared_location("a/b/c/deep_file.parquet")
        expected = os.path.join(shared_dir, "user_files", "a", "b", "c", "deep_file.parquet")
        assert result == expected
        assert os.path.isdir(os.path.dirname(result))

    def test_defaults_to_shared_when_env_not_set(self, tmp_dir: Path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("FLOWFILE_KERNEL_SHARED_DIR", raising=False)
        # Patch os.makedirs to avoid PermissionError on /shared in CI
        created = []
        monkeypatch.setattr(os, "makedirs", lambda p, exist_ok=False: created.append(p))

        result = flowfile_client.get_shared_location("test.csv")
        assert result == os.path.join("/shared", "user_files", "test.csv")
        assert created == [os.path.join("/shared", "user_files")]

    def test_file_is_writable(self, tmp_dir: Path, monkeypatch: pytest.MonkeyPatch):
        shared_dir = str(tmp_dir / "shared")
        monkeypatch.setenv("FLOWFILE_KERNEL_SHARED_DIR", shared_dir)

        path = flowfile_client.get_shared_location("writable_test.csv")
        with open(path, "w") as f:
            f.write("col1,col2\n1,2\n")
        assert os.path.isfile(path)

    def test_does_not_require_execution_context(self, tmp_dir: Path, monkeypatch: pytest.MonkeyPatch):
        """shared_location works without _set_context() being called."""
        shared_dir = str(tmp_dir / "shared")
        monkeypatch.setenv("FLOWFILE_KERNEL_SHARED_DIR", shared_dir)
        flowfile_client._clear_context()

        result = flowfile_client.get_shared_location("no_context.csv")
        assert "no_context.csv" in result

    def test_write_parquet_roundtrip(self, tmp_dir: Path, monkeypatch: pytest.MonkeyPatch):
        """Write a Polars DataFrame to shared_location and read it back."""
        monkeypatch.setenv("FLOWFILE_KERNEL_SHARED_DIR", str(tmp_dir / "shared"))

        df = pl.DataFrame({"id": [1, 2, 3], "value": [10.5, 20.0, 30.1]})
        path = flowfile_client.get_shared_location("output.parquet")
        df.write_parquet(path)

        result = pl.read_parquet(path)
        assert result.shape == (3, 2)
        assert result["id"].to_list() == [1, 2, 3]
        assert result["value"].to_list() == [10.5, 20.0, 30.1]

    def test_write_parquet_nested_path(self, tmp_dir: Path, monkeypatch: pytest.MonkeyPatch):
        """Write parquet into a nested subdirectory via shared_location."""
        monkeypatch.setenv("FLOWFILE_KERNEL_SHARED_DIR", str(tmp_dir / "shared"))

        df = pl.DataFrame({"name": ["alice", "bob"], "score": [95, 87]})
        path = flowfile_client.get_shared_location("exports/daily/scores.parquet")
        df.write_parquet(path)

        result = pl.read_parquet(path)
        assert result["name"].to_list() == ["alice", "bob"]

    def test_write_csv_and_parquet_same_dir(self, tmp_dir: Path, monkeypatch: pytest.MonkeyPatch):
        """Write both CSV and Parquet to the same shared subdirectory."""
        monkeypatch.setenv("FLOWFILE_KERNEL_SHARED_DIR", str(tmp_dir / "shared"))

        df = pl.DataFrame({"x": [1, 2], "y": [3, 4]})
        csv_path = flowfile_client.get_shared_location("reports/data.csv")
        parquet_path = flowfile_client.get_shared_location("reports/data.parquet")
        df.write_csv(csv_path)
        df.write_parquet(parquet_path)

        assert pl.read_csv(csv_path).shape == (2, 2)
        assert pl.read_parquet(parquet_path)["x"].to_list() == [1, 2]


class TestPathTranslation:
    """Host<->container path translation, including Windows host prefixes.

    The kernel always runs on Linux, but the Docker host may be Windows —
    Core then hands over backslash-separated host paths that must rebase to
    pure-POSIX container paths (and back, with host-native separators).
    """

    WIN_SHARED = r"C:\Users\u\.flowfile\temp\kernel_shared"
    WIN_CATALOG = r"C:\Users\u\.flowfile\catalog_tables"

    def test_host_to_container_windows_shared(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("FLOWFILE_HOST_CATALOG_TABLES_DIR", raising=False)
        monkeypatch.setenv("FLOWFILE_HOST_SHARED_DIR", self.WIN_SHARED)
        result = flowfile_client._translate_host_path_to_container(
            self.WIN_SHARED + r"\artifact_staging\5_model.joblib"
        )
        assert result == "/shared/artifact_staging/5_model.joblib"

    def test_host_to_container_windows_catalog(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("FLOWFILE_HOST_CATALOG_TABLES_DIR", self.WIN_CATALOG)
        monkeypatch.delenv("FLOWFILE_HOST_SHARED_DIR", raising=False)
        result = flowfile_client._translate_host_path_to_container(self.WIN_CATALOG + r"\orders_ab12cd34")
        assert result == "/catalog_tables/orders_ab12cd34"

    def test_host_to_container_catalog_priority(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("FLOWFILE_HOST_CATALOG_TABLES_DIR", self.WIN_CATALOG)
        monkeypatch.setenv("FLOWFILE_HOST_SHARED_DIR", self.WIN_SHARED)
        assert flowfile_client._translate_host_path_to_container(self.WIN_CATALOG) == "/catalog_tables"
        assert flowfile_client._translate_host_path_to_container(self.WIN_SHARED) == "/shared"

    def test_host_to_container_posix_unchanged(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("FLOWFILE_HOST_CATALOG_TABLES_DIR", raising=False)
        monkeypatch.setenv("FLOWFILE_HOST_SHARED_DIR", "/home/u/.flowfile/temp/kernel_shared")
        result = flowfile_client._translate_host_path_to_container(
            "/home/u/.flowfile/temp/kernel_shared/1/5/outputs"
        )
        assert result == "/shared/1/5/outputs"

    def test_host_to_container_no_env_passthrough(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("FLOWFILE_HOST_CATALOG_TABLES_DIR", raising=False)
        monkeypatch.delenv("FLOWFILE_HOST_SHARED_DIR", raising=False)
        path = "/app/internal_storage/temp/kernel_shared/1/5/outputs"
        assert flowfile_client._translate_host_path_to_container(path) == path

    def test_host_to_container_unrelated_passthrough(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("FLOWFILE_HOST_SHARED_DIR", self.WIN_SHARED)
        other = r"D:\elsewhere\data.parquet"
        assert flowfile_client._translate_host_path_to_container(other) == other

    def test_host_to_container_sibling_prefix_not_matched(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("FLOWFILE_HOST_CATALOG_TABLES_DIR", raising=False)
        monkeypatch.setenv("FLOWFILE_HOST_SHARED_DIR", "/home/u/shared")
        path = "/home/u/shared_other/x.parquet"
        assert flowfile_client._translate_host_path_to_container(path) == path

    def test_container_to_host_windows(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("FLOWFILE_HOST_CATALOG_TABLES_DIR", self.WIN_CATALOG)
        monkeypatch.setenv("FLOWFILE_KERNEL_CATALOG_TABLES_DIR", "/catalog_tables")
        result = flowfile_client._translate_container_path_to_host("/catalog_tables/orders_ab12")
        assert result == self.WIN_CATALOG + "\\orders_ab12"
        assert "/" not in result

    def test_container_to_host_windows_exact(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("FLOWFILE_HOST_CATALOG_TABLES_DIR", self.WIN_CATALOG)
        monkeypatch.setenv("FLOWFILE_KERNEL_CATALOG_TABLES_DIR", "/catalog_tables")
        assert flowfile_client._translate_container_path_to_host("/catalog_tables") == self.WIN_CATALOG

    def test_container_to_host_posix(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("FLOWFILE_HOST_CATALOG_TABLES_DIR", "/home/u/.flowfile/catalog_tables")
        monkeypatch.setenv("FLOWFILE_KERNEL_CATALOG_TABLES_DIR", "/catalog_tables")
        result = flowfile_client._translate_container_path_to_host("/catalog_tables/orders_ab12")
        assert result == "/home/u/.flowfile/catalog_tables/orders_ab12"

    def test_container_to_host_no_env_passthrough(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("FLOWFILE_HOST_CATALOG_TABLES_DIR", raising=False)
        path = "/catalog_tables/orders_ab12"
        assert flowfile_client._translate_container_path_to_host(path) == path

    def test_windows_roundtrip(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("FLOWFILE_HOST_CATALOG_TABLES_DIR", self.WIN_CATALOG)
        monkeypatch.setenv("FLOWFILE_KERNEL_CATALOG_TABLES_DIR", "/catalog_tables")
        host = self.WIN_CATALOG + r"\orders_ab12"
        container = flowfile_client._translate_host_path_to_container(host)
        assert container == "/catalog_tables/orders_ab12"
        assert flowfile_client._translate_container_path_to_host(container) == host


TABLE_MIME = "application/vnd.flowfile.table+json"
GWALKER_MIME = "application/vnd.flowfile.gwalker+json"


class TestDisplayDataFrames:
    """flowfile_ctx.display / explore rendering of Polars frames."""

    def test_display_dataframe_emits_table_mime(self):
        flowfile_client._reset_displays()
        df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        flowfile_client.display(df)

        out = flowfile_client._get_displays()
        assert len(out) == 1
        assert out[0]["mime_type"] == TABLE_MIME
        payload = json.loads(out[0]["data"])
        assert payload["columns"] == ["a", "b"]
        assert payload["data"] == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
        assert payload["total_rows"] == 2
        assert payload["loaded_rows"] == 2
        assert payload["truncated"] is False

    def test_display_field_semantic_types(self):
        flowfile_client._reset_displays()
        df = pl.DataFrame({"num": [1.5], "name": ["a"], "day": [datetime.date(2024, 1, 1)]})
        flowfile_client.display(df)

        payload = json.loads(flowfile_client._get_displays()[0]["data"])
        sem = {f["name"]: f["semanticType"] for f in payload["fields"]}
        analytic = {f["name"]: f["analyticType"] for f in payload["fields"]}
        assert sem == {"num": "quantitative", "name": "nominal", "day": "temporal"}
        assert analytic == {"num": "measure", "name": "dimension", "day": "dimension"}

    def test_display_lazyframe_head_collected_and_truncated(self):
        flowfile_client._reset_displays()
        lf = pl.LazyFrame({"n": list(range(25))})
        flowfile_client.display(lf, max_rows=10)

        payload = json.loads(flowfile_client._get_displays()[0]["data"])
        assert payload["total_rows"] == 25
        assert payload["loaded_rows"] == 10
        assert payload["truncated"] is True
        assert len(payload["data"]) == 10

    def test_display_temporal_decimal_are_json_safe(self):
        flowfile_client._reset_displays()
        df = pl.DataFrame(
            {
                "d": [datetime.date(2024, 1, 2)],
                "ts": [datetime.datetime(2024, 1, 2, 3, 4, 5)],
                "dec": [Decimal("1.25")],
            }
        )
        flowfile_client.display(df)

        row = json.loads(flowfile_client._get_displays()[0]["data"])["data"][0]
        assert row["d"] == "2024-01-02"
        assert row["ts"] == "2024-01-02T03:04:05"
        assert row["dec"] == 1.25

    def test_display_non_finite_floats_become_null(self):
        flowfile_client._reset_displays()
        df = pl.DataFrame({"x": [1.0, float("nan"), float("inf")]})
        flowfile_client.display(df)

        data = flowfile_client._get_displays()[0]["data"]
        # Browser JSON.parse rejects NaN/Infinity tokens.
        assert "NaN" not in data
        assert "Infinity" not in data
        rows = json.loads(data)["data"]
        assert rows[0]["x"] == 1.0
        assert rows[1]["x"] is None
        assert rows[2]["x"] is None

    def test_explore_emits_gwalker_mime(self):
        flowfile_client._reset_displays()
        flowfile_client.explore(pl.DataFrame({"a": [1]}))

        out = flowfile_client._get_displays()
        assert out[0]["mime_type"] == GWALKER_MIME
        assert json.loads(out[0]["data"])["columns"] == ["a"]

    def test_explore_non_frame_falls_back_to_display(self):
        flowfile_client._reset_displays()
        flowfile_client.explore("hello")

        out = flowfile_client._get_displays()
        assert out[0]["mime_type"] == "text/plain"
        assert out[0]["data"] == "hello"

    def test_display_non_frame_stays_text(self):
        flowfile_client._reset_displays()
        flowfile_client.display({"k": "v"})

        out = flowfile_client._get_displays()
        assert out[0]["mime_type"] == "text/plain"

    def test_display_pandas_stays_text(self):
        pd = pytest.importorskip("pandas")
        flowfile_client._reset_displays()
        flowfile_client.display(pd.DataFrame({"a": [1, 2]}))

        out = flowfile_client._get_displays()
        assert out[0]["mime_type"] == "text/plain"
