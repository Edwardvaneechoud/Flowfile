"""Tests for the project export: multi-file FlowFrame project generation.

Covers manifest structure, verbatim notebook module emission, the local
flowfile_ctx shim, custom-node module emission, zip packaging, and an
end-to-end execution of an exported project.
"""

import io
import subprocess
import sys
import zipfile
from pathlib import Path

import polars as pl
import pytest

from flowfile_core.configs.node_store import add_to_custom_node_store
from flowfile_core.flowfile.code_generator import project_shim
from flowfile_core.flowfile.code_generator.project_exporter import (
    export_flow_to_project,
    project_to_zip_bytes,
)
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings, Section, TextInput
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.output_model import ProjectExportManifest


def create_flow_settings(flow_id: int = 1) -> schemas.FlowSettings:
    return schemas.FlowSettings(
        flow_id=flow_id,
        execution_mode="Performance",
        execution_location="local",
        path="/tmp/test_flow",
    )


def create_basic_flow(flow_id: int = 1, name: str = "test_flow") -> FlowGraph:
    return FlowGraph(flow_settings=create_flow_settings(flow_id), name=name)


def add_sample_input(flow: FlowGraph, node_id: int = 1, node_reference: str | None = None) -> None:
    flow.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow.flow_id,
            node_id=node_id,
            node_reference=node_reference,
            raw_data_format=input_schema.RawData(
                columns=[
                    input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                    input_schema.MinimalFieldInfo(name="age", data_type="Integer"),
                ],
                data=[[1, 2, 3], [25, 30, 35]],
            ),
        )
    )


def add_notebook_node(
    flow: FlowGraph,
    node_id: int,
    depending_on_ids: list[int],
    *,
    cells: list[str] | None = None,
    code: str = "",
    output_names: list[str] | None = None,
    description: str = "",
) -> None:
    notebook_cells = None
    if cells is not None:
        notebook_cells = [input_schema.NotebookCell(id=f"cell-{i}", code=c) for i, c in enumerate(cells)]
    flow.add_python_script(
        input_schema.NodePythonScript(
            flow_id=flow.flow_id,
            node_id=node_id,
            depending_on_ids=depending_on_ids,
            description=description,
            python_script_input=input_schema.PythonScriptInput(code=code, cells=notebook_cells),
            output_names=output_names or ["main"],
        )
    )


def get_file(manifest: ProjectExportManifest, path: str) -> str:
    for file in manifest.files:
        if file.path == path:
            return file.content
    raise AssertionError(f"File {path!r} not in manifest: {[f.path for f in manifest.files]}")


def file_paths(manifest: ProjectExportManifest) -> set[str]:
    return {file.path for file in manifest.files}


def write_project(manifest: ProjectExportManifest, target: Path) -> Path:
    project_dir = target / manifest.project_name
    for file in manifest.files:
        path = project_dir / file.path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(file.content, encoding="utf-8")
    return project_dir


# ---------------------------------------------------------------------------
# Manifest structure
# ---------------------------------------------------------------------------


def test_project_manifest_without_notebooks():
    flow = create_basic_flow(name="Plain Flow")
    add_sample_input(flow, node_id=1)
    flow.add_filter(
        input_schema.NodeFilter(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(filter_type="advanced", advanced_filter="[age]>26"),
        )
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    manifest = export_flow_to_project(flow)

    assert manifest.project_name == "plain_flow"
    assert {"pipeline.py", "main.py", "pyproject.toml", "README.md"} <= file_paths(manifest)
    # No notebook nodes -> no shim, no notebooks package
    assert "flowfile_ctx.py" not in file_paths(manifest)
    assert not any(path.startswith("notebooks/") for path in file_paths(manifest))
    assert "import flowfile_ctx" not in get_file(manifest, "pipeline.py")
    assert manifest.warnings == []


def test_project_scaffolding_contents():
    flow = create_basic_flow(name="My Flow")
    add_sample_input(flow, node_id=1)

    manifest = export_flow_to_project(flow)

    pyproject = get_file(manifest, "pyproject.toml")
    assert 'name = "my-flow"' in pyproject
    assert "flowfile" in pyproject
    assert "polars" in pyproject

    readme = get_file(manifest, "README.md")
    assert "My Flow" in readme
    assert "manual_input" in readme

    main_py = get_file(manifest, "main.py")
    assert "from pipeline import run_etl_pipeline" in main_py

    pipeline = get_file(manifest, "pipeline.py")
    assert "def run_etl_pipeline():" in pipeline


def test_project_manifest_with_notebook():
    flow = create_basic_flow(name="Notebook Flow")
    add_sample_input(flow, node_id=1)
    add_notebook_node(
        flow,
        node_id=2,
        depending_on_ids=[1],
        description="Clean data",
        cells=["df = flowfile_ctx.read_input()", "flowfile_ctx.publish_output(df)"],
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    manifest = export_flow_to_project(flow)

    assert {"flowfile_ctx.py", "notebooks/__init__.py", "notebooks/node_02_clean_data.py"} <= file_paths(manifest)
    pipeline = get_file(manifest, "pipeline.py")
    assert "import flowfile_ctx" in pipeline
    assert 'flowfile_ctx.run_node(' in pipeline
    assert '"notebooks.node_02_clean_data"' in pipeline
    # The shipped shim is the real module source
    assert get_file(manifest, "flowfile_ctx.py") == Path(project_shim.__file__).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Notebook module emission
# ---------------------------------------------------------------------------


def test_notebook_module_keeps_cells_verbatim():
    cell_1 = "import polars as pl\n\ndf = flowfile_ctx.read_input()"
    cell_2 = "result = df.with_columns((pl.col('age') * 2).alias('age2'))\nflowfile_ctx.publish_output(result)"
    flow = create_basic_flow()
    add_sample_input(flow, node_id=1)
    add_notebook_node(flow, node_id=2, depending_on_ids=[1], cells=[cell_1, cell_2])
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    manifest = export_flow_to_project(flow)

    module = get_file(manifest, "notebooks/node_02_python_script.py")
    assert "import flowfile_ctx" in module
    assert cell_1 in module
    assert cell_2 in module
    assert module.count("# %%") == 2


def test_notebook_module_falls_back_to_code_without_cells():
    code = "df = flowfile_ctx.read_input()\nflowfile_ctx.publish_output(df.head(1))"
    flow = create_basic_flow()
    add_sample_input(flow, node_id=1)
    add_notebook_node(flow, node_id=2, depending_on_ids=[1], code=code)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    manifest = export_flow_to_project(flow)

    module = get_file(manifest, "notebooks/node_02_python_script.py")
    assert code in module
    assert "# %%" not in module


def test_notebook_inputs_use_node_reference():
    flow = create_basic_flow()
    add_sample_input(flow, node_id=1, node_reference="orders")
    add_notebook_node(flow, node_id=2, depending_on_ids=[1], cells=["df = flowfile_ctx.read_input('orders')"])
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    manifest = export_flow_to_project(flow)

    pipeline = get_file(manifest, "pipeline.py")
    assert '"orders": [orders.data]' in pipeline
    assert '"main": [orders.data]' in pipeline


def test_notebook_multi_output_feeds_downstream_nodes():
    flow = create_basic_flow()
    add_sample_input(flow, node_id=1)
    add_notebook_node(
        flow,
        node_id=2,
        depending_on_ids=[1],
        cells=["flowfile_ctx.publish_output(flowfile_ctx.read_input(), name='main')"],
        output_names=["main", "rejected"],
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    flow.add_filter(
        input_schema.NodeFilter(
            flow_id=1,
            node_id=3,
            depending_on_id=2,
            filter_input=transform_schema.FilterInput(filter_type="advanced", advanced_filter="[age]>26"),
        )
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(2, 3, output_handle="output-1"))

    manifest = export_flow_to_project(flow)

    pipeline = get_file(manifest, "pipeline.py")
    assert "output_names=['main', 'rejected']" in pipeline
    assert 'df_2_rejected = ff.FlowFrame(_nb_2_outputs["rejected"])' in pipeline
    # The downstream filter must consume the output-1 variable
    assert "df_3 = df_2_rejected.filter" in pipeline


def test_notebook_with_server_only_api_adds_warning():
    flow = create_basic_flow()
    add_sample_input(flow, node_id=1)
    add_notebook_node(
        flow,
        node_id=2,
        depending_on_ids=[1],
        cells=["model = flowfile_ctx.get_global('my_model')"],
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    manifest = export_flow_to_project(flow)

    assert any("get_global" in warning for warning in manifest.warnings)
    readme = get_file(manifest, "README.md")
    assert "Limitations" in readme


# ---------------------------------------------------------------------------
# Custom (user-defined) nodes
# ---------------------------------------------------------------------------


@pytest.fixture
def MarkerColumnNode():
    """A custom node that adds a marker column."""

    class MarkerColumnSettings(NodeSettings):
        config: Section = Section(
            title="Configuration",
            column_name=TextInput(label="Column Name", default="marker"),
        )

    class MarkerColumn(CustomNodeBase):
        node_name: str = "Marker Column"
        node_category: str = "Transform"
        number_of_inputs: int = 1
        number_of_outputs: int = 1
        settings_schema: MarkerColumnSettings = MarkerColumnSettings()

        def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
            col_name = self.settings_schema.config.column_name.value
            return inputs[0].with_columns(pl.lit(True).alias(col_name))

    return MarkerColumn


def test_custom_node_exported_as_module(MarkerColumnNode):
    add_to_custom_node_store(MarkerColumnNode)

    flow = create_basic_flow()
    add_sample_input(flow, node_id=1)
    node_settings = input_schema.UserDefinedNode(
        flow_id=1,
        node_id=2,
        settings={"config": {"column_name": "is_processed"}},
        is_user_defined=True,
    )
    flow.add_user_defined_node(
        custom_node=MarkerColumnNode.from_settings(node_settings.settings),
        user_defined_node_settings=node_settings,
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    manifest = export_flow_to_project(flow)

    assert {"custom_nodes/__init__.py", "custom_nodes/marker_column.py"} <= file_paths(manifest)
    assert "class MarkerColumn" in get_file(manifest, "custom_nodes/marker_column.py")
    pipeline = get_file(manifest, "pipeline.py")
    assert "from custom_nodes.marker_column import MarkerColumn" in pipeline
    # The class must not be inlined in the pipeline itself
    assert "class MarkerColumn" not in pipeline
    assert "_custom_node_2.process(" in pipeline


# ---------------------------------------------------------------------------
# Shim behaviour
# ---------------------------------------------------------------------------


@pytest.fixture
def shim_context():
    """Activate a shim node context and clean it up afterwards."""

    def activate(inputs: dict[str, list[pl.LazyFrame]]):
        project_shim._current_context = {"inputs": inputs, "outputs": {}, "node_name": "test"}
        return project_shim._current_context

    yield activate
    project_shim._current_context = None


def test_shim_read_input_concatenates_frames(shim_context):
    frame_a = pl.LazyFrame({"a": [1]})
    frame_b = pl.LazyFrame({"a": [2]})
    shim_context({"main": [frame_a, frame_b]})
    assert project_shim.read_input().collect().to_series().to_list() == [1, 2]


def test_shim_read_input_unknown_name_lists_available(shim_context):
    shim_context({"orders": [pl.LazyFrame({"a": [1]})], "main": [pl.LazyFrame({"a": [1]})]})
    with pytest.raises(KeyError, match="orders"):
        project_shim.read_input("customers")


def test_shim_requires_active_context():
    with pytest.raises(RuntimeError, match="run_node"):
        project_shim.read_input()


def test_shim_artifact_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setattr(project_shim, "_ARTIFACTS_DIR", tmp_path / ".artifacts")
    project_shim.publish_artifact("model", {"weights": [1, 2, 3]})
    assert project_shim.read_artifact("model") == {"weights": [1, 2, 3]}
    assert [a["name"] for a in project_shim.list_artifacts()] == ["model"]
    project_shim.delete_artifact("model")
    assert project_shim.list_artifacts() == []
    with pytest.raises(KeyError):
        project_shim.read_artifact("model")


def test_shim_server_only_apis_raise():
    with pytest.raises(NotImplementedError, match="publish_global"):
        project_shim.publish_global("name", object())
    with pytest.raises(NotImplementedError, match="read_catalog_table"):
        project_shim.read_catalog_table("table")


def test_shim_run_node_executes_module_and_falls_back(tmp_path, monkeypatch):
    package_dir = tmp_path / "notebooks"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("")
    (package_dir / "publishes.py").write_text(
        "import flowfile_ctx\n"
        "df = flowfile_ctx.read_input()\n"
        "flowfile_ctx.publish_output(df.with_columns(b=2 * df.collect_schema().len()))\n"
    )
    (package_dir / "passthrough.py").write_text("import flowfile_ctx  # noqa: F401\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    # The notebook modules import the shim by its in-project name
    monkeypatch.setitem(sys.modules, "flowfile_ctx", project_shim)

    frame = pl.LazyFrame({"a": [1, 2]})
    published = project_shim.run_node("notebooks.publishes", {"main": [frame]}, ["main"])
    assert published["main"].collect().columns == ["a", "b"]

    fallback = project_shim.run_node("notebooks.passthrough", {"main": [frame]}, ["main"])
    assert fallback["main"] is frame

    with pytest.raises(RuntimeError, match="did not publish"):
        project_shim.run_node("notebooks.passthrough", {"main": [frame]}, ["main", "rejected"])


# ---------------------------------------------------------------------------
# Packaging & execution
# ---------------------------------------------------------------------------


def test_zip_round_trip():
    flow = create_basic_flow(name="Zip Flow")
    add_sample_input(flow, node_id=1)

    manifest = export_flow_to_project(flow)
    zip_bytes = project_to_zip_bytes(manifest)

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        names = archive.namelist()
        assert all(name.startswith("zip_flow/") for name in names)
        assert "zip_flow/pipeline.py" in names
        unpacked = archive.read("zip_flow/pipeline.py").decode("utf-8")
    assert unpacked == get_file(manifest, "pipeline.py")


def test_project_executes_end_to_end(tmp_path):
    """Write an exported project (with a notebook node) to disk and run it."""
    flow = create_basic_flow(name="E2E Flow")
    add_sample_input(flow, node_id=1)
    add_notebook_node(
        flow,
        node_id=2,
        depending_on_ids=[1],
        description="Double the age",
        cells=[
            "import polars as pl\n\ndf = flowfile_ctx.read_input()",
            "result = df.with_columns((pl.col('age') * 2).alias('age_doubled'))\n"
            "flowfile_ctx.log_info('doubled the age column')\n"
            "flowfile_ctx.publish_output(result)",
        ],
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    flow.add_filter(
        input_schema.NodeFilter(
            flow_id=1,
            node_id=3,
            depending_on_id=2,
            filter_input=transform_schema.FilterInput(filter_type="advanced", advanced_filter="[age_doubled]>50"),
        )
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(2, 3))

    manifest = export_flow_to_project(flow)
    project_dir = write_project(manifest, tmp_path)

    result = subprocess.run(
        [sys.executable, "main.py"], cwd=project_dir, capture_output=True, text=True, timeout=300
    )
    assert result.returncode == 0, result.stderr
    assert "doubled the age column" in result.stdout
    assert "age_doubled" in result.stdout


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
